extern crate colored;
extern crate crossbeam_channel;
extern crate glob;
extern crate grep_cli;
extern crate num_cpus;
extern crate prettytable;
extern crate regex;
extern crate cpu_time;

#[macro_use]
extern crate lazy_static;
extern crate structopt;

use std::{
    collections::{BTreeMap, HashSet},
    fmt::Display,
    fs,
    io::prelude::*,
    path::PathBuf,
    thread,
    time::{Duration, Instant},
};

use cpu_time::ProcessTime;
use crossbeam_channel as channel;
use grep_cli::DecompressionReader;
use regex::Regex;

use prettytable::{cell::Cell, format, row::Row, Table};

use colored::*;

use glob::glob_with;
use glob::MatchOptions;

type MyMap = BTreeMap<String, KeySum>;

mod cli;
mod gen;
mod testre;

use cli::{get_cli, CliCfg};
use gen::{io_thread_swizzle, FileChunk};
use testre::testre;

#[derive(Debug)]
struct KeySum {
    count: u64,
    sums: Vec<f64>,
    avgs: Vec<(f64, usize)>,
    distinct: Vec<HashSet<String>>,
}

impl KeySum {
    pub fn new(sum_len: usize, dist_len: usize, avg_len: usize) -> KeySum {
        KeySum {
            count: 0,
            sums: vec![0f64; sum_len],
            avgs: vec![(0f64,0usize); avg_len],
            distinct: {
                let mut v = Vec::with_capacity(dist_len);
                for _ in 0..dist_len {
                    v.push(HashSet::new());
                }
                v
            },
        }
    }
}

// main/cli/csv thread-spawn read->thread  { spawn block { re | csv } }

fn main() {
    if let Err(err) = csv() {
        eprintln!("error: {}", &err);
        std::process::exit(1);
    }
}

fn csv() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = get_cli()?;

    let mut total_rowcount = 0usize;
    let mut total_fieldcount = 0usize;
    let mut total_blocks = 0usize;
    let mut total_bytes = 0usize;
    let start_f = Instant::now();
    let startcpu = ProcessTime::now();

    if cfg.testre.is_some() {
        testre(&cfg)?;
        return Ok(());
    }
    let mut main_map = MyMap::new();

    /////////////////////////////////////////////////////////////////////////////////////////////////

    let mut worker_handler = vec![];
    let (send, recv): (channel::Sender<Option<FileChunk>>, channel::Receiver<Option<FileChunk>>) = channel::bounded(cfg.thread_qsize);

    for no_threads in 0..cfg.no_threads {
        let cfg = cfg.clone();
        let clone_recv = recv.clone();

        let h = match cfg.re_str.len() {
            0 => thread::Builder::new()
                .name(format!("worker_csv{}", no_threads))
                .spawn(move || worker_csv(&cfg, &clone_recv))
                .unwrap(),
            1 => thread::Builder::new()
                .name(format!("worker_re{}", no_threads))
                .spawn(move || worker_re(&cfg, &clone_recv))
                .unwrap(),
            _ => thread::Builder::new()
                .name(format!("wr_mul_re{}", no_threads))
                .spawn(move || worker_multi_re(&cfg, &clone_recv))
                .unwrap(),
        };
        worker_handler.push(h);
    }

    // SETUP IO
    let block_size = match cfg.block_size_b {
        0 => cfg.block_size_k * 1024,
        _ => cfg.block_size_b,
    };

    //
    // forward all IO to the block queue
    //

    let per_file = |filename: &PathBuf| {
        let metadata = match fs::metadata(&filename) {
            Ok(m) => m,
            Err(err) => {
                eprintln!("skipping file \"{}\", could not get stats on it, cause: {}", filename.display(), err);
                return Ok((0, 0));
            }
        };
        if !metadata.is_file() {
            return Ok((0, 0));
        }

        if cfg.verbose >= 1 {
            eprintln!("processing file: {}", filename.display());
        }
        let mut rdr = DecompressionReader::new(&filename)?;
        // let mut f = match OpenOptions::new().read(true).write(false).create(false).open(&filename) {
        //     Ok(f) => f,
        //     Err(e) => panic!("cannot open file \"{}\" due to this error: {}", filename.display(), e),
        // };
        io_thread_swizzle(&filename.display(), block_size, cfg.verbose, &mut rdr, &send)
    };

    if cfg.files.len() <= 0 && cfg.glob.is_none() {
        eprintln!("{}", "<<< reading from stdin".red());
        let stdin = std::io::stdin();
        let mut handle = stdin; // .lock();

        let (blocks, bytes) = io_thread_swizzle(&"STDIO".to_string(), block_size, cfg.verbose, &mut handle, &send)?;
        total_bytes += bytes;
        total_blocks += blocks;
    } else if cfg.files.len() > 0 {
        let filelist = &cfg.files;
        for filename in filelist {
            let (blocks, bytes) = per_file(&filename)?;
            total_blocks += blocks;
            total_bytes += bytes;
        }
    } else {
        let options = MatchOptions {
            case_sensitive: true,
            require_literal_separator: false,
            require_literal_leading_dot: false,
        };
        for entry in glob_with(&(cfg.glob.as_ref().expect("NO glob found when expected")), options).unwrap() {
            if let Ok(path) = entry {
                let (blocks, bytes) = per_file(&path)?;
                total_blocks += blocks;
                total_bytes += bytes;
            }
        }
    }
    // total_blocks += blocks;
    // total_bytes += bytes;

    // tell threads that IO is over
    for _i in 0..cfg.no_threads {
        send.send(None)?;
    }

    // merge the data from the workers
    let mut all_the_maps = vec![];
    for h in worker_handler {
        let (map, linecount, fieldcount) = h.join().unwrap();
        total_rowcount += linecount;
        total_fieldcount += fieldcount;
        all_the_maps.push(map);
    }
    sum_maps(&mut main_map, all_the_maps, cfg.verbose);

    // OUTPUT
    // write the data structure
    //

    // restore indexes to users input - really just makes testing slightly easier
    fn re_mod_idx<T>(cfg: &CliCfg, v: T) -> T
    where
        T: std::ops::Sub<Output = T> + From<usize>,
    {
        if !cfg.fullmatch_as_field && cfg.re_str.len() > 0 {
            v - 1.into()
        } else {
            v
        }
    }
    if !cfg.csv_output {
        let celltable = std::cell::RefCell::new(Table::new());
        celltable.borrow_mut().set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
        {
            let mut vcell = vec![];
            if cfg.key_fields.len() > 0 {
                for x in &cfg.key_fields {
                    vcell.push(Cell::new(&format!("k:{}", re_mod_idx(&cfg, *x))));
                }
            } else {
                vcell.push(Cell::new("k:-"));
            }
            if !cfg.no_record_count {
                vcell.push(Cell::new("count"));
            }
            for x in &cfg.sum_fields {
                vcell.push(Cell::new(&format!("s:{}", re_mod_idx(&cfg, *x))));
            }
            for x in &cfg.avg_fields {
                vcell.push(Cell::new(&format!("a:{}", re_mod_idx(&cfg, *x))));
            }
            for x in &cfg.unique_fields {
                vcell.push(Cell::new(&format!("u:{}", re_mod_idx(&cfg, *x))));
            }
            let row = Row::new(vcell);
            celltable.borrow_mut().set_titles(row);
        }

        for (ff, cc) in &main_map {
            let mut vcell = vec![];
            let z1: Vec<&str> = ff.split('|').collect();
            for x in &z1 {
                if x.len() <= 0 {
                    vcell.push(Cell::new(&cfg.empty));
                } else {
                    vcell.push(Cell::new(&x.to_string()));
                }
            }

            if !cfg.no_record_count {
                vcell.push(Cell::new(&format!("{}", cc.count)));
            }
            for x in &cc.sums {
                vcell.push(Cell::new(&format!("{}", x)));
            }
            for x in &cc.avgs {
                if x.1 <= 0 {
                    vcell.push(Cell::new("unknown"));
                } else {
                    vcell.push(Cell::new(&format!("{}", (x.0/(x.1 as f64)) )));
                }
            }
            for x in &cc.distinct {
                vcell.push(Cell::new(&format!("{}", x.len())));
            }
            let row = Row::new(vcell);
            celltable.borrow_mut().add_row(row);
        }

        celltable.borrow_mut().printstd();
    } else {
        {
            let mut vcell = vec![];
            if cfg.key_fields.len() > 0 {
                for x in &cfg.key_fields {
                    vcell.push(format!("k:{}", re_mod_idx(&cfg, *x)));
                }
            } else {
                vcell.push("k:-".to_string());
            }
            if !cfg.no_record_count {
                vcell.push("count".to_string());
            }
            for x in &cfg.sum_fields {
                vcell.push(format!("s:{}", re_mod_idx(&cfg, *x)));
            }
            for x in &cfg.avg_fields {
                vcell.push(format!("a:{}", re_mod_idx(&cfg, *x)));
            }
            for x in &cfg.unique_fields {
                vcell.push(format!("u:{}", re_mod_idx(&cfg, *x)));
            }
            println!("{}", vcell.join(&cfg.od));
        }
        let mut thekeys: Vec<String> = Vec::new();
        for (k, _v) in &main_map {
            thekeys.push(k.clone());
        }
        thekeys.sort_unstable();
        // tain(|ff,cc| {
        for ff in thekeys.iter() {
            let mut vcell = vec![];
            let z1: Vec<String> = ff.split('|').map(|x| x.to_string()).collect();
            for x in &z1 {
                if x.len() <= 0 {
                    vcell.push(format!("{}", cfg.empty));
                } else {
                    vcell.push(format!("{}", x));
                }
            }
            let cc = main_map.get(ff).unwrap();
            if !cfg.no_record_count {
                vcell.push(format!("{}", cc.count));
            }
            for x in &cc.sums {
                vcell.push(format!("{}", x));
            }
            for x in &cc.avgs {
                vcell.push(format!("{}", x.0/(x.1 as f64)));
            }
            for x in &cc.distinct {
                vcell.push(format!("{}", x.len()));
            }
            println!("{}", vcell.join(&cfg.od));
        }
    }

    if cfg.verbose >= 1 || cfg.stats {
        let elapsed = start_f.elapsed();
        let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000_000.0);
        let rate: f64 = (total_bytes as f64 / (1024f64 * 1024f64)) as f64 / sec;
        let elapsedcpu: Duration = startcpu.elapsed();
        let seccpu: f64 = (elapsedcpu.as_secs() as f64) + (elapsedcpu.subsec_nanos() as f64 / 1000_000_000.0);
        eprintln!(
            "rows: {}  fields: {}  rate: {:.2} MB/s  time(sec): {:.3}  cpu(sec): {:.3}  blocks: {}",
            total_rowcount,
            total_fieldcount,
            rate,
            sec,
            seccpu,
            total_blocks
        );
    }
    Ok(())
}

fn worker_re(cfg: &CliCfg, recv: &channel::Receiver<Option<FileChunk>>) -> (MyMap, usize, usize) {
    // return lines / fields
    match _worker_re(cfg, recv) {
        Ok((map, lines, fields)) => return (map, lines, fields),
        Err(e) => {
            let err_msg = format!("Unable to process inner file - likely compressed or not UTF8 text: {}", e);
            panic!(err_msg);
        }
    }
}

fn _worker_re(cfg: &CliCfg, recv: &channel::Receiver<Option<FileChunk>>) -> Result<(MyMap, usize, usize), Box<dyn std::error::Error>> {
    // return lines / fields

    let mut map = MyMap::new();

    let re_str = &cfg.re_str[0];

    let re = match Regex::new(re_str) {
        Err(err) => panic!("Cannot parse regular expression {}, error = {}", re_str, err),
        Ok(r) => r,
    };
    let mut buff = String::with_capacity(256); // dyn buffer
    let mut fieldcount = 0;
    let mut rowcount = 0;
    let mut _skipped = 0;
    loop {
        let fc = match recv.recv().expect("thread failed to get next job from channel") {
            Some(fc) => fc,
            None => {
                break;
            }
        };
        if !cfg.noop_proc {
            for line in fc.block[0..fc.len].lines() {
                let line = line?;
                if let Some(ref line_contains) = cfg.re_line_contains {
                    if !line.contains(line_contains) {
                        if cfg.verbose > 3 {
                            println!("DBG: re_line_contains skip line: {}", line);
                        }
                        continue;
                    }
                }

                if let Some(record) = re.captures(line.as_str()) {
                    fieldcount += store_rec(&mut buff, &line, &record, record.len(), &mut map, &cfg, &mut rowcount);
                } else {
                    _skipped += 1;
                }
            }
        }
    }
    Ok((map, rowcount, fieldcount))
}

fn worker_csv(cfg: &CliCfg, recv: &channel::Receiver<Option<FileChunk>>) -> (MyMap, usize, usize) {
    match _worker_csv(cfg, recv) {
        Ok((map, lines, fields)) => return (map, lines, fields),
        Err(e) => {
            let err_msg = format!("Unable to process inner block - likely compressed or not UTF8 text: {}", e);
            panic!(err_msg);
        }
    }
}

fn _worker_csv(cfg: &CliCfg, recv: &channel::Receiver<Option<FileChunk>>) -> Result<(MyMap, usize, usize), Box<dyn std::error::Error>> {
    // return lines / fields

    let mut map = MyMap::new();

    let mut builder = csv::ReaderBuilder::new();
    //let delimiter = dbg!(cfg.delimiter.expect("delimiter is malformed"));
    builder.delimiter(cfg.delimiter as u8).has_headers(cfg.skip_header).flexible(true);

    let mut buff = String::with_capacity(256); // dyn buffer
    let mut fieldcount = 0;
    let mut rowcount = 0;
    loop {
        let fc = match recv.recv().unwrap() {
            Some(fc) => fc,
            None => {
                break;
            }
        };
        if !cfg.noop_proc {
            let mut recrdr = builder.from_reader(&fc.block[0..fc.len]);
            for record in recrdr.records() {
                let record = record?;
                // let record = match record {
                //     Ok(record) => record,
                //     Err(err) => {eprintln!("error pulling csv record: {}", err); continue;}
                // };
                fieldcount += store_rec(&mut buff, "", &record, record.len(), &mut map, &cfg, &mut rowcount);
            }
        }
    }
    Ok((map, rowcount, fieldcount))
}

fn worker_multi_re(cfg: &CliCfg, recv: &channel::Receiver<Option<FileChunk>>) -> (MyMap, usize, usize) {
    // return lines / fields
    match _worker_multi_re(cfg, recv) {
        Ok((map, lines, fields)) => return (map, lines, fields),
        Err(e) => {
            let err_msg = format!("Unable to process inner file - likely compressed or not UTF8 text: {}", e);
            panic!(err_msg);
        }
    }
}

fn grow_str_vec_or_add(idx: usize, v: &mut Vec<String>, s: &str) {
    if idx < v.len() {
        v[idx].push_str(s);
    } else {
        v.push(String::from(s));
    }
}

fn _worker_multi_re(cfg: &CliCfg, recv: &channel::Receiver<Option<FileChunk>>) -> Result<(MyMap, usize, usize), Box<dyn std::error::Error>> {
    // return lines / fields

    let mut map = MyMap::new();

    let mut re_es = vec![];
    for r in &cfg.re_str {
        re_es.push(Regex::new(&r)?); // .expect(format!("Cannot parse regular expression {}, error = {}", r, err)));
    }

    let mut buff = String::with_capacity(256); // dyn buffer
    let mut fieldcount = 0;
    let mut rowcount = 0;
    let mut _skipped = 0;
    let mut re_curr_idx = 0;
    let mut acc_record: Vec<String> = vec![];

    acc_record.push(String::new()); // push 1st blank whole match

    loop {
        let mut acc_idx = 1usize;

        let fc = match recv.recv().expect("thread failed to get next job from channel") {
            Some(fc) => fc,
            None => {
                break;
            }
        };
        if fc.index <= 0 {
            re_curr_idx = 0;
            for s in &mut acc_record {
                s.clear();
            }
            acc_idx = 1;
        } // start new file - start at first RE

        if !cfg.noop_proc {
            for line in fc.block[0..fc.len].lines() {
                let line = line?;

                let re = &re_es[re_curr_idx];
                if let Some(record) = re.captures(line.as_str()) {
                    for f in record.iter().skip(1) {
                        let f = f.unwrap().as_str();
                        grow_str_vec_or_add(acc_idx, &mut acc_record, f);
                        acc_idx += 1;
                        if cfg.verbose > 2 {
                            eprintln!("mRE MATCHED: {}  REC: {:?}", line, acc_record);
                        }
                    }
                    re_curr_idx += 1;
                    if re_curr_idx >= re_es.len() {
                        fieldcount += store_rec(&mut buff, &line, &acc_record, acc_record.len(), &mut map, &cfg, &mut rowcount);
                        if cfg.verbose > 2 {
                            eprintln!("mRE STORE {}", line);
                        }
                        re_curr_idx = 0;
                        for s in &mut acc_record {
                            s.clear();
                        }
                        acc_idx = 1;
                    }
                } else {
                    _skipped += 1;
                }
            }
        }
    }
    Ok((map, rowcount, fieldcount))
}
use std::string::String;

fn store_rec<T>(ss: &mut String, line: &str, record: &T, rec_len: usize, map: &mut MyMap, cfg: &CliCfg, rowcount: &mut usize) -> usize
where
    T: std::ops::Index<usize> + std::fmt::Debug,
    <T as std::ops::Index<usize>>::Output: AsRef<str>,
{
    //let mut ss: String = String::with_capacity(256);
    ss.clear();

    let mut fieldcount = 0usize;

    if cfg.verbose >= 3 {
        if line.len() > 0 {
            eprintln!("DBG:  {:?}  from: {}", &record, line);
        } else {
            eprintln!("DBG:  {:?}", &record);
        }
    }
    if cfg.key_fields.len() > 0 {
        fieldcount += rec_len;
        for i in 0 .. cfg.key_fields.len() {
            let index = cfg.key_fields[i];
            if index < rec_len {
                ss.push_str(&record[index].as_ref());
            } else {
                ss.push_str("NULL");
            }
            ss.push('|');
        }
        ss.pop();  // remove the trailing | instead of check each iteration
        // we know we get here because of the if above.
    } else {
        ss.push_str("NULL");
    }
    *rowcount += 1;

    let mut brec: &mut KeySum = {
        if let Some(v1) = map.get_mut(ss) {
            v1
        } else {
            let v2 = KeySum::new(cfg.sum_fields.len(), cfg.unique_fields.len(), cfg.avg_fields.len());
            map.insert(ss.clone(), v2);
            // TODO:  gree - just inserted but cannot use it right away instead of doing a lookup again?!!!
            // return v2 or &v2 does not compile
            map.get_mut(ss).unwrap()
        }
    };

    brec.count += 1;

    if cfg.sum_fields.len() > 0 {
        for i in 0 .. cfg.sum_fields.len() {
            let index = cfg.sum_fields[i];
            if index < rec_len {
                let v = &record[index];
                match v.as_ref().parse::<f64>() {
                    Err(_) => {
                        if cfg.verbose >= 1 {
                            eprintln!("error parsing string |{}| as a float for summary index: {} so pretending value is 0", v.as_ref(), index);
                        }
                    }
                    Ok(vv) => brec.sums[i] += vv,
                }
            }
        }
    }

    if cfg.avg_fields.len() > 0 {
        for i in 0 .. cfg.avg_fields.len() {
            let index = cfg.avg_fields[i];
            if index < rec_len {
                let v = &record[index];
                match v.as_ref().parse::<f64>() {
                    Err(_) => {
                        if cfg.verbose >= 1 {
                            eprintln!("error parsing string |{}| as a float for summary index: {} so pretending value is 0", v.as_ref(), index);
                        }
                    }
                    Ok(vv) => {
                        brec.avgs[i].0 += vv;
                        brec.avgs[i].1 += 1;
                    },
                }
            }
        }
    }
    if cfg.unique_fields.len() > 0 {
        for i in 0 .. cfg.unique_fields.len() {
            let index = cfg.unique_fields[i];
            if index < rec_len {
                if !brec.distinct[i].contains(record[index].as_ref()) {
                    brec.distinct[i].insert(record[index].as_ref().to_string());
                }
            }
        }
    }

    fieldcount
}

fn sum_maps(p_map: &mut MyMap, maps: Vec<MyMap>, verbose: usize) {
    let start = Instant::now();
    for i in 0..maps.len() {
        for (k, v) in maps.get(i).unwrap() {
            let v_new = p_map.entry(k.to_string()).or_insert(KeySum::new(v.sums.len(), v.distinct.len(), v.avgs.len()));
            v_new.count += v.count;

            for j in 0..v.sums.len() {
                v_new.sums[j] += v.sums[j];
            }

            for j in 0..v.avgs.len() {
                v_new.avgs[j].0 += v.avgs[j].0;
                v_new.avgs[j].1 += v.avgs[j].1;
            }

            for j in 0..v.distinct.len() {
                for (_ii, u) in v.distinct[j].iter().enumerate() {
                    v_new.distinct[j].insert(u.to_string());
                }
            }
        }
    }
    let end = Instant::now();
    let dur = end - start;
    if verbose > 0 {
        println!("re thread merge maps time: {:?}", dur);
    }
}
