#![allow(unused)]
extern crate num_cpus;
extern crate csv;
extern crate prettytable;
extern crate regex;
extern crate chan;
extern crate crossbeam_channel;
// 0.2.5;
extern crate crossbeam;


use chan::{Sender, Receiver};

use crossbeam_channel as channel;
use crossbeam::scope;

use std::io::prelude::*;
use std::fs::OpenOptions;
use std::io::Cursor;
use std::fs;
use std::process;
use std::string::String;
use std::env::args;
use std::io::Read;
use std::io::BufReader;
use std::io::BufRead;
use std::io::Error;
use std::io::ErrorKind;
use std::fs::File;

use std::borrow::Borrow;
use std::path::Path;
use std::thread;
use std::sync::{Arc, RwLock, Mutex};

use std::io::Lines;
use std::time::Instant;

use std::collections::HashMap;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::slice;

use prettytable::Table;
use prettytable::row::Row;
use prettytable::cell::Cell;
use prettytable::format;

type MyMap = BTreeMap<String, KeySum>;

use regex::Regex;

mod gen;

use gen::greek;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

#[derive(Debug)]
struct KeySum {
    count: u64,
    sums: Vec<f64>,
    distinct: Vec<HashSet<String>>,
}

#[derive(Debug)]
struct FileChunk {
    block: Vec<u8>,
    len: usize,
}


// main/cli/csv thread-spawn read->thread  { spawn block { re | csv } }


fn main() {
    if let Err(err) = csv() {
        match err.kind() {
            ErrorKind::BrokenPipe => println!("broken pipe"),
            _ => {
                eprintln!("error: of another kind {:?}", &err);
                std::process::exit(1);
            }
        }
    }
}

fn help(msg: &str) {
    if msg.len() > 0 {
        eprintln!("error: {}", msg);
    }
    eprintln!(r###"csv [options] file1... fileN
csv [options] -i # read from standard input
    --help this help
    -i - read from stdin
    -h - data has header so skip first line
    # All the follow field lists are ZERO BASED INDEXING - first field is 0
    -f x,y...z - comma seperated field list of fields to use as a group by
    -s x,y...z - comma seperated field list of files to do a f64 sum of
    -u x,y...z - comma seperated field list of unique or distinct records
    -a turn off human friendly table format - use csv format
    -d input_delimiter - single char
    -v - verbose
    -vv - more verbose
    --nc - do not write record counts
    -r <RE> parse lines using regular expression and use sub groups as fields
    -t <num> number of worker threads to spawn - default cpu_count upto 12
    -q <num> number queue-entries between reader and parser threads - default 4 * thread_count
    --noop_proc  do nothing but read blocks and pass to threads - no parsing
        used to measure IO and thread queueing performance possible from main thread
    --block_size_k <num>  size of the job blocks sent to threads
"###);
    eprintln!("version: {}", env!("CARGO_PKG_VERSION"));
    eprintln!("CARGO_MANIFEST_DIR: {}", env!("CARGO_MANIFEST_DIR"));
    eprintln!("CARGO_PKG_VERSION: {}", env!("CARGO_PKG_VERSION"));
    eprintln!("CARGO_PKG_HOMEPAGE: {}", env!("CARGO_PKG_HOMEPAGE"));
    // if built_info::GIT_VERSION.is_some() {
    //     println!("git rev: {}  build time: {}", built_info::GIT_VERSION.unwrap(),built_info::BUILT_TIME_UTC);
    //
    // }
    process::exit(1);
}


#[derive(Debug, Clone)]
struct CliCfg<> {
    key_fields: Vec<usize>,
    unique_fields: Vec<usize>,
    sum_fields: Vec<usize>,
    delimiter: char,
    od: String,
    auto_align: bool,
    verbose: usize,
    hasheader: bool,
    write_record_count: bool,
    re_str: String,
    read_stdin: bool,
    empty: String,
    thread_no: usize,
    thread_qsize: usize,
    noop_proc: bool,
    block_size: usize,
}



fn csv() -> Result<(), std::io::Error> {

    let cpu_count = num_cpus::get();

    let mut default_thread = cpu_count;

    if cpu_count > 12 {
        default_thread = 12;
    }

    let mut default_q_size = 4 * default_thread;

    let mut cfg: CliCfg = CliCfg {
        key_fields: vec![],
        unique_fields: vec![],
        sum_fields: vec![],
        delimiter: ',',
        od: String::new(),
        auto_align: true,
        verbose: 0,
        hasheader: false,
        write_record_count: true,
        re_str: String::new(),
        read_stdin: false,
        empty: String::new(),
        thread_no: default_thread,
        thread_qsize: default_q_size,
        noop_proc: false,
        block_size: 256*1024,
    };
    let argv: Vec<String> = args().skip(1).map(|x| x).collect();
    let filelist = &mut vec![];

    let mut i = 0;

    if argv.len() <= 1 {
        help("no command line options used");
    }

    while i < argv.len() {
        match &argv[i][..] {
            "--help" => { // field list processing
                help("command line requested help info");
            }
            "-i" => {
                cfg.read_stdin = true;
            }
            "-a" => {
                cfg.auto_align = false;
            }
            "-f" => { // field list processing
                i += 1;
                cfg.key_fields.splice(.., (&argv[i][..]).split(",").map(|x| x.parse::<usize>().expect("cannot parse number list for -f")));
            }
            "-s" => { // field list processing
                i += 1;
                cfg.sum_fields.splice(.., (&argv[i][..]).split(",").map(|x| x.parse::<usize>().expect("cannot parse number list for -s")));
            }
            "-u" => { // unique count AsMut
                i += 1;
                cfg.unique_fields.splice(.., (&argv[i][..]).split(",").map(|x| x.parse::<usize>().expect("cannot parse number list for -u")));
            }
            "--od" => { // unique count AsMut
                i += 1;
                cfg.od.push_str(&argv[i]);
            }
            "-r" => { // unique count AsMut
                i += 1;
                cfg.re_str.push_str(&argv[i]);
            }
            "-d" => { // unique count AsMut
                i += 1;
                cfg.delimiter = argv[i].as_bytes()[0] as char;
            }
            "-v" => { // write out AsMut
                cfg.verbose = 1;
                eprintln!("writing stats and other info ON")
            }
            "-vv" => { // write out AsMut
                cfg.verbose = 2;
                eprintln!("writing stats and other debug info ON")
            }
            "-vvv" => { // write out AsMut
                cfg.verbose = 3;
                eprintln!("writing stats and other debug info ON")
            }
            "-vvvv" => { // write out AsMut
                cfg.verbose = 4;
                eprintln!("writing stats and other debug info ON")
            }
            "-t" => { // thread count
                i += 1;
                cfg.thread_no = argv[i].parse::<usize>().expect("cannot parse number of -t option - number must be 1 or more");
            }
            "-q" => { // qsize count
                i += 1;
                cfg.thread_qsize = argv[i].parse::<usize>().expect("cannot parse number of -q option - number must be 1 or more");
            }
            "-h" => { // write out AsMut
                cfg.hasheader = true;
            }
            "--nc" => { // just write the keys and not the row count
                cfg.write_record_count = false;
            }
            "--noop_proc" => { // do nothing real in the re thread
                cfg.noop_proc = true;
            }
            "--block_size_k" => { // do nothing real in the re thread
                i += 1;
                cfg.block_size = argv[i].parse::<usize>().unwrap() * 1024;
            }
            x => {
                if cfg.verbose >= 1 { eprintln!("adding filename {} to scan", x); }
                filelist.push(x);
            }
        }

        i += 1;
    }
    // if key_fields.len() <= 0 {
    //     help("missing key fields - you must specify -f option with something or no summaries can be made");
    // }

    // just check RE early for problems
    match Regex::new(&cfg.re_str) {
        Err(err) => panic!("Cannot parse regular expression {}, error = {}", cfg.re_str, err),
        Ok(r) => r,
    };

    if cfg.re_str.len() > 0 {
        cfg.key_fields.iter_mut().for_each(|x| *x += 1);
        cfg.sum_fields.iter_mut().for_each(|x| *x += 1);
        cfg.unique_fields.iter_mut().for_each(|x| *x += 1);
    }

    if cfg.verbose >= 1 {
        eprintln!("\tdelimiter: {}", cfg.delimiter);
        eprintln!("\theader: {}", cfg.hasheader);
        eprintln!("\tkey_fields: {:?}  len={}", cfg.key_fields, cfg.key_fields.len());
        eprintln!("\tsum_fields: {:?}  len={}", cfg.sum_fields, cfg.sum_fields.len());
        eprintln!("\tunique_fields: {:?}", cfg.unique_fields);
        eprintln!("\tfile list {:?}", filelist);
        eprintln!("\tre {}", cfg.re_str);
        eprintln!("\tmax thread queue size {}", cfg.thread_qsize);
        eprintln!("\tno of working threads {}", cfg.thread_no);
        if cfg.noop_proc {
            eprintln!("noop_proc - just read lines or parse csv");
        }
        if filelist.len() <= 0 {
            eprintln!("\tprocessing stdin");
        }
    }

    let mut total_rowcount = 0usize;
    let mut total_fieldcount = 0usize;
    let mut total_bytes = 0usize;
    let start_f = Instant::now();

    if filelist.len() <= 0 && !cfg.read_stdin {
        help("either use stdin via -i option or put files on command line");
    }
    let mut main_map = MyMap::new();

    /////////////////////////////////////////////////////////////////////////////////////////////////


    let mut worker_handler = vec![];
    let (send, recv): (channel::Sender<Option<FileChunk>>, channel::Receiver<Option<FileChunk>>) = channel::bounded(cfg.thread_qsize);

    for thrno in 0..cfg.thread_no {
        let cfg = cfg.clone();
        let clone_recv = recv.clone();

        let h = if cfg.re_str.len() > 0 {
            thread::Builder::new().name(format!("worker_re{}",thrno)).spawn(move ||worker_re(&cfg, &clone_recv)).unwrap()
        } else {
            thread::Builder::new().name(format!("worker_csv{}",thrno)).spawn(move ||worker_csv(&cfg, &clone_recv)).unwrap()
        };
        worker_handler.push(h);
    }


    // SETUP IO
    //
    // forward all IO to the block queue
    //
    if filelist.len() <= 0 && cfg.read_stdin {
        let stdin = std::io::stdin();
        let mut handle = stdin.lock();
        total_bytes += io_thread_swizzle("STDIO", &cfg, &mut handle, &send)?;
    } else {
        for filename in filelist.into_iter() {
            // let metadata = fs::metadata(&filename)?;
            let metadata = match fs::metadata(&filename) {
                Ok(m) => m,
                Err(err) => {
                    eprintln!("skipping file \"{}\", could not get stats on it, cause: {}", &filename, err);
                    continue;
                }
            };

            if cfg.verbose >= 1 { eprintln!("file: {}", filename); }
            let mut f = match OpenOptions::new()
                .read(true)
                .write(false)
                .create(false)
                .open(&filename)
                {
                    Ok(f) => f,
                    Err(e) => panic!("cannot open file \"{}\" due to this error: {}", filename, e),
                };
            // let mut handle = BufReader::with_capacity(1024 * 1024 * 4, f);
            total_bytes += io_thread_swizzle(filename, &cfg,&mut f, &send)?;
        }
    }

    // tell threads that IO is over
    for i in 0..cfg.thread_no { send.send(None); }

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
    if cfg.auto_align {
        let celltable = std::cell::RefCell::new(Table::new());
        celltable.borrow_mut().set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
        {
            let mut vcell = vec![];
            if cfg.key_fields.len() > 0 {
                for x in cfg.key_fields {
                    vcell.push(Cell::new(&format!("k:{}", &x)));
                }
            } else {
                vcell.push(Cell::new("k:-"));
            }
            if cfg.write_record_count { vcell.push(Cell::new("count")); }
            for x in cfg.sum_fields {
                vcell.push(Cell::new(&format!("s:{}", &x)));
            }
            for x in cfg.unique_fields {
                vcell.push(Cell::new(&format!("u:{}", &x)));
            }
            let mut row = Row::new(vcell);
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

            if cfg.write_record_count {
                vcell.push(Cell::new(&format!("{}", cc.count)));
            }
            for x in &cc.sums {
                vcell.push(Cell::new(&format!("{}", x)));
            }
            for x in &cc.distinct {
                vcell.push(Cell::new(&format!("{}", x.len())));
            }
            let mut row = Row::new(vcell);
            celltable.borrow_mut().add_row(row);
        };

        celltable.borrow_mut().printstd();
    } else {
        {
            let mut vcell = vec![];
            if cfg.key_fields.len() > 0 {
                for x in cfg.key_fields {
                    vcell.push(format!("k:{}", &x));
                }
            } else {
                vcell.push("k:-".to_string());
            }
            if cfg.write_record_count { vcell.push("count".to_string()); }
            for x in cfg.sum_fields {
                vcell.push(format!("s:{}", x));
            }
            for x in cfg.unique_fields {
                vcell.push(format!("u:{}", x));
            }
            println!("{}", vcell.join(&cfg.od));
        }
        let mut thekeys: Vec<String> = Vec::new();
        for (k, v) in &main_map {
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
            if cfg.write_record_count {
                vcell.push(format!("{}", cc.count));
            }
            for x in &cc.sums {
                vcell.push(format!("{}", x));
            }
            for x in &cc.distinct {
                vcell.push(format!("{}", x.len()));
            }
            println!("{}", vcell.join(&cfg.od));
        };
    }

    if cfg.verbose >= 1 {
        let elapsed = start_f.elapsed();
        let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000_000.0);
        let rate: f64 = (total_bytes as f64 / (1024f64 * 1024f64)) as f64 / sec;
        eprintln!("rows: {}  fields: {}  rate: {:.2}MB/s", total_rowcount, total_fieldcount, rate);
    }
    Ok(())
}

fn worker_re(cfg: &CliCfg, recv: &channel::Receiver<Option<FileChunk>>) -> (MyMap, usize, usize) { // return lines / fields
    match _worker_re(cfg, recv) {
        Ok((map, lines, fields)) => return (map,lines,fields),
        Err(e) => {
            let err_msg = format!("Unable to process inner file - likely compressed or not UTF8 text: {}",e);
            panic!(err_msg);
        }
    }
}

fn _worker_re(cfg: &CliCfg, recv: &channel::Receiver<Option<FileChunk>>) -> Result<(MyMap, usize, usize), Box<std::error::Error>> { // return lines / fields

    let mut map = MyMap::new();

    let re = match Regex::new(&cfg.re_str) {
        Err(err) => panic!("Cannot parse regular expression {}, error = {}", cfg.re_str, err),
        Ok(r) => r,
    };
    let mut buff = String::with_capacity(256); // dyn buffer
    let mut fieldcount = 0;
    let mut rowcount=0;
    let mut skipped = 0;
    loop {
        let fc = match recv.recv().expect("thread failed to get next job from channel") {
            Some(fc) => fc,
            None => { break; }
        };
        if !cfg.noop_proc {
            for line in fc.block[0..fc.len].lines() {
                let line = line?;
                if let Some(record) = re.captures(line.as_str()) {
                    fieldcount += store_rec(&mut buff, &line, &record, record.len(), &mut map, &cfg, &mut rowcount);
                } else {
                    skipped += 1;
                }
            }
        }
    }
    Ok((map, rowcount, fieldcount))
}

fn worker_csv(cfg: &CliCfg, recv: &channel::Receiver<Option<FileChunk>>)  -> (MyMap, usize, usize) {
    match _worker_csv(cfg, recv) {
        Ok((map, lines, fields)) => return (map,lines,fields),
        Err(e) => {
            let err_msg = format!("Unable to process inner block - likely compressed or not UTF8 text: {}",e);
            panic!(err_msg);
        }
    }
}

fn _worker_csv(cfg: &CliCfg, recv: &channel::Receiver<Option<FileChunk>>)  -> Result<(MyMap, usize, usize), Box<std::error::Error>>  { // return lines / fields

    let mut map = MyMap::new();

    let mut builder = csv::ReaderBuilder::new();
    builder.delimiter(cfg.delimiter as u8).has_headers(cfg.hasheader).flexible(true);

    let mut buff = String::with_capacity(256); // dyn buffer
    let mut fieldcount = 0;
    let mut rowcount = 0;
    loop {
        let fc = match recv.recv().unwrap() {
            Some(fc) => fc,
            None => { break; }
        };
        if !cfg.noop_proc {
            let mut recrdr = builder.from_reader(&fc.block[0..fc.len]);
            for record in recrdr.records() {
                let record = record.expect("thread failed to parse block as csv records");
                fieldcount += store_rec(&mut buff, "", &record, record.len(), &mut map, &cfg, &mut rowcount);
            }
        }
    }
    Ok((map, rowcount, fieldcount))
}

#[inline]
fn store_rec<T>(ss: &mut String, line: &str, record: &T, rec_len: usize, map: &mut MyMap, cfg: &CliCfg, rowcount: &mut usize) -> usize
    where T: std::ops::Index<usize> + std::fmt::Debug,
          <T as std::ops::Index<usize>>::Output: ToString
{
    //let mut ss: String = String::with_capacity(256);
    ss.clear();

    let mut fieldcount = 0usize;

    let mut sum_grab = vec![];
    let mut uni_grab = vec![];

    let mut local_count = 0;

    if cfg.verbose >= 3 {
        if line.len() > 0 {
            eprintln!("DBG:  {:?}  from: {}", &record, line);
        } else {
            eprintln!("DBG:  {:?}", &record);
        }
    }
    let mut i = 0;
    if cfg.key_fields.len() > 0 {
        fieldcount += rec_len;
        while i < cfg.key_fields.len() {
            let index = cfg.key_fields[i];
            if index < rec_len {
                ss.push_str(&record[index].to_string());
            } else {
                ss.push_str("NULL");
            }
            if i != cfg.key_fields.len() - 1 {
                ss.push('|');
            }
            i += 1;
        }
    } else {
        ss.push_str("NULL");
        //println!("no match: {}", line);
    }

    // println!("{:?}", ss);

    if cfg.sum_fields.len() > 0 {
        sum_grab.truncate(0);
        i = 0;
        while i < cfg.sum_fields.len() {
            let index = cfg.sum_fields[i];
            if index < rec_len {
                let v = &record[index];
                match v.to_string().parse::<f64>() {
                    Err(_) => {
                        if cfg.verbose >= 1 {
                            eprintln!("error parsing string |{}| as a float for summary index: {} so pretending value is 0", v.to_string(), index);
                        }
                        sum_grab.push(0f64);
                    }
                    Ok(vv) => sum_grab.push(vv),
                }
            } else {
                sum_grab.push(0f64);
            }
            i += 1;
        }
    }

    if cfg.unique_fields.len() > 0 {
        uni_grab.truncate(0);
        i = 0;
        while i < cfg.unique_fields.len() {
            let index = cfg.unique_fields[i];
            if index < rec_len {
                uni_grab.push(record[index].to_string());
            } else {
                uni_grab.push("NULL".to_string());
            }
            i += 1;
        }
    }

    if ss.len() > 0 {
        *rowcount += 1;
        let v = map.entry(ss.clone()).or_insert(KeySum { count: 0, sums: Vec::new(), distinct: Vec::new() });
        v.count += 1;
        // println!("sum on: {:?}", sum_grab);
        if v.sums.len() <= 0 {
            for f in &sum_grab {
                v.sums.push(*f);
            }
        } else {
            for (i, f) in sum_grab.iter().enumerate() {
                v.sums[i] += f;
            }
        }

        if uni_grab.len() > 0 {
            while v.distinct.len() < uni_grab.len() {
                v.distinct.push(HashSet::new());
            }
            for (i, u) in uni_grab.iter().enumerate() {
                v.distinct[i].insert(u.to_string());
            }
        }
    }
    fieldcount
}


fn sum_maps(p_map: &mut MyMap, maps: Vec<MyMap>, verbose: usize) {
    let start = Instant::now();
    for i in 0..maps.len() {
        for (k, v) in maps.get(i).unwrap() {
            let v_new = p_map.entry(k.to_string()).or_insert(KeySum { count: 0, sums: Vec::new(), distinct: Vec::new() });
            v_new.count += v.count;

            for j in 0..v.sums.len() {
                if v_new.sums.len() > j {
                    v_new.sums[j] += v_new.sums[j] + v.sums[j];
                } else {
                    v_new.sums.push(v.sums[j]);
                }
            }

            while v_new.distinct.len() < v.distinct.len() {
                v_new.distinct.push(HashSet::new());
            }
            for j in 0..v.distinct.len() {
                for (ii, u) in v.distinct[j].iter().enumerate() {
                    v_new.distinct[j].insert(u.to_string());
                }
            }
        }
    }
    let end = Instant::now();
    let dur = (end - start);
    if verbose > 0 {
        println!("re thread merge maps time: {:?}", dur);
    }
}

fn io_thread_swizzle(currfilename: &str, cfg: &CliCfg, handle: &mut Read, send: & channel::Sender<Option<FileChunk>>) -> Result<usize, std::io::Error>  {
    let mut split_slop = 0;
    let mut block_count = 0;
    let mut bytes = 0;

    let mut holdover = vec![0u8; cfg.block_size];
    let mut left_len = 0;
    let mut last_left_len;
    let mut curr_pos = 0usize;
    loop {
        let mut block = vec![0u8; cfg.block_size];
        if left_len > 0 {
            split_slop += left_len;
            block[0..left_len].copy_from_slice(&holdover[0..left_len]);
        }

        let sz = handle.read(&mut block[left_len..])?;

        let mut end = 0;
        let mut found = false;

        for i in (0..sz+left_len).rev() {
            let c = block[i];
            if c == b'\r' || c == b'\n' {
                end = i;
                found =true;
                break;
            }
        }

        curr_pos += sz;
        if sz > 0 {
            block_count += 1;
            if !found {
                panic!("Cannot find end line marker in current block at pos {} from file {}", curr_pos, currfilename);
            }
            last_left_len = left_len;
            left_len = (sz + last_left_len) - (end + 1);
            holdover[0..left_len].copy_from_slice(&block[end + 1..sz + last_left_len]);

            send.send(Some(FileChunk{block: block, len: end+1}));
            bytes += end;

        } else {
            send.send(Some(FileChunk{block: block, len: left_len}));
            bytes += end;

            break;
        }
    }

    Ok(bytes)
}
