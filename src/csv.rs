#![allow(unused)]
extern crate crossbeam_channel;
extern crate csv;
extern crate num_cpus;
extern crate prettytable;
extern crate regex;

#[macro_use]
extern crate lazy_static;
extern crate structopt;

use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashSet},
    fmt::Display,
    fs,
    fs::OpenOptions,
    io::{prelude::*, ErrorKind},
    ops::Add,
    path::PathBuf,
    process, slice,
    sync::Arc,
    thread,
    time::Instant,
};

use crossbeam_channel as channel;
use regex::Regex;
use structopt::StructOpt;

use prettytable::{cell::Cell, format, row::Row, Table};

type MyMap = BTreeMap<String, KeySum>;

mod gen;

use gen::{greek, io_thread_swizzle, FileChunk};

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

#[derive(Debug)]
struct KeySum {
    count: u64,
    sums: Vec<f64>,
    distinct: Vec<HashSet<String>>,
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
    eprintln!(
        r###"csv [options] file1... fileN
csv [options]
    --help this help
    -i - read from stdin
    -h - data has header so skip first line
    # All the follow field lists are ZERO BASED INDEXING - first field is 0
    -f x,y...z - comma seperated field list of fields to use as a group by
    -s x,y...z - comma seperated field list of files to do a f64 sum of
    -u x,y...z - comma seperated field list of unique or distinct records
    -a turn off human friendly table format - use csv format
    -d input_delimiter - single char
    --od output_delimiter when -a option used, option is a string that defaults to ,
    -v - verbose
    -vv - more verbose
    --nc - do not write record counts
    -r <RE> parse lines using regular expression and use sub groups as fields
    Several -r <RE> used?  Experimental.  Notes:
    If more than one -r RE is specified, then it will switch to multiline mode.
    This will allow only a single RE parser thread and will slow down progress
    significantly, but will create a virtual record across each line that matches.
    They must match in order and only the first match of each will have it's
    sub groups captured and added to the record.  Only when the last RE is matched
    will results be captured, and at this point it will start looking for the first
    RE to match again.
    -t <num> number of worker threads to spawn - default cpu_count maxes out at 12
        This max of 12 can be overridden here but will likely requiring tweaking
        other settings like --block_size_k size and -q size.
    -q <num> number queue-entries between reader and parser threads - default 4 * thread_count
    --block_size_k <num>  size of the job blocks sent to threads
    --noop_proc  do nothing but read blocks and pass to threads - no parsing
        used to measure IO and thread queueing performance possible from main thread
"###
    );
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

fn usage(msg: &str) {
    eprintln!("ERROR: {}", msg);
    //    CliCfg::clap().print_help();
    eprintln!("Use --help to get full help message");
    std::process::exit(1);
    //structopt::clap::App::print_help(&mut cfg);
}

fn get_default_thread_no() -> usize {
    let cpu_count = num_cpus::get();
    let mut default_thread = cpu_count;
    if cpu_count > 12 {
        default_thread = 12;
    }
    default_thread
}

fn get_default_queue_size() -> usize {
    get_default_thread_no() * 4
}

lazy_static! {
    static ref DEFAULT_THREAD_NO: String = get_default_thread_no().to_string();
    static ref DEFAULT_QUEUE_SIZE: String = get_default_queue_size().to_string();
}

#[derive(Debug, StructOpt, Clone)]
#[structopt(
    name = "csv",
    raw(setting = "structopt::clap::AppSettings::DeriveDisplayOrder"),
    about = "Perform sql-group-bys on csv files or text files."
)]
struct CliCfg {
    #[structopt(short = "f", long = "groupby_fields", raw(use_delimiter = "true"))]
    /// Fields that will act as group by keys - base index 0
    key_fields: Vec<usize>,
    #[structopt(short = "u", long = "unique_values", raw(use_delimiter = "true"))]
    /// Fields to count distinct - base index 0
    unique_fields: Vec<usize>,
    #[structopt(short = "s", long = "sum_values", raw(use_delimiter = "true"))]
    /// Field to sum as float64s - base index 0
    sum_fields: Vec<usize>,
    #[structopt(short = "r", long = "regex", conflicts_with = "d")]
    /// Regex mode regular expression
    ///
    /// Several -r <RE> used?  Experimental.  Notes:
    /// If more than one -r RE is specified, then it will switch to multiline mode.
    /// This will allow only a single RE parser thread and will slow down progress
    /// significantly, but will create a virtual record across each line that matches.
    /// They must match in order and only the first match of each will have it's
    /// sub groups captured and added to the record.  Only when the last RE is matched
    /// will results be captured, and at this point it will start looking for the first
    /// RE to match again.
    re_str: Vec<String>,
    #[structopt(long = "--fullmatch_as_field")]
    /// Using whole regex match as 0th field - adds 1 to all others
    fullmatch_as_field: bool,
    #[structopt(short = "d", long = "input_delimiter", default_value = ",", conflicts_with = "r")]
    /// Delimiter if in csv mode
    delimiter: char,
    #[structopt(short = "od", long = "output_delimiter", default_value = ",")]
    /// Output delimiter for written summaries
    od: String,
    #[structopt(short = "c", long = "csv_output")]
    csv_output: bool,
    #[structopt(short = "v", parse(from_occurrences))]
    /// Verbosity - use more than one v for greater detail
    verbose: usize,
    #[structopt(long = "skip_header")]
    /// Skip the first (header) line of input for each file or all of stdin
    skip_header: bool,
    #[structopt(long = "no_record_count")]
    /// Do not write counts for each group by key tuple
    no_record_count: bool,
    #[structopt(short = "e", long = "empty_string", default_value = "")]
    /// Empty string substitution - default is "" empty/nothing/notta
    empty: String,
    #[structopt(short = "t", long = "worker_threads", raw(default_value = "&DEFAULT_THREAD_NO"))]
    /// Number of csv or re parsing threads - defaults to up to 12 if you have that many CPUs
    no_threads: u64,
    #[structopt(short = "q", long = "queue_size", raw(default_value = "&DEFAULT_QUEUE_SIZE"))]
    /// Length of queue between IO block reading and parsing threads
    thread_qsize: usize,
    #[structopt(long = "noop_proc")]
    /// do no real work - used for testing IO
    noop_proc: bool,
    /// Size of the IO block "K" (1024 bytes) used between reading thread and parser threads
    #[structopt(long = "block_size_k", default_value = "256")]
    block_size_k: usize,
    /// Block size for IO to queue
    #[structopt(long = "block_size_B", default_value = "0")]
    /// Block size for IO to queue used for testing really small blocks
    /// and possible related that might occurr
    block_size_b: usize,
    /// list of input files, defaults to stdin
    #[structopt(short = "i", name = "FILE", parse(from_os_str))]
    files: Vec<PathBuf>,
}

fn csv() -> Result<(), std::io::Error> {
    // CliCfg is made immutable for thread saftey - does not need to be
    // changed after a this point.  But, we must using Arc in combination
    // to work around the scope issue.
    // Arc prevents the unneeded copy for cloning when passing to thread.
    // Threads need static scope OR their own copy of a thing
    // The scope inside the new allow the config to be mutable
    // but then put into to th Arc as immutable
    let cfg = Arc::new({
        let mut cfg: CliCfg = CliCfg::from_args();
        let cpu_count = num_cpus::get();
        let mut default_thread = cpu_count;
        if cpu_count > 12 {
            default_thread = 12;
        }
        let mut default_q_size = 4 * default_thread;
        // setup intelligent defaults for ones not set
        // if cfg.block_size_k == 0 { cfg.block_size_k = 256; }
        //        if cfg.no_threads == 0 { cfg.no_threads = default_thread; }
        if cfg.re_str.len() > 1 {
            cfg.no_threads = 1;
            if cfg.verbose >= 1 {
                eprintln!("Override thread number to 1 since you have multiple [{}] REs listed ", cfg.re_str.len());
            }
        }
        if cfg.re_str.len() > 0 {
            if !cfg.fullmatch_as_field {
            cfg.key_fields.iter_mut().for_each(|x| *x += 1);
            cfg.sum_fields.iter_mut().for_each(|x| *x += 1);
            cfg.unique_fields.iter_mut().for_each(|x| *x += 1);
            }
                eprintln!("Using full regex match as 0th field")
        } 
        if cfg.verbose >= 2 {
            eprintln!("{:#?}", cfg);
        }
        cfg
    });

    // just check RE early for problems
    for re in &cfg.re_str {
        match Regex::new(re) {
            Err(err) => panic!("Cannot parse regular expression {}, error = {}", re, err),
            Ok(r) => r,
        };
    }

    let mut total_rowcount = 0usize;
    let mut total_fieldcount = 0usize;
    let mut total_blocks = 0usize;
    let mut total_bytes = 0usize;
    let start_f = Instant::now();

    if cfg.key_fields.len() <= 0 && cfg.sum_fields.len() <= 0 && cfg.unique_fields.len() <= 0 {
        usage("No work to do! - you should specify at least one or more field options");
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
                .name(format!("worker_re{}", no_threads))
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
    if cfg.files.len() <= 0 {
        if cfg.verbose > 0 {
            eprintln!("<<< reading from stdin");
        }
        let stdin = std::io::stdin();
        let mut handle = stdin; // .lock();
        let (blocks, bytes) = io_thread_swizzle(&"STDIO".to_string(), block_size, cfg.verbose, &mut handle, &send)?;
        total_bytes += bytes;
        total_blocks += blocks;
        
    } else {
        let filelist = cfg.files.clone();
        for filename in filelist.into_iter() {
            // let metadata = fs::metadata(&filename)?;
            let metadata = match fs::metadata(&filename) {
                Ok(m) => m,
                Err(err) => {
                    eprintln!("skipping file \"{}\", could not get stats on it, cause: {}", filename.display(), err);
                    continue;
                }
            };

            if cfg.verbose >= 1 {
                eprintln!("file: {}", filename.display());
            }
            let mut f = match OpenOptions::new().read(true).write(false).create(false).open(&filename) {
                Ok(f) => f,
                Err(e) => panic!("cannot open file \"{}\" due to this error: {}", filename.display(), e),
            };
            let (blocks, bytes) = io_thread_swizzle(&filename.display(), block_size, cfg.verbose, &mut f, &send)?;
            total_blocks += blocks;
            total_bytes += bytes;
        }
    }

    // tell threads that IO is over
    for i in 0..cfg.no_threads {
        send.send(None);
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
            for x in &cfg.unique_fields {
                vcell.push(Cell::new(&format!("u:{}", re_mod_idx(&cfg, *x))));
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

            if !cfg.no_record_count {
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
            for x in &cfg.unique_fields {
                vcell.push(format!("u:{}", re_mod_idx(&cfg, *x)));
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
            if !cfg.no_record_count {
                vcell.push(format!("{}", cc.count));
            }
            for x in &cc.sums {
                vcell.push(format!("{}", x));
            }
            for x in &cc.distinct {
                vcell.push(format!("{}", x.len()));
            }
            println!("{}", vcell.join(&cfg.od));
        }
    }

    if cfg.verbose >= 1 {
        let elapsed = start_f.elapsed();
        let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000_000.0);
        let rate: f64 = (total_bytes as f64 / (1024f64 * 1024f64)) as f64 / sec;
        eprintln!("rows: {}  fields: {}  rate: {:.2}MB/s rt: {}s blocks: {}", total_rowcount, total_fieldcount, rate, elapsed.as_secs(), total_blocks);
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
    let mut skipped = 0;
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
                let record = record.expect("thread failed to parse block as csv records");
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
        re_es.push(match Regex::new(&r) {
            Err(err) => panic!("Cannot parse regular expression {}, error = {}", r, err),
            Ok(r) => r,
        })
    }
    let mut buff = String::with_capacity(256); // dyn buffer
    let mut fieldcount = 0;
    let mut rowcount = 0;
    let mut skipped = 0;
    let mut re_curr_idx = 0;
    let mut acc_record: Vec<String> = vec![];
    let acc_limit = 20;

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
                    skipped += 1;
                }
            }
        }
    }
    Ok((map, rowcount, fieldcount))
}

#[inline]
fn store_rec<T>(ss: &mut String, line: &str, record: &T, rec_len: usize, map: &mut MyMap, cfg: &CliCfg, rowcount: &mut usize) -> usize
where
    T: std::ops::Index<usize> + std::fmt::Debug,
    <T as std::ops::Index<usize>>::Output: ToString,
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
        let v = map.entry(ss.clone()).or_insert(KeySum {
            count: 0,
            sums: Vec::new(),
            distinct: Vec::new(),
        });
        v.count += 1;
        // println!("sum on: {:?}", sum_grab);
        if v.sums.len() <= 0 {
            for f in &sum_grab {
                v.sums.push(*f);
            }
        } else {
            for (i, f) in sum_grab.iter().enumerate() {
                v.sums[i] += *f;
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
            let v_new = p_map.entry(k.to_string()).or_insert(KeySum {
                count: 0,
                sums: Vec::new(),
                distinct: Vec::new(),
            });
            v_new.count += v.count;

            for j in 0..v.sums.len() {
                if v_new.sums.len() > 0 {
                    v_new.sums[j] += v.sums[j];
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
