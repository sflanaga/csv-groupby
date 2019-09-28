#![allow(dead_code)]
#![allow(unused_imports)]

use atty::Stream;
use pcre2::bytes::{CaptureLocations as CaptureLocations_pcre2, Captures as Captures_pcre2, Regex as Regex_pre2};
use prettytable::{cell::Cell, format, row::Row, Table};
use regex::{CaptureLocations, Regex};
use std::alloc::System;
use std::{
    collections::{BTreeMap, HashSet},
    io::prelude::*,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

type MyMap = BTreeMap<String, KeySum>;

mod cli;
mod gen;
mod mem;
mod testre;

use cli::{get_cli, CliCfg};
use colored::Colorize;
use cpu_time::ProcessTime;
use gen::{distro_format, io_thread_slicer, mem_metric_digit, per_file_thread, FileSlice, IoSlicerStatus};
use glob::{glob_with, MatchOptions};
use testre::testre;

use mem::{CounterAtomicUsize, CounterTlsToAtomicUsize, CounterUsize, GetAlloc};
use std::collections::HashMap;
use std::ops::Index;
//use bstr::ByteSlice;

//pub static GLOBAL_TRACKER: std::alloc::System = std::alloc::System;
//pub static GLOBAL_TRACKER: System = System; //CounterAtomicUsize = CounterAtomicUsize;
#[cfg(target_os = "linux")]
#[global_allocator]
pub static GLOBAL_TRACKER: CounterTlsToAtomicUsize = CounterTlsToAtomicUsize;
//pub static GLOBAL_TRACKER: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[cfg(target_os = "windows")]
#[global_allocator]
pub static GLOBAL_TRACKER: CounterTlsToAtomicUsize = CounterTlsToAtomicUsize;

#[derive(Debug)]
struct KeySum {
    count: u64,
    sums: Vec<f64>,
    avgs: Vec<(f64, usize)>,
    distinct: Vec<HashMap<String, usize>>,
}

impl KeySum {
    pub fn new(sum_len: usize, dist_len: usize, avg_len: usize) -> KeySum {
        KeySum {
            count: 0,
            sums: vec![0f64; sum_len],
            avgs: vec![(0f64, 0usize); avg_len],
            distinct: {
                let mut v = Vec::with_capacity(dist_len);
                for _ in 0..dist_len {
                    v.push(HashMap::new());
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

fn stat_ticker(thread_stopper: Arc<AtomicBool>, status: &mut Arc<IoSlicerStatus>, send: &crossbeam_channel::Sender<Option<FileSlice>>) {
    let start_f = Instant::now();
    let startcpu = ProcessTime::now();
    loop {
        thread::sleep(Duration::from_millis(250));
        if thread_stopper.load(Ordering::Relaxed) {
            break;
        }
        let total_bytes = status.bytes.load(std::sync::atomic::Ordering::Relaxed);
        let elapsed = start_f.elapsed();
        let sec: f64 = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000_000.0);
        let rate = (total_bytes as f64 / sec) as usize;
        let elapsedcpu: Duration = startcpu.elapsed();
        let seccpu: f64 = (elapsedcpu.as_secs() as f64) + (elapsedcpu.subsec_nanos() as f64 / 1000_000_000.0);
        {
            let curr_file = status.curr_file.lock().unwrap();
            eprint!(
                "{}",
                format!(
                    " q: {}  {}  rate: {}/s at  time(sec): {:.3}  cpu(sec): {:.3}  curr: {}  mem: {}                    \r",
                    send.len(),
                    mem_metric_digit(total_bytes, 4),
                    mem_metric_digit(rate, 4),
                    sec,
                    seccpu,
                    curr_file,
                    mem_metric_digit(GLOBAL_TRACKER.get_alloc(), 4),
                )
                .green()
            );
        }
    }
    eprint!("                                                                             \r");
}
fn type_name_of<T: core::any::Any>(t: &T) -> &str {
    return std::any::type_name::<T>();
}


fn csv() -> Result<(), Box<dyn std::error::Error>> {
    let start_f = Instant::now();
    let startcpu = ProcessTime::now();

    let cfg = get_cli()?;

    if cfg.verbose >= 1 {
        eprintln!("Global allocator / tracker: {}", type_name_of(&GLOBAL_TRACKER));
    }

    if cfg.verbose >= 1 && pcre2::is_jit_available() {
        eprintln!("pcre2 JIT is available");
    }

    let mut total_rowcount = 0usize;
    let mut total_fieldcount = 0usize;
    let mut total_blocks = 0usize;
    let mut total_bytes = 0usize;

    if cfg.testre.is_some() {
        testre(&cfg)?;
        return Ok(());
    }
    let mut main_map = MyMap::new();
    if cfg.verbose >= 1 {
	eprintln!("map type: {}", type_name_of(&main_map));
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////

    let mut worker_handlers = vec![];
    let (send_fileslice, recv_fileslice): (crossbeam_channel::Sender<Option<FileSlice>>, crossbeam_channel::Receiver<Option<FileSlice>>) =
        crossbeam_channel::bounded(cfg.thread_qsize);

    let do_stdin = cfg.files.len() <= 0 && cfg.glob.is_none();
    let mut io_status = Arc::new(IoSlicerStatus::new());
    let block_size = match cfg.block_size_b {
        0 => cfg.block_size_k * 1024,
        _ => cfg.block_size_b,
    };

    let (send_blocks, recv_blocks): (crossbeam_channel::Sender<Vec<u8>>, crossbeam_channel::Receiver<Vec<u8>>) = crossbeam_channel::unbounded();
    if cfg.recycle_io_blocks {
        for _ in 0..cfg.thread_qsize {
            let ablock = vec![0u8; block_size];
            send_blocks.send(ablock)?;
        }
    }

    // start processing threads
    for no_threads in 0..cfg.no_threads {
        let cfg = cfg.clone();
        let clone_recv_fileslice = recv_fileslice.clone();
        let clone_senc_blocks = send_blocks.clone();
        let h = match cfg.re_str.len() {
            0 => thread::Builder::new()
                .name(format!("worker_csv{}", no_threads))
                .spawn(move || worker_csv(&clone_senc_blocks, &cfg, &clone_recv_fileslice))
                .unwrap(),
            1 => thread::Builder::new()
                .name(format!("worker_re{}", no_threads))
                .spawn(move || worker_re(&clone_senc_blocks, &cfg, &clone_recv_fileslice))
                .unwrap(),
            _ => thread::Builder::new()
                .name(format!("wr_mul_re{}", no_threads))
                .spawn(move || worker_multi_re(&clone_senc_blocks, &cfg, &clone_recv_fileslice))
                .unwrap(),
        };
        worker_handlers.push(h);
    }

    // SETUP IO

    // IO slicer threads
    let mut io_handler = vec![];
    let (send_pathbuff, recv_pathbuff): (crossbeam_channel::Sender<Option<PathBuf>>, crossbeam_channel::Receiver<Option<PathBuf>>) = crossbeam_channel::bounded(cfg.thread_qsize);
    for no_threads in 0..cfg.no_threads {
        let cfg = cfg.clone();
        let clone_recv_pathbuff = recv_pathbuff.clone();
        let clone_send_fileslice = send_fileslice.clone();
        let clone_recv_blocks = recv_blocks.clone();
        let io_status_cloned = io_status.clone();
        let h = thread::Builder::new()
            .name(format!("worker_io{}", no_threads))
            .spawn(move || per_file_thread(cfg.recycle_io_blocks,&clone_recv_blocks, &clone_recv_pathbuff, &clone_send_fileslice, block_size, cfg.verbose, io_status_cloned))
            .unwrap();
        io_handler.push(h);
    }

    //
    // TICKER - setup statistics ticker thread in case we get bored waiting on things
    //
    let stopper = Arc::new(AtomicBool::new(false));
    let thread_stopper = stopper.clone();
    let do_ticker = atty::is(Stream::Stderr) && /* !do_stdin && */ !(cfg.verbose >= 3);
    let _ticker = {
        if do_ticker {
            let mut io_status_cloned = io_status.clone();
            let clone_send = send_fileslice.clone();

            Some(thread::spawn(move || stat_ticker(thread_stopper, &mut io_status_cloned, &clone_send)))
        } else {
            None
        }
    };

    //
    // FILE - setup feed by the source of data
    //
    if do_stdin {
        eprintln!("{}", "<<< reading from stdin".red());
        let stdin = std::io::stdin();
        let mut handle = stdin; // .lock();

        let (blocks, bytes) = io_thread_slicer(&recv_blocks, &"STDIO".to_string(), block_size, cfg.recycle_io_blocks, cfg.verbose, &mut handle, &mut io_status, &send_fileslice)?;
        total_bytes += bytes;
        total_blocks += blocks;
    } else if cfg.files.len() > 0 {
        let filelist = &cfg.files;
        for path in filelist {
            send_pathbuff
                .send(Some(path.clone()))
                .expect(&format!("Unable to send path: {} to path queue", path.display()));
        }
    } else {
        let options = MatchOptions {
            case_sensitive: true,
            require_literal_separator: false,
            require_literal_leading_dot: false,
        };
        // io and worker threads are started so this should kick off stuff
        // at first file... but not sure glob works that way - it may collect
        // internally first - not sure
        for entry in glob_with(
            &(cfg.glob.as_ref().expect(&format!("glob unparseable or non existent: {}", cfg.glob.as_ref().unwrap()))),
            options,
        )
        .unwrap()
        {
            if let Ok(path) = entry {
                send_pathbuff
                    .send(Some(path.clone()))
                    .expect(&format!("Unable to send path: {} to path queue", path.display()));
            }
        }
    }
    if cfg.verbose > 1 {
        eprintln!("sending stops to thread");
    }

    for _i in 0..cfg.no_threads {
        send_pathbuff.send(None)?;
    }
    if cfg.verbose > 1 {
        eprintln!("joining io handles");
    }
    for h in io_handler {
        let (blocks, bytes) = h.join().unwrap();
        total_bytes += bytes;
        total_blocks += blocks;
    }

    // proc workers on slices can ONLY be told to stop AFTER the IO threads are done
    for _i in 0..cfg.no_threads {
        send_fileslice.send(None)?;
    }
    // merge the data from the workers
    let mut all_the_maps = vec![];
    for h in worker_handlers {
        let (map, linecount, fieldcount) = h.join().unwrap();
        total_rowcount += linecount;
        total_fieldcount += fieldcount;
        all_the_maps.push(map);
    }
    if !cfg.no_output {
        sum_maps(&mut main_map, all_the_maps, cfg.verbose);
    }

    stopper.swap(true, Ordering::Relaxed);

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
    if do_ticker {
        eprintln!();
    } // write extra line at the end of stderr in case the ticker munges things

    let startout = Instant::now();

    if !cfg.no_output {
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
                ff.split('|').for_each(|x| {
                    if x.len() <= 0 {
                        vcell.push(Cell::new(&cfg.empty));
                    } else {
                        vcell.push(Cell::new(&x));
                    }
                });

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
                        vcell.push(Cell::new(&format!("{}", (x.0 / (x.1 as f64)))));
                    }
                }
                for i in 0usize..cc.distinct.len() {
                    if cfg.write_distros.contains(&cfg.unique_fields[i]) {
                        vcell.push(Cell::new(&format!("{}", distro_format(&cc.distinct[i], cfg.write_distros_upper, cfg.write_distros_bottom))));
                    } else {
                        vcell.push(Cell::new(&format!("{}", &cc.distinct[i].len())));
                    }
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
            for ff in thekeys.iter() {
                let mut vcell = vec![];
                ff.split('|').for_each(|x| {
                    if x.len() <= 0 {
                        vcell.push(format!("{}", cfg.empty));
                    } else {
                        vcell.push(x.to_string());
                    }
                });
                let cc = main_map.get(ff).unwrap();
                if !cfg.no_record_count {
                    vcell.push(format!("{}", cc.count));
                }
                for x in &cc.sums {
                    vcell.push(format!("{}", x));
                }
                for x in &cc.avgs {
                    vcell.push(format!("{}", x.0 / (x.1 as f64)));
                }
                for i in 0usize..cc.distinct.len() {
                    if cfg.write_distros.contains(&cfg.unique_fields[i]) {
                        vcell.push(format!("{}", distro_format(&cc.distinct[i], cfg.write_distros_upper, cfg.write_distros_bottom)));
                    } else {
                        vcell.push(format!("{}", &cc.distinct[i].len()));
                    }
                }
                println!("{}", vcell.join(&cfg.od));
            }
        }
    }
    let endout = Instant::now();
    let outdur = endout - startout;
    if cfg.verbose > 0 {
        eprintln!("output composition/write time: {:.3}", outdur.as_millis() as f64 / 1000.0);
    }
    if cfg.verbose >= 1 || cfg.stats {
        let elapsed = start_f.elapsed();
        let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000_000.0);
        let rate: f64 = (total_bytes as f64) as f64 / sec;
        let elapsedcpu: Duration = startcpu.elapsed();
        let seccpu: f64 = (elapsedcpu.as_secs() as f64) + (elapsedcpu.subsec_nanos() as f64 / 1000_000_000.0);
        eprintln!(
            "read: {}  blocks: {}  rows: {}  fields: {}  rate: {}/s  time: {:.3}  cpu: {:.3}  mem: {}",
            mem_metric_digit(total_bytes, 5),
            total_blocks,
            total_rowcount,
            total_fieldcount,
            mem_metric_digit(rate as usize, 5),
            sec,
            seccpu,
            mem_metric_digit(GLOBAL_TRACKER.get_alloc(), 4)
        );
    }
    Ok(())
}

fn worker_re(send_blocks: &crossbeam_channel::Sender<Vec<u8>>, cfg: &CliCfg, recv: &crossbeam_channel::Receiver<Option<FileSlice>>) -> (MyMap, usize, usize) {
    // return lines / fields
    match _worker_re(&send_blocks, cfg, recv) {
        Ok((map, lines, fields)) => return (map, lines, fields),
        Err(e) => {
            let err_msg = format!("Unable to process inner file - likely compressed or not UTF8 text: {}", e);
            panic!(err_msg);
        }
    }
}

#[derive(Debug)]
struct CapWrap<'t> {
    pub cl: &'t CaptureLocations_pcre2,
    pub text: &'t str,
}
//T: std::ops::Index<usize> + std::fmt::Debug,
//<T as std::ops::Index<usize>>::Output: AsRef<str>,

impl<'t> Index<usize> for CapWrap<'t> {
    type Output = str;

    fn index(&self, i: usize) -> &str {
        self.cl.get(i).map(|m| &self.text[m.0..m.1]).unwrap_or_else(|| panic!("no group at index '{}'", i))
    }
}

impl<'t> CapWrap<'t> {
    fn len(&self) -> usize {
        self.cl.len()
    }
}

fn _worker_re(
    send_blocks: &crossbeam_channel::Sender<Vec<u8>>,
    cfg: &CliCfg,
    recv: &crossbeam_channel::Receiver<Option<FileSlice>>,
) -> Result<(MyMap, usize, usize), Box<dyn std::error::Error>> {
    // return lines / fields

    let mut map = MyMap::new();

    let re_str = &cfg.re_str[0];
    if cfg.verbose > 2 {
        eprintln!("Start of {}", thread::current().name().unwrap());
    }
    let re = match Regex_pre2::new(re_str) {
        Err(err) => panic!("Cannot parse regular expression {}, error = {}", re_str, err),
        Ok(r) => r,
    };
    let mut buff = String::with_capacity(256); // dyn buffer
    let mut fieldcount = 0;
    let mut rowcount = 0;
    let mut _skipped = 0;
    let mut cl = re.capture_locations();

    loop {
        let fc = match recv.recv().expect("thread failed to get next job from channel") {
            Some(fc) => fc,
            None => {
                if cfg.verbose > 1 {
                    eprintln!("{} exit on None", thread::current().name().unwrap())
                }
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

                if let Some(_record) = re.captures_read(&mut cl, line.as_bytes())? {
                    let cw = CapWrap { cl: &cl, text: line.as_str() };
                    fieldcount += store_rec(&mut buff, &line, &cw, cw.len(), &mut map, &cfg, &mut rowcount);
                } else {
                    _skipped += 1;
                }
            }
        }
        if cfg.recycle_io_blocks {
            send_blocks.send(fc.block)?;
        }
    }
    Ok((map, rowcount, fieldcount))
}

fn worker_csv(send_blocks: &crossbeam_channel::Sender<Vec<u8>>, cfg: &CliCfg, recv: &crossbeam_channel::Receiver<Option<FileSlice>>) -> (MyMap, usize, usize) {
    match _worker_csv(&send_blocks, cfg, recv) {
        Ok((map, lines, fields)) => return (map, lines, fields),
        Err(e) => {
            let err_msg = format!("Unable to process inner block - likely compressed or not UTF8 text: {}", e);
            panic!(err_msg);
        }
    }
}

fn _worker_csv(
    send_blocks: &crossbeam_channel::Sender<Vec<u8>>,
    cfg: &CliCfg,
    recv: &crossbeam_channel::Receiver<Option<FileSlice>>,
) -> Result<(MyMap, usize, usize), Box<dyn std::error::Error>> {
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
                if cfg.verbose > 1 {
                    eprintln!("{} exit on None", thread::current().name().unwrap())
                }
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
        if cfg.recycle_io_blocks {
            send_blocks.send(fc.block)?;
        }
    }
    Ok((map, rowcount, fieldcount))
}

fn worker_multi_re(send_blocks: &crossbeam_channel::Sender<Vec<u8>>, cfg: &CliCfg, recv: &crossbeam_channel::Receiver<Option<FileSlice>>) -> (MyMap, usize, usize) {
    // return lines / fields
    match _worker_multi_re(&send_blocks, cfg, recv) {
        Ok((map, lines, fields)) => return (map, lines, fields),
        Err(e) => {
            let err_msg = format!("Unable to process inner file - likely compressed or not UTF8 text: {}", e);
            panic!(err_msg);
        }
    }
}

fn grow_str_vec_or_add(idx: usize, v: &mut Vec<String>, s: &str, line: &str, linecount: usize, i:usize, verbose: usize) {
    if idx < v.len() {
        if verbose > 1 && v[idx].len() > 0 {
            eprintln!("idx: {} pushing more: {} to existing: {}  line: {} : {}  match# {}", idx, s, v[idx], line, linecount, i);
        }
        v[idx].push_str(s);
    } else {
        v.push(String::from(s));
    }
}

fn _worker_multi_re(
    send_blocks: &crossbeam_channel::Sender<Vec<u8>>,
    cfg: &CliCfg,
    recv: &crossbeam_channel::Receiver<Option<FileSlice>>,
) -> Result<(MyMap, usize, usize), Box<dyn std::error::Error>> {
    let mut map = MyMap::new();

    let mut re_es = vec![];
    //let mut cls = vec![];
    for r in &cfg.re_str {
        let re = match Regex_pre2::new(r) {
            Err(err) => panic!("Cannot parse regular expression {}, error = {}", r, err),
            Ok(r) => r,
        };
     
        re_es.push(re);
        //cls.push(re.capture_locations());
    }

    let mut buff = String::with_capacity(256); // dyn buffer
    let mut fieldcount = 0;
    let mut rowcount = 0;
    let mut linecount = 0;
    let mut _skipped = 0;
    let mut re_curr_idx = 0;
    let mut acc_record: Vec<String> = vec![];

    acc_record.push(String::new()); // push 1st blank whole match
    let mut cl = re_es[0].capture_locations();

    let mut acc_idx = 1usize;
    loop {

        let fc = match recv.recv().expect("thread failed to get next job from channel") {
            Some(fc) => fc,
            None => {
                if cfg.verbose > 1 {
                    eprintln!("{} exit on None", thread::current().name().unwrap())
                }
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
        if cfg.verbose > 1 { eprintln!("file slice index: filename: {} index: {} len: {}", fc.filename, fc.index, fc.len); }

        if !cfg.noop_proc {
            for line in fc.block[0..fc.len].lines() {
                let line = line?;
                linecount += 1;
                let re = &re_es[re_curr_idx];
                if let Some(record) = re.captures_read(&mut cl, line.as_bytes())? {
                    let cw = CapWrap { cl: &cl, text: line.as_str() };
                    for i in 1 .. cw.len() {
                        let f = &cw[i];
                        acc_idx += 1;
                        grow_str_vec_or_add(acc_idx, &mut acc_record, f, &line, linecount, i, cfg.verbose);
                        if cfg.verbose > 2 {
                            eprintln!("mRE MATCHED: {}  REC: {:?}", line, acc_record);
                        }
                    }
                    re_curr_idx += 1;
                    if re_curr_idx >= re_es.len() {
                        fieldcount += store_rec(&mut buff, &line, &acc_record, acc_record.len(), &mut map, &cfg, &mut rowcount);
                        if cfg.verbose > 2 {
                            eprintln!("mRE STORE {:?} on line: {}", acc_record, line);
                        }
                        re_curr_idx = 0;
                        acc_record.iter_mut().for_each(move |s| s.clear());
                        for s in &mut acc_record {
                            s.clear();
                        }
                        acc_idx = 0;
                    }
                } else {
                    _skipped += 1;
                }
            }
        }
        if cfg.recycle_io_blocks {
            send_blocks.send(fc.block)?;
        }
    }
    Ok((map, rowcount, fieldcount))
}

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
        for i in 0..cfg.key_fields.len() {
            let index = cfg.key_fields[i];
            if index < rec_len {
                ss.push_str(&record[index].as_ref());
            } else {
                ss.push_str("NULL");
            }
            ss.push('|');
        }
        ss.pop(); // remove the trailing | instead of check each iteration
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
        for i in 0..cfg.sum_fields.len() {
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
        for i in 0..cfg.avg_fields.len() {
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
                    }
                }
            }
        }
    }
    if cfg.unique_fields.len() > 0 {
        for i in 0..cfg.unique_fields.len() {
            let index = cfg.unique_fields[i];
            if index < rec_len {
                if !brec.distinct[i].contains_key(record[index].as_ref()) {
                    brec.distinct[i].insert(record[index].as_ref().to_string(), 1);
                } else {
                    let x = brec.distinct[i].get_mut(record[index].as_ref()).unwrap();
                    *x = *x + 1;
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
                    if !v_new.distinct[j].contains_key(u.0) {
                        v_new.distinct[j].insert(u.0.clone(), *u.1);
                    } else {
                        let x = v_new.distinct[j].get_mut(u.0).unwrap();
                        *x = *x + *u.1;
                    }
                }
            }
        }
    }
    let end = Instant::now();
    let dur = end - start;
    if verbose > 0 {
        println!("re thread merge maps time: {:.3}s", dur.as_millis() as f64 / 1000.0f64);
    }
}
