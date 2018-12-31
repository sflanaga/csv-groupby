#![allow(unused)]
extern crate csv;
extern crate prettytable;
extern crate regex;
extern crate chan;
extern crate crossbeam_channel; // 0.2.5;
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

fn sum_maps(p_map: &mut MyMap, maps: Vec<MyMap>, verbose: u32) {
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
    # All the follow field lists are zero based - first field is 0
    -f x,y...z - comma seperated field list of fields to use as a group by
    -s x,y...z - comma seperated field list of files to do a f64 sum of
    -u x,y...z - comma seperated field list of unique or distinct records
    -a turn off table format - use csv format
    -d input_delimiter - single char
    -v - verbose
    -vv - more verbose
    --nc - do not write record counts
    -r <RE> parse lines using regular expression and use sub groups as fields
    -j <num> number of RE threads to spawn - default 4
    -q <num> number queue-entries between line reader and re parser thread - default 500
    -p <num> number of lines per queue entry to reduce queue touches - default 100
    --noop_proc  do nothing but read lines or parse csv in records - no datastructure updates
        use this to measure the flow rate of data input from file system???
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

fn csv() -> Result<(), std::io::Error> {
    let mut key_fields = vec![];
    let mut unique_fields = vec![];
    let mut sum_fields = vec![];
    let mut delimiter: char = ',';
    let mut od = ",".to_string();
    let mut auto_align: bool = true;
    let mut verbose = 0;
    let mut hasheader = false;
    let mut write_record_count = true;
    let mut re_str = String::new();
    let mut read_stdin = false;
    let mut empty = "EMPTY".to_string();
    let mut re_threadno = 4;
    let mut re_thread_qsize = 500;
    let mut re_pace: usize = 100;
    let mut noop_proc = false;

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
                read_stdin = true;
            }
            "-a" => {
                auto_align = false;
            }
            "-f" => { // field list processing
                i += 1;
                key_fields.splice(.., (&argv[i][..]).split(",").map(|x| x.parse::<usize>().unwrap()));
            }
            "-s" => { // field list processing
                i += 1;
                sum_fields.splice(.., (&argv[i][..]).split(",").map(|x| x.parse::<usize>().unwrap()));
            }
            "-u" => { // unique count AsMut
                i += 1;
                unique_fields.splice(.., (&argv[i][..]).split(",").map(|x| x.parse::<usize>().unwrap()));
            }
            "--od" => { // unique count AsMut
                i += 1;
                od.clear();
                od.push_str(&argv[i]);
            }
            "-r" => { // unique count AsMut
                i += 1;
                re_str.push_str(&argv[i]);
            }
            "-d" => { // unique count AsMut
                i += 1;
                delimiter = argv[i].as_bytes()[0] as char;
            }
            "-v" => { // write out AsMut
                verbose = 1;
                eprintln!("writing stats and other info ON")
            }
            "-vv" => { // write out AsMut
                verbose = 2;
                eprintln!("writing stats and other debug info ON")
            }
            "-j" => { // thread count 
                i += 1;
                re_threadno = argv[i].parse::<usize>().unwrap();
            }
            "-p" => { //
                i += 1;
                re_pace = argv[i].parse::<usize>().unwrap();
            }
            "-q" => { // qsize count
                i += 1;
                re_thread_qsize = argv[i].parse::<usize>().unwrap();
            }
            "-h" => { // write out AsMut
                hasheader = true;
            }
            "--nc" => { // just write the keys and not the row count
                write_record_count = false;
            }
            "--noop_proc" => { // do nothing real in the re thread
                noop_proc = true;
            }
            x => {
                if verbose >= 1 { eprintln!("adding filename {} to scan", x); }
                filelist.push(x);
            }
        }

        i += 1;
    }
    // if key_fields.len() <= 0 {
    //     help("missing key fields - you must specify -f option with something or no summaries can be made");
    // }
    let mut regex = match Regex::new(&re_str) {
        Err(err) => panic!("Cannot parse regular expression {}, error = {}", re_str, err),
        Ok(r) => r,
    };

    let maxfield = 1;

    if verbose >= 1 {
        eprintln!("\tdelimiter: {}", delimiter);
        eprintln!("\theader: {}", hasheader);
        eprintln!("\tkey_fields: {:?}  len={}", key_fields, key_fields.len());
        eprintln!("\tsum_fields: {:?}  len={}", sum_fields, sum_fields.len());
        eprintln!("\tunique_fields: {:?}", unique_fields);
        eprintln!("\tfile list {:?}", filelist);
        eprintln!("\tre {}", re_str);
        eprintln!("\tre thread queue size {}", re_thread_qsize);
        eprintln!("\tre no of threads {}", re_threadno);
        if noop_proc {
            eprintln!("noop_proc - just read lines or parse csv");
        }
        if filelist.len() <= 0 {
            eprintln!("\tprocessing stdin");
        }
    }

    //let mut hm_arc : Arc<RwLock<MyMap<String, KeySum>>> = Arc::new(RwLock::new(MyMap::default()));
    //let mut hm_arc : Arc<MyMap<String, KeySum>> = Arc::new(MyMap::default());

    let mut total_rowcount = 0usize;
    let mut total_fieldcount = 0usize;
    let mut total_bytes = 0u64;
    let start_f = Instant::now();

    if filelist.len() <= 0 && !read_stdin {
        help("either use stdin via -i option or put files on command line");
    }
    let mut main_map = MyMap::new();

    // SETUP
    //
    // STDIO reading
    //
    if filelist.len() <= 0 && read_stdin {
        let stdin = std::io::stdin();
        let mut handle = stdin.lock();
        let (rowcount, fieldcount, bytecount) = if re_str.len() > 0 {
            process_re(&regex, &mut handle, delimiter, &mut main_map, &key_fields, &sum_fields, &unique_fields, hasheader, verbose, re_threadno, re_thread_qsize, noop_proc, re_pace)
        } else { //(0,0,0) }; //else {
            process_csv(&mut handle, delimiter, &mut main_map, &key_fields, &sum_fields, &unique_fields, hasheader, verbose, noop_proc)
        };

        total_rowcount += rowcount;
        total_fieldcount += fieldcount;
        total_bytes += bytecount;
    } else {
        //
        // FILELIST reading
        //
        for filename in filelist.into_iter() {
            // let metadata = fs::metadata(&filename)?;
            let metadata = match fs::metadata(&filename) {
                Ok(m) => m,
                Err(err) => {
                    eprintln!("skipping file \"{}\", could not get stats on it, cause: {}", &filename, err);
                    continue;
                }
            };

            if verbose >= 1 { eprintln!("file: {}", filename); }
            let f = match OpenOptions::new()
                .read(true)
                .write(false)
                .create(false)
                .open(&filename)
                {
                    Ok(f) => f,
                    Err(e) => panic!("cannot open file \"{}\" due to this error: {}", filename, e),
                };
            let mut handle = BufReader::with_capacity(1024 * 1024 * 4, f);
            let (rowcount, fieldcount, bytecount) = if re_str.len() > 0 {
                process_re(&regex, &mut handle, delimiter, &mut main_map, &key_fields, &sum_fields, &unique_fields, hasheader, verbose, re_threadno, re_thread_qsize, noop_proc, re_pace)
            } else { // (0,0,0) };
                process_csv(&mut handle, delimiter, &mut main_map, &key_fields, &sum_fields, &unique_fields, hasheader, verbose, noop_proc)
            };

            total_rowcount += rowcount;
            total_fieldcount += fieldcount;
            total_bytes += metadata.len() as u64;
        }
    };

    // OUTPUT
    // write the data structure
    //
    if auto_align {
        let celltable = std::cell::RefCell::new(Table::new());
        celltable.borrow_mut().set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
        {
            let mut vcell = vec![];
            if key_fields.len() > 0 {
                for x in key_fields {
                    vcell.push(Cell::new(&format!("k:{}", &x)));
                }
            } else {
                vcell.push(Cell::new("k:-"));
            }
            if write_record_count { vcell.push(Cell::new("count")); }
            for x in sum_fields {
                vcell.push(Cell::new(&format!("s:{}", &x)));
            }
            for x in unique_fields {
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
                    vcell.push(Cell::new(&empty));
                } else {
                    vcell.push(Cell::new(&x.to_string()));
                }
            }

            if write_record_count {
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
            if key_fields.len() > 0 {
                for x in key_fields {
                    vcell.push(format!("k:{}", &x));
                }
            } else {
                vcell.push("k:-".to_string());
            }
            if write_record_count { vcell.push("count".to_string()); }
            for x in sum_fields {
                vcell.push(format!("s:{}", x));
            }
            for x in unique_fields {
                vcell.push(format!("u:{}", x));
            }
            println!("{}", vcell.join(&od));
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
                    vcell.push(format!("{}", empty));
                } else {
                    vcell.push(format!("{}", x));
                }
            }
            let cc = main_map.get(ff).unwrap();
            if write_record_count {
                vcell.push(format!("{}", cc.count));
            }
            for x in &cc.sums {
                vcell.push(format!("{}", x));
            }
            for x in &cc.distinct {
                vcell.push(format!("{}", x.len()));
            }
            println!("{}", vcell.join(&od));
        };
    }

    if verbose >= 1 {
        let elapsed = start_f.elapsed();
        let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000_000.0);
        let rate: f64 = (total_bytes as f64 / (1024f64 * 1024f64)) as f64 / sec;
        eprintln!("rows: {}  fields: {}  rate: {:.2}MB/s", total_rowcount, total_fieldcount, rate);
    }
    Ok(())
}

#[inline]
fn store_rec<T>(ss: &mut String, line: &str, record: &T, rec_len: usize, map: &mut MyMap, key_fields: &Vec<usize>, sum_fields: &Vec<usize>, unique_fields: &Vec<usize>, rowcount: &mut usize, verbose: u32) -> usize
    where T: std::ops::Index<usize> + std::fmt::Debug,
          <T as std::ops::Index<usize>>::Output: ToString
{
    //let mut ss: String = String::with_capacity(256);
    ss.clear();

    let mut fieldcount = 0usize;

    let mut sum_grab = vec![];
    let mut uni_grab = vec![];

    let mut local_count = 0;

    if verbose >= 3 {
        if line.len() > 0  {
            eprintln!("DBG:  {:?}  from: {}", &record, line);
        } else {
            eprintln!("DBG:  {:?}", &record);
        }
    }
    let mut i = 0;
    if key_fields.len() > 0 {
        fieldcount += rec_len;
        while i < key_fields.len() {
            let index = key_fields[i];
            if index + 1 < rec_len {
                ss.push_str(&record[index + 1].to_string());
            } else {
                ss.push_str("NULL");
            }
            if i != key_fields.len() - 1 {
                ss.push('|');
            }
            i += 1;
        }
    } else {
        ss.push_str("NULL");
        //println!("no match: {}", line);
    }

    // println!("{:?}", ss);

    if sum_fields.len() > 0 {
        sum_grab.truncate(0);
        i = 0;
        while i < sum_fields.len() {
            let index = sum_fields[i];
            if index + 1 < rec_len {
                let v = &record[index + 1];
                match v.to_string().parse::<f64>() {
                    Err(_) => {
                        if verbose >= 1 {
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

    if unique_fields.len() > 0 {
        uni_grab.truncate(0);
        i = 0;
        while i < unique_fields.len() {
            let index = unique_fields[i];
            if index + 1 < rec_len {
                uni_grab.push(record[index + 1].to_string());
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

fn process_re(re: &Regex, rdr: &mut BufRead, delimiter: char, map: &mut MyMap, key_fields: &Vec<usize>, sum_fields: &Vec<usize>,
              unique_fields: &Vec<usize>, header: bool, verbose: u32, re_threadno: usize, re_thread_qsize: usize, noop_proc: bool, re_pace: usize) -> (usize, usize, u64) {
    let mut skipped = 0u64;
    let mut bytecount = 0u64;
    let mut rowcount = 0usize;
    //
    //  setup worker threads for RE mode
    //
    let mut threadhandles = vec![];
    let (send, recv): (channel::Sender<Option<Vec<String>>>, channel::Receiver<Option<Vec<String>>>) = channel::bounded(re_thread_qsize);
    for thrno in 0..re_threadno {
        let clone_recv = recv.clone();
        //let clone_arc = hm_arc.clone();
        let hm = MyMap::new();
        let cloned_re = re.clone();

        let key_fields = key_fields.clone();
        let sum_fields = sum_fields.clone();
        let unique_fields = unique_fields.clone();
        let re = re.clone();

        let h = thread::spawn(move || {
            let mut map = MyMap::new();

            let mut ss: String = String::with_capacity(256);

            let mut fieldcount = 0usize;

//            let mut sum_grab = vec![];
//            let mut uni_grab = vec![];

            let mut local_count = 0;
            loop {
                let mut lines;
                match clone_recv.recv().unwrap() {
                    Some(l) => { lines = l; }
                    None => { break; }
                }
                for line in lines {
                    if !noop_proc {
                        if let Some(record) = re.captures(line.as_str()) {
                            local_count += 1;

                            fieldcount += store_rec(&mut ss, &line, &record, record.len(), &mut map, &key_fields, &sum_fields, &unique_fields, &mut rowcount, verbose);

                        } else {
                            skipped += 1;
                        }
                    }  // noop_re
                }
            }
            if verbose > 0 { eprintln!("threadid: {} parsed: {} lines and skipped: {}", thrno, local_count, skipped); }
            return map;
        });
        threadhandles.push(h);
    }
    let mut allthemaps = Vec::new();
    //for result in recrdr.records() {
    let mut pacevec: Vec<String> = Vec::with_capacity(re_pace);
    for line in rdr.lines() {
        let line = &line.unwrap();
        rowcount += 1;
        bytecount += line.len() as u64 + 1;

        pacevec.push(line.clone());
        if pacevec.len() >= re_pace {
            send.send(Some(pacevec));
            pacevec = Vec::with_capacity(re_pace);
        }
    }
    if pacevec.len() > 0 {
        send.send(Some(pacevec));
    }

    for i in 0..re_threadno { send.send(None); }

    for h in threadhandles {
        allthemaps.push(h.join().unwrap());
    }

    sum_maps(map, allthemaps, verbose);

    (rowcount, 0 /*fieldcount*/, bytecount)
}


fn __process_csv(rdr: &mut BufRead, delimiter: char, map: &mut MyMap, key_fields: &Vec<usize>, sum_fields: &Vec<usize>, unique_fields: &Vec<usize>, header: bool, verbose: u32, noop_proc: bool) -> (usize, usize, u64) {
    let mut ss: String = String::with_capacity(256);

    let mut recrdr = csv::ReaderBuilder::new()
        .delimiter(delimiter as u8).has_headers(header).flexible(true)
        .from_reader(rdr);
    //println!("{:?}", &recrdr);
    let mut rowcount = 0usize;
    let mut fieldcount = 0usize;

//    let mut sum_grab = vec![];
//    let mut uni_grab = vec![];

    let mut bytecount = 0u64;
    for result in recrdr.records() {
        let record: csv::StringRecord = result.unwrap();

        let pos = record.position().expect("a record position");
        bytecount = pos.byte();

        if verbose >= 2 {
            eprintln!("DBG: rec: {:?}", record);
        }
        fieldcount += store_rec( &mut ss, "", &record, record.len(), map, &key_fields, &sum_fields, &unique_fields, &mut rowcount, verbose);


        if !noop_proc {

        }
    } // for record loop

    (rowcount, fieldcount, bytecount)
}

fn process_csv(rdr: &mut BufRead, delimiter: char, map: &mut MyMap, key_fields: &Vec<usize>, sum_fields: &Vec<usize>, unique_fields: &Vec<usize>, header: bool, verbose: u32, noop_proc: bool) -> (usize, usize, u64) {
    let mut ss: String = String::with_capacity(256);

    let mut recrdr = rdr;
    //println!("{:?}", &recrdr);
    let mut rowcount = 0usize;
    let mut fieldcount = 0usize;
//    let mut sum_grab = vec![];
//    let mut uni_grab = vec![];

    let mut bytecount = 0u64;

    let buff = String::with_capacity(256);

    for result in recrdr.lines() {
        let line: String = result.unwrap();
        bytecount += line.len() as u64;

     //   if !noop_proc {
            let record: Vec<_> = line.split(delimiter).collect();
            if verbose >= 2 {
                eprintln!("DBG: rec: {:?}", record);
            }
            // let pos = record.position().expect("a record position");
            bytecount += line.len() as u64;

            fieldcount += store_rec(&mut ss, &line, &record, record.len(), map, &key_fields, &sum_fields, &unique_fields, &mut rowcount, verbose);

       // }
    } // for record loop

    (rowcount, fieldcount, bytecount)
}
