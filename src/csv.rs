#![allow(unused)]
extern crate csv;
extern crate prettytable;
extern crate regex;
extern crate chan;

use chan::{Sender,Receiver};


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
use std::sync::{Arc,RwLock,Mutex};

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


use regex::Regex;

mod gen;

use gen::greek;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

#[derive(Debug)]
struct KeySum {
    count : u64,
    sums : Vec<f64>,
    distinct: Vec<HashSet<String>>
}
fn main() {
    // built::write_built_file().expect("Failed to acquire build-time information");
    if let Err(err) = csv() {
        match err.kind() {
            ErrorKind::BrokenPipe => println!("broken pipe"),
            _ => {
                eprintln!("error: of another kind {:?}", &err);
                std::process::exit(1);
            },
        }
    }
/*
    match csv() {
		ErrorKind::BrokenPipe(err) => {},
		Err(err) => {
	},
	}
*/
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

fn csv() -> Result<(),std::io::Error> {

    let mut key_fields = vec![];
    let mut unique_fields = vec![];
    let mut sum_fields = vec![];
    let mut delimiter : char = ',';
    let mut od = ",".to_string();
    let mut auto_align: bool = true;
    let mut verbose = 0;
    let mut hasheader = false;
    let mut write_record_count = true;
    let mut re_str = String::new();
    let mut read_stdin = false;
    let mut EMPTY = "EMPTY".to_string();

    let argv : Vec<String> = args().skip(1).map( |x| x).collect();
    let filelist = &mut vec![];

    let mut i = 0;

    if argv.len() <= 1 {
        help("no command line options used");
    }

    while i < argv.len() {
        match &argv[i][..] {
            "--help" => { // field list processing
                help("command line requested help info");
            },
            "-i" => {
                read_stdin = true;
            },
            "-a" => {
                auto_align = false;
            },
            "-f" => { // field list processing
                i += 1;
                key_fields.splice(.., (&argv[i][..]).split(",").map( |x| x.parse::<usize>().unwrap()) );
            },
            "-s" => { // field list processing
                i += 1;
                sum_fields.splice(.., (&argv[i][..]).split(",").map( |x| x.parse::<usize>().unwrap()) );
            },
            "-u" => { // unique count AsMut
                i += 1;
                unique_fields.splice(.., (&argv[i][..]).split(",").map( |x| x.parse::<usize>().unwrap()) );
            },
            "--od" => { // unique count AsMut
                i += 1;
                od.clear();
                od.push_str(&argv[i]);
            },
            "-r" => { // unique count AsMut
                i += 1;
                re_str.push_str(&argv[i]);
            },
            "-d" => { // unique count AsMut
                i += 1;
                delimiter = argv[i].as_bytes()[0] as char;
            },
            "-v" => { // write out AsMut
                verbose = 1;
                eprintln!("writing stats and other info ON")
            },
             "-vv" => { // write out AsMut
                verbose = 2;
                eprintln!("writing stats and other debug info ON")
            },
            "-h" => { // write out AsMut
                hasheader = true;
            },
            "--nc" => { // just write the keys and not the row count
                write_record_count = false;
            },
            x => {
                if verbose>=1 { eprintln!("adding filename {} to scan", x); }
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

    if verbose>=1 {
        eprintln!("\tdelimiter: {}", delimiter);
        eprintln!("\theader: {}", hasheader);
        eprintln!("\tkey_fields: {:?}  len={}", key_fields, key_fields.len() );
        eprintln!("\tsum_fields: {:?}  len={}", sum_fields, sum_fields.len() );
        eprintln!("\tunique_fields: {:?}", unique_fields);
        eprintln!("\tfile list {:?}", filelist);
        if filelist.len() <= 0 {
            eprintln!("\tprocessing stdin");
        }
    }

    let mut hm_arc : Arc<RwLock<BTreeMap<String, KeySum>>> = Arc::new(RwLock::new(BTreeMap::new()));

    let mut total_rowcount = 0usize;
    let mut total_fieldcount = 0usize;
    let mut total_bytes = 0u64;
    let start_f = Instant::now();

    if filelist.len() <= 0 && !read_stdin {
        help("either use stdin via -i option or put files on command line");
    }  

    if filelist.len() <= 0 && read_stdin {
        let stdin = std::io::stdin();
        let mut handle = stdin.lock();
        let (rowcount, fieldcount, bytecount) = if re_str.len() > 0 {
            process_re(& regex, &mut handle, &mut hm_arc, delimiter, & key_fields, & sum_fields, & unique_fields, hasheader, verbose)
        } else {
            process_csv(&mut handle, &mut hm_arc, delimiter, & key_fields, & sum_fields, & unique_fields, hasheader, verbose)
        };

        total_rowcount += rowcount;
        total_fieldcount += fieldcount;
        total_bytes += bytecount;
    } else {
        for filename in filelist.into_iter() {
           // let metadata = fs::metadata(&filename)?;
            let metadata = match fs::metadata(&filename) {
                Ok(m) => m,
                Err(err) => {
                    eprintln!("skipping file \"{}\", could not get stats on it, cause: {}", &filename, err);
                    continue;
                },
            };

            if verbose>=1 { eprintln!("file: {}", filename); }
            let f = match OpenOptions::new()
                    .read(true)
                    .write(false)
                    .create(false)
                    .open(&filename)
                    {
                        Ok(f) => f,
                        Err(e) => panic!("cannot open file \"{}\" due to this error: {}",filename, e),
                    };
            let mut handle = BufReader::with_capacity(1024*1024*4,f);
            let (rowcount, fieldcount, bytecount) = if re_str.len() > 0 {
                process_re(& regex, &mut handle, &mut hm_arc, delimiter, & key_fields, & sum_fields, & unique_fields, hasheader, verbose)
            } else {
                process_csv(&mut handle, &mut hm_arc, delimiter, & key_fields, & sum_fields, & unique_fields, hasheader, verbose)
            };

            total_rowcount += rowcount;
            total_fieldcount += fieldcount;
            total_bytes += metadata.len() as u64;
        }
    }
    let cloned_hm = hm_arc.clone();
    let hm = cloned_hm.write().unwrap();
    if auto_align {
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
        {
            let mut vcell = vec![];
            if key_fields.len() > 0 {
                for x in key_fields {
                    vcell.push(Cell::new(&format!("k:{}",&x)));
                }
            } else {
                vcell.push(Cell::new("k:-"));
            }
            if write_record_count { vcell.push(Cell::new("count")); }
            for x in sum_fields {
                vcell.push(Cell::new(&format!("s:{}",&x)));
            }
            for x in unique_fields {
                vcell.push(Cell::new(&format!("u:{}",&x)));
            }
            let mut row = Row::new(vcell);
            table.set_titles(row);
        }

        for (ff,cc) in hm.iter() {
            let mut vcell = vec![];
            let z1: Vec<&str> = ff.split('|').collect();
            for x in &z1 {
                if x.len() <= 0 {
                    vcell.push(Cell::new(&EMPTY));
                } else {
                    vcell.push(Cell::new(&x.to_string()));
                }
            }
            // z1.iter().map( |x| { println!("{}", x); vcell.push(Cell::new(x));} );
            // //vcell.push(Cell::new(&ff));
            if write_record_count {
                vcell.push(Cell::new(&format!("{}",cc.count)));
            }
            for x in &cc.sums {
                vcell.push(Cell::new(&format!("{}",x)));
            }
            for x in &cc.distinct {
                vcell.push(Cell::new(&format!("{}",x.len())));
            }
            let mut row = Row::new(vcell);
            table.add_row(row);
        }
        table.printstd();
    } else {
        {
            let mut vcell = vec![];
            if key_fields.len() > 0 {
                for x in key_fields {
                    vcell.push(format!("k:{}",&x));
                }
            } else {
                vcell.push("k:-".to_string());
            }
            if write_record_count { vcell.push("count".to_string()); }
            for x in sum_fields {
                vcell.push(format!("s:{}",x));
            }
            for x in unique_fields {
                vcell.push(format!("u:{}",x));
            }
            println!("{}", vcell.join(&od));
            // let mut row = Row::new(vcell);
            // table.set_titles(row);
        }
        for (ff,cc) in hm.iter() {
            let mut vcell = vec![];
            let z1: Vec<String> = ff.split('|').map( |x| x.to_string() ).collect();
            for x in &z1 {
                if x.len() <= 0 {
                    vcell.push(format!("{}", EMPTY));
                } else {
                    vcell.push(format!("{}", x));
                }
            }
            if write_record_count {
                vcell.push(format!("{}",cc.count));
            }
            for x in &cc.sums {
                vcell.push(format!("{}",x));
            }
            for x in &cc.distinct {
                vcell.push(format!("{}",x.len()));
            }
            println!("{}", vcell.join(&od));
        }
    }


    if verbose>=1 {
        let elapsed = start_f.elapsed();
        let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000_000.0);
        let rate : f64= (total_bytes as f64 / (1024f64*1024f64)) as f64 / sec;
        eprintln!("rows: {}  fields: {}  rate: {:.2}MB/s", total_rowcount, total_fieldcount, rate);
    }
    Ok( () )
}


fn process_re( re: &Regex, rdr: &mut BufRead, hm_arc : &mut Arc<RwLock<BTreeMap<String, KeySum>>>,
    delimiter: char, key_fields : & Vec<usize>, sum_fields : & Vec<usize>, 
    unique_fields: & Vec<usize>, header: bool, verbose: u32) -> (usize,usize,u64) {

    let mut skipped = 0u64;
    let mut bytecount = 0u64;
    let mut rowcount = 0usize;

    //
    //  setup worker threads for RE mode
    //
    let no_threads = 4;
    let mut threadhandles = vec![];
    let (send, recv): (Sender<Option<String>>, Receiver<Option<String>>) = chan::sync(4000);
    for thrno in 0..no_threads { 
        let clone_recv = recv.clone();
        let clone_arc = hm_arc.clone();
        let cloned_re = re.clone();

        let key_fields = key_fields.clone();
        let sum_fields = sum_fields.clone();
        let unique_fields = unique_fields.clone();
        let re = re.clone();

        let h = thread::spawn( move || {

            let mut ss : String = String::with_capacity(256);

            let mut fieldcount = 0usize;
            
            let mut sum_grab = vec![];
            let mut uni_grab = vec![];
            
            let mut local_count = 0;
            loop {
                let mut line;
                match clone_recv.recv().unwrap() {
                    Some(l) => { line = l; }
                    None => { break; }
                }
                if let Some(record) = re.captures(line.as_str()) {
                    local_count += 1;
                    ss.clear();
                    if verbose >= 2 {
                        eprintln!("DBG:  {:?}  from: {}", &record, line);
                    }
                    let mut i = 0;
                    if key_fields.len() > 0 {
                        fieldcount += record.len();
                        while i < key_fields.len() {
                            let index = key_fields[i];
                            if index+1 < record.len() {
                                ss.push_str(&record[index+1]);
                            } else {
                                ss.push_str("NULL");
                            }
                            if i != key_fields.len()-1 {
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
                        i=0;
                        while i < sum_fields.len() {
                            let index = sum_fields[i];
                            if index+1 < record.len() {
                                let v = &record[index+1];
                                match v.parse::<f64>() {
                                    Err(_) => {
                                        if verbose>=1 {
                                            eprintln!("error parsing string |{}| as a float for summary index: {} so pretending value is 0",v, index);
                                        }
                                        sum_grab.push(0f64);
                                    },
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
                        i=0;
                        while i < unique_fields.len() {
                            let index = unique_fields[i];
                            if index+1 < record.len() {
                                uni_grab.push(record[index+1].to_string());
                            } else {
                                uni_grab.push("NULL".to_string());
                            }
                            i += 1;
                        }
                    }

                    if ss.len() > 0 {
                        rowcount += 1;
                        {
                            let mut hm = clone_arc.write().unwrap();
                            let v = hm.entry(ss.clone()).or_insert(KeySum{ count: 0, sums: Vec::new(), distinct: Vec::new() });
                            v.count = v.count +1;
                            // println!("sum on: {:?}", sum_grab);
                            if v.sums.len() <= 0 {
                                for f in &sum_grab {
                                    v.sums.push(*f);
                                }
                            } else {
                                for (i,f) in sum_grab.iter().enumerate() {
                                    v.sums[i] = v.sums[i] + f;
                                }
                            }

                            if uni_grab.len() > 0 {
                                while v.distinct.len() < uni_grab.len() {
                                    v.distinct.push(HashSet::new());
                                }
                                for (i,u) in uni_grab.iter().enumerate() {
                                    v.distinct[i].insert(u.to_string());
                                }
                            }
                        }
                    }
                } else {
                    skipped += 1;
                }
    
            }
            if verbose > 0 { println!("threadid: {} parsed: {} lines and skipped: {}",thrno, local_count, skipped); }
        });
        threadhandles.push(h);
    }
    
    //for result in recrdr.records() {
    for line in rdr.lines() {
        let line = &line.unwrap();
        rowcount += 1;
        bytecount += line.len() as u64 + 1;
        let passline = Some(line.clone());
        send.send(passline);

    }
    for i in 0..no_threads { send.send(None); }
    for h in threadhandles { h.join().unwrap(); }
    (rowcount, 0 /*fieldcount*/ , bytecount)
}

fn process_csv(rdr: &mut BufRead, hm_arc : &mut Arc<RwLock<BTreeMap<String, KeySum>>>,
    delimiter: char, key_fields : & Vec<usize>, sum_fields : & Vec<usize>, unique_fields: & Vec<usize>, header: bool, verbose: u32) -> (usize,usize,u64) {

    let mut ss : String = String::with_capacity(256);

    let mut recrdr = csv::ReaderBuilder::new()
        .delimiter(delimiter as u8).has_headers(header).flexible(true)
        .from_reader(rdr);
    //println!("{:?}", &recrdr);
    let mut rowcount = 0usize;
    let mut fieldcount = 0usize;
    let mut sum_grab = vec![];
    let mut uni_grab = vec![];

    let mut bytecount = 0u64;
    let clone_hm = hm_arc.clone();
    let mut hm = clone_hm.write().unwrap();
    for result in recrdr.records() {
        //println!("here");
        //

        let record : csv::StringRecord = result.unwrap();
        if verbose >= 2 { 
            eprintln!("DBG: rec: {:?}", record);
        } 
        let pos = record.position().expect("a record position");
        bytecount = pos.byte();
        ss.clear();

        let mut i = 0;
        if key_fields.len() > 0 {
            while i < key_fields.len() {
                let index = key_fields[i];
                if index < record.len() {
                    ss.push_str(&record[index]);
                } else {
                    ss.push_str("NULL");
                }
                if i != key_fields.len()-1 {
                    ss.push('|');
                }
                i += 1;
            }
        } else {
            ss.push_str("NULL");
        }

        if sum_fields.len() > 0 {
            sum_grab.truncate(0);
            i=0;
            while i < sum_fields.len() {
                let index = sum_fields[i];
                if index < record.len() {
                    let v = &record[index];
                    match v.parse::<f64>() {
                        Err(_) => {
                            if verbose>=0 {
                                eprintln!("error parseing string |{}| as a float for summary index: {} so pretending value is 0",v, index);
                            }
                            sum_grab.push(0f64);},
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
            i=0;
            while i < unique_fields.len() {
                let index = unique_fields[i];
                if index < record.len() {
                    uni_grab.push(record[index].to_string());
                } else {
                    uni_grab.push("NULL".to_string());
                }
                i += 1;
            }
        }

        // if ss.len() > 0 
        {
            rowcount += 1;
            fieldcount += record.len();
            {
                let v = hm.entry(ss.clone()).or_insert(KeySum{ count: 0, sums: sum_grab.to_vec(), distinct: Vec::new() });
                v.count = v.count +1;
                for (i,f) in sum_grab.iter().enumerate() {
                    v.sums[i] = v.sums[i] + f;
                }
                if uni_grab.len() > 0 {
                    while v.distinct.len() < uni_grab.len() {
                        v.distinct.push(HashSet::new());
                    }
                    for (i,u) in uni_grab.iter().enumerate() {
                        v.distinct[i].insert(u.to_string());
                    }
                }
            }
        }

    }

    (rowcount, fieldcount, bytecount)
}
