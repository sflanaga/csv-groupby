use grep_cli::DecompressionReader;
use std::collections::HashMap;
use std::fmt::Display;
use std::io::{Read, BufRead, Write, BufReader, BufWriter};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::{fs, thread};
use pcre2::bytes::{Captures as Captures_pcre2, Regex as Regex_pre2};
use std::fs::File;
use flate2::read::GzDecoder;

#[allow(dead_code)]
pub fn distro_format<T,S>(map: &HashMap<T, usize, S>, upper: usize, bottom: usize) -> String
where
    T: std::fmt::Display + std::fmt::Debug + std::clone::Clone + Ord,
{
    let mut vec: Vec<(usize, T)> = Vec::with_capacity(map.len());
    for x in map.iter() {
        vec.push((*x.1, x.0.clone()));
    }
    vec.sort_by(|x, y| {
        let ev = y.0.cmp(&x.0);
        // we do the extra ordering here to get predictable results
        // for testing and for user sanity
        if ev == core::cmp::Ordering::Equal {
            // println!("having the compare {} {}", y.1, x.1);
            x.1.cmp(&y.1)
        } else {
            ev
        }
    });

    let mut msg = String::with_capacity(16);
    if upper + bottom >= vec.len() {
        for e in &vec {
            msg.push_str(&format!("({} x {})", e.1, e.0));
        }
    } else {
        for e in vec.iter().take(upper) {
            msg.push_str(&format!("({} x {})", e.1, e.0));
        }
        msg.push_str(&format!("..{}..", vec.len() - (bottom + upper)));
        for e in vec.iter().skip(vec.len() - bottom) {
            msg.push_str(&format!("({} x {})", e.1, e.0));
        }
    }
    msg
}
#[test]
fn test_distro_format() {
    let mut v: HashMap<String, usize> = HashMap::new();
    v.insert("a".to_string(), 10);
    v.insert("z".to_string(), 111);
    v.insert("c".to_string(), 11);
    v.insert("q".to_string(), 5);

    let result = "0 0 distro: ..4..
0 1 distro: ..3..(q x 5)
0 2 distro: ..2..(a x 10)(q x 5)
0 3 distro: ..1..(c x 11)(a x 10)(q x 5)
1 0 distro: (z x 111)..3..
1 1 distro: (z x 111)..2..(q x 5)
1 2 distro: (z x 111)..1..(a x 10)(q x 5)
1 3 distro: (z x 111)(c x 11)(a x 10)(q x 5)
2 0 distro: (z x 111)(c x 11)..2..
2 1 distro: (z x 111)(c x 11)..1..(q x 5)
2 2 distro: (z x 111)(c x 11)(a x 10)(q x 5)
2 3 distro: (z x 111)(c x 11)(a x 10)(q x 5)
3 0 distro: (z x 111)(c x 11)(a x 10)..1..
3 1 distro: (z x 111)(c x 11)(a x 10)(q x 5)
3 2 distro: (z x 111)(c x 11)(a x 10)(q x 5)
3 3 distro: (z x 111)(c x 11)(a x 10)(q x 5)
";

    let mut res = String::new();
    for u in 0..4 {
        for b in 0..4 {
            res.push_str(&format!("{} {} distro: {}\n", u, b, &distro_format(&v, u, b)));
        }
    }
    assert_eq!(&res, result, "distro_format cmp failed");
}

fn mem_metric<'a>(v: usize) -> (f64, &'a str) {
    const METRIC: [&str; 8] = ["B ", "KB", "MB", "GB", "TB", "PB", "EB", "ZB"];

    let mut size = 1usize << 10;
    for e in &METRIC {
        if v < size {
            return ((v as f64 / (size >> 10) as f64) as f64, e);
        }
        size <<= 10;
    }
    (v as f64, "")
}

/// keep only a few significant digits of a simple float value
fn sig_dig(v: f64, digits: usize) -> String {
    let x = format!("{}", v);
    let mut d = String::new();
    let mut count = 0;
    let mut found_pt = false;
    for c in x.chars() {
        if c != '.' {
            count += 1;
        } else {
            if count >= digits {
                break;
            }
            found_pt = true;
        }

        d.push(c);

        if count >= digits && found_pt {
            break;
        }
    }
    d
}

pub fn mem_metric_digit(v: usize, sig: usize) -> String {
    if v == 0 || v > std::usize::MAX / 2 {
        return format!("{:>width$}", "unknown", width = sig + 3);
    }
    let vt = mem_metric(v);
    format!("{:>width$} {}", sig_dig(vt.0, sig), vt.1, width = sig + 1,)
}

#[test]
fn test_mem_metric_digit() -> Result<(), Box<dyn std::error::Error>> {
    for t in &[
        (0, "unknown"),
        (1, "    1 B "),
        (5, "    5 B "),
        (1024, "    1 KB"),
        (2524, "2.464 KB"),
        (1024 * 999, "  999 KB"),
        (1024 * 1023, " 1023 KB"),
        (11usize << 40, "   11 TB"),
    ] {
        //let v = mem_metric(*t);
        assert_eq!(mem_metric_digit(t.0, 4), t.1, "mem_metric_digit test");
        println!("{}\n>>>{}<<<\n", t.0, mem_metric_digit(t.0, 4));
    }
    Ok(())
}

pub fn greek(v: f64) -> String {
    const GR_BACKOFF: f64 = 24.0;
    const GROWTH: f64 = 1024.0;
    const KK: f64 = GROWTH;
    const MM: f64 = KK * GROWTH;
    const GG: f64 = MM * GROWTH;
    const TT: f64 = GG * GROWTH;
    const PP: f64 = TT * GROWTH;

    let a = v.abs();
    // println!("hereZ {}  {}  {}", v, MM-(GR_BACKOFF*KK), GG-(GR_BACKOFF*MM));
    let t = if a > 0.0 && a < KK - GR_BACKOFF {
        (v, "B")
    } else if a >= KK - GR_BACKOFF && a < MM - (GR_BACKOFF * KK) {
        // println!("here {}", v);
        (v / KK, "K")
    } else if a >= MM - (GR_BACKOFF * KK) && a < GG - (GR_BACKOFF * MM) {
        // println!("here2 {}  {}  {}", v, MM-(GR_BACKOFF*KK), GG-(GR_BACKOFF*MM));
        (v / MM, "M")
    } else if a >= GG - (GR_BACKOFF * MM) && a < TT - (GR_BACKOFF * GG) {
        // println!("here3 {}", v);
        (v / GG, "G")
    } else if a >= TT - (GR_BACKOFF * GG) && a < PP - (GR_BACKOFF * TT) {
        // println!("here4 {}", v);
        (v / TT, "T")
    } else {
        // println!("here5 {}", v);
        (v / PP, "P")
    };

    let mut s = format!("{}", t.0);
    s.truncate(4);
    if s.ends_with('.') {
        s.pop();
    }

    format!("{}{}", s, t.1)
    // match v {
    // 	(std::ops::Range {start: 0, end: (KK - GR_BACKOFF)}) => v,
    // 	//KK-GR_BACKOFF .. MM-(GR_BACKOFF*KK)			=> v,
    // 	_ => v,
    // }
}
pub fn user_pause() {
    println!("hi enter to continue...");
    let mut buf: [u8; 1] = [0; 1];
    let stdin = ::std::io::stdin();
    let mut stdin = stdin.lock();
    let _it = stdin.read(&mut buf[..]);
}

#[derive(Default, Debug)]
pub struct IoSlicerStatus {
    pub bytes: AtomicUsize,
    pub files: AtomicUsize,
    pub curr_file: Mutex<String>,
}

impl IoSlicerStatus {
    pub fn new() -> IoSlicerStatus {
        IoSlicerStatus {
            bytes: AtomicUsize::new(0),
            files: AtomicUsize::new(0),
            curr_file: Mutex::new(String::new()),
        }
    }
}

#[derive(Debug)]
pub struct FileSlice {
    pub block: Vec<u8>,
    pub len: usize,
    pub index: usize,
    pub filename: String,
	pub sub_grps: Vec<String>,
}

fn fill_buff(handle: &mut dyn Read, buff: &mut [u8]) -> Result<usize, std::io::Error> {
    // eprintln!("call fill");
    let mut sz = handle.read(&mut buff[..])?;
    // eprintln!("mid read: {}", sz);
    loop {
        if sz == 0 {
            return Ok(sz);
        } else if sz == buff.len() {
            return Ok(sz);
        }

        let sz2 = handle.read(&mut buff[sz..])?;
        // eprintln!("mid2 read: {}", sz2);

        if sz2 == 0 {
            return Ok(sz);
        } else {
            sz += sz2;
        }
    }

}

pub fn io_thread_slicer(
    recv_blocks: &crossbeam_channel::Receiver<Vec<u8>>,
    currfilename: &dyn Display,
    file_subgrps: &[String],
    block_size: usize,
    recycle_io_blocks_disable: bool,
    verbosity: usize,
    handle: &mut dyn Read,
    status: &mut Arc<IoSlicerStatus>,
    send: &crossbeam_channel::Sender<Option<FileSlice>>,
) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    if verbosity >= 2 {
        eprintln!("Using block_size {} bytes", block_size);
    }

    {
        let mut curr_file = status.curr_file.lock().unwrap();
        curr_file.clear();
        curr_file.push_str(format!("{}", currfilename).as_str());
    }
    status.files.fetch_add(1, Ordering::Relaxed);

    let mut block_count = 0;
    let mut bytes = 0;

    let mut holdover = vec![0u8; block_size];
    let mut left_len = 0;
    let mut last_left_len;
    let mut curr_pos = 0usize;
    loop {
        let mut block = if !recycle_io_blocks_disable { recv_blocks.recv()? } else { vec![0u8; block_size] };
        if left_len > 0 {
            block[0..left_len].copy_from_slice(&holdover[0..left_len]);
        }
        let (expected_sz, sz) = {
            let expected_sz = block.len() - left_len;
            let sz = fill_buff(handle, &mut block[left_len..])?; 
            (expected_sz, sz)
        };

        let apparent_eof = expected_sz != sz;

        let mut end = 0;
        let mut found = false;
        for i in (0..(sz + left_len)).rev() {
            end = i;
            match block[end] {
                b'\r' | b'\n' => {
                    found = true;
                    break;
                }
                _ => {}
            }
        }
        // println!("read block: {:?}\nhold block: {:?}", block, holdover);

        curr_pos += sz;
        if sz > 0 {
            block_count += 1;
            if !found {
                last_left_len = left_len;
                left_len = 0;
                //holdover[0..left_len].copy_from_slice(&block[end..sz + last_left_len]);

                if !apparent_eof {
                    eprintln!("WARNING: sending no EOL found in block around file:pos {}:{} ", currfilename, curr_pos);
                }
                send.send(Some(FileSlice {
                    block,
                    len: sz + last_left_len,
                    index: block_count,
                    filename: currfilename.to_string(),
					sub_grps: file_subgrps.to_vec(),
                }))?;
                status.bytes.fetch_add(sz, Ordering::Relaxed);
                bytes += sz;

            // panic!("Cannot find end line marker in current block at pos {} from file {}", curr_pos, currfilename);
            } else {
                last_left_len = left_len;
                left_len = (sz + last_left_len) - (end + 1);
                holdover[0..left_len].copy_from_slice(&block[end + 1..sz + last_left_len]);

                if verbosity > 2 {
                    eprintln!("sending found EOL at {} ", end + 1);
                }
                send.send(Some(FileSlice {
                    block,
                    len: end + 1,
                    index: block_count,
                    filename: currfilename.to_string(),
					sub_grps: file_subgrps.to_vec(),
                }))?;
                status.bytes.fetch_add(end + 1, Ordering::Relaxed);
                bytes += end + 1;
            }
        } else {
            if verbosity > 2 {
                eprintln!("sending tail len {} on file: {} ", left_len, currfilename);
            }
            send.send(Some(FileSlice {
                block,
                len: left_len,
                index: block_count,
                filename: currfilename.to_string(),
				sub_grps: file_subgrps.to_vec(),
            }))?;
            bytes += left_len;
            status.bytes.fetch_add(left_len, Ordering::Relaxed);

            break;
        }
    }
    Ok((block_count, bytes))
}

pub fn caps_to_vec_strings(caps: &Captures_pcre2) -> Vec<String> {
	let mut v = vec![];
	for x in 1 .. caps.len() {
        let y = caps.get(x).unwrap().as_bytes();
        let ss = String::from_utf8(y.to_vec()).unwrap();
		v.push( ss);
	}
	v
}

pub fn subs_from_path_buff(path: &PathBuf, regex: &Option<Regex_pre2>) -> (bool, Vec<String>) {
	let v = vec![];
	match regex {
		None => (true,v),
		Some(re) => {
			match re.captures(path.to_str().unwrap().as_bytes()) {
				Ok(Some(caps)) => (true, caps_to_vec_strings(&caps)),
				_ => (false, v)
			}
		}
	}
}

pub fn per_file_thread(
    recycle_io_blocks_disable: bool,
    recv_blocks: &crossbeam_channel::Receiver<Vec<u8>>,
    recv_pathbuff: &crossbeam_channel::Receiver<Option<PathBuf>>,
    send_fileslice: &crossbeam_channel::Sender<Option<FileSlice>>,
    block_size: usize,
    verbosity: usize,
    mut io_status: Arc<IoSlicerStatus>,
    path_re: &Option<Regex_pre2>,
) -> (usize, usize) {
    let mut block_count = 0usize;
    let mut bytes = 0usize;
    loop {
        let filename = match recv_pathbuff.recv().expect("thread failed to get next job from channel") {
            Some(path) => path,
            None => {
                if verbosity > 1 {
                    eprintln!("slicer exit on None {}", thread::current().name().unwrap());
                }
                return (block_count, bytes);
            }
        };
        if verbosity > 2 { eprintln!("considering path: {}", filename.display()); }
		let (file_matched, file_subgrps) = subs_from_path_buff(&filename, &path_re);
		if !file_matched {
			if verbosity > 0 { eprintln!("file: {} did not match expected pattern", filename.display()); }
			continue;
		}

        match fs::metadata(&filename) {
            Ok(m) => if !m.is_file() { continue; },
            Err(err) => {
                eprintln!("skipping file \"{}\", could not get stats on it, cause: {}", filename.display(), err);
                continue;
            }
        };

        if verbosity >= 1 {
            eprintln!("processing file: {}", filename.display());
        }

        let ext = {
            match filename.to_str().unwrap().rfind('.') {
                None => String::from(""),
                Some(i) => String::from(&filename.to_str().unwrap()[i..]),
            }
        };
        let mut rdr:Box<dyn Read> = match &ext[..] {
            ".gz" => {
                match File::open(&filename) {
                    Ok(f) => Box::new(BufReader::new(GzDecoder::new(f))),
                    Err(err) => {
                        eprintln!("skipping file \"{}\", due to error: {}", filename.display(), err);
                        continue;
                    },
                }
            },
            ".zst" => {
                match File::open(&filename) {
                    Ok(f) => {
                        match zstd::stream::read::Decoder::new(f) {
                            Ok(d) =>Box::new(BufReader::new(d)),
                            Err(err) => {
                                eprintln!("skipping file \"{}\", zstd decoder error: {}", filename.display(), err);
                                continue;
                            }
                        }
                    },
                    Err(err) => {
                        eprintln!("skipping file \"{}\", due to error: {}", filename.display(), err);
                        continue;
                    },
                }
            },
            _ => {
                match DecompressionReader::new(&filename) {
                    Ok(rdr) => Box::new(rdr),
                    Err(err) => {
                        eprintln!("skipping file \"{}\", due to error: {}", filename.display(), err);
                        continue;
                    },
                }
            },
        };
        match io_thread_slicer(
            &recv_blocks,
            &filename.display(),
            &file_subgrps,
            block_size,
            recycle_io_blocks_disable,
            verbosity,
            &mut rdr,
            &mut io_status,
            &send_fileslice,
        ) {
            Ok((bc, by)) => {
                block_count += bc;
                bytes += by;
            }
            Err(err) => eprintln!("error io slicing {}, error: {}", &filename.display(), err),
        }
    }
}


#[cfg(target_os = "linux")]
pub fn get_reader_writer() -> (impl BufRead, impl Write) {
    use std::os::unix::io::FromRawFd;
    let stdin = unsafe { File::from_raw_fd(0) };
    let stdout = unsafe { File::from_raw_fd(1) };
    let (reader, writer) = (BufReader::new(stdin), BufWriter::new(stdout));
    (reader, writer)
}
#[cfg(target_os = "windows")]
pub fn get_reader_writer() -> (impl BufRead, impl Write) {
    use std::os::windows::io::{AsRawHandle, FromRawHandle};
    let stdin = unsafe { File::from_raw_handle(std::io::stdin().as_raw_handle()) };
    let stdout = unsafe { File::from_raw_handle(std::io::stdout().as_raw_handle()) };

    let (reader, writer) = (BufReader::new(stdin), BufWriter::new(stdout));
    (reader, writer)
}