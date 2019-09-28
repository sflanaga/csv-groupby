use std::sync::{Arc,Mutex,atomic::{AtomicUsize, Ordering}};
use std::fmt::Display;
use std::io::Read;
use std::path::PathBuf;
use std::{fs, thread};
use grep_cli::DecompressionReader;
use std::collections::HashMap;

pub fn distro_format<T>(map: &HashMap<T, usize>, upper: usize, bottom: usize) -> String
	where T: std::fmt::Display + std::fmt::Debug + std::clone::Clone + Ord
{
	let mut vec: Vec<(usize, T)> = Vec::with_capacity(map.len());
	for x in map.iter() {
		vec.push((*x.1,x.0.clone()));
	}
	vec.sort_by(|x,y| {
		let ev = y.0.cmp(&x.0);
		// we do the extra ordering here to get predictable results
		// for testing and for user sanity
		if ev == core::cmp::Ordering::Equal {
			y.1.cmp(&y.1)
		} else {
			ev
		}});

	let mut msg = String::with_capacity(16);
	if upper+bottom >= vec.len() {
		for i in 0..vec.len() {
			msg.push_str(&format!("({} x {})", vec[i].1, vec[i].0));
		}
	} else {
		for i in 0..upper {
			msg.push_str(&format!("({} x {})", vec[i].1, vec[i].0));
		}
		msg.push_str(&format!("..{}..", vec.len() - (bottom+upper) ));
		for i in vec.len()-bottom .. vec.len() {
			msg.push_str(&format!("({} x {})", vec[i].1, vec[i].0));
		}
	}
	msg
}
#[test]
fn test_distro_format() {


	let mut v: HashMap<String, usize> = HashMap::new();
	v.insert("a".to_string(), 10 );
	v.insert("z".to_string(), 111 );
	v.insert("c".to_string(), 11 );
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
			res.push_str(&format!("{} {} distro: {}\n",u,b, &distro_format(&v, u, b)));
		}
	}
	assert_eq!(&res, result, "distro_format cmp failed");


}

fn mem_metric<'a>(v: usize) -> (f64,&'a str) {
	const METRIC : [&str;8] = ["B ","KB","MB","GB","TB","PB","EB", "ZB"];

	let mut size = 1usize<<10;
	for i in 0..METRIC.len() {
		if v < size {
			return ( (v as f64 / (size>>10) as f64) as f64, &METRIC[i]);
		}
		size = size << 10;
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
			if count >= digits { break;}
			found_pt = true;
		}

		d.push(c);

		if count >= digits && found_pt { break; }
	}
	d
}

pub fn mem_metric_digit(v: usize, sig: usize) -> String {
    if v <= 0 || v > std::usize::MAX/2 {
        return format!("{:>width$}", "unknown", width=sig+3)
    }
	let vt = mem_metric(v);
    format!("{:>width$} {}", sig_dig(vt.0, sig), vt.1, width=sig+1, )
}

#[test]
fn test_mem_metric_digit() -> Result<(), Box<dyn std::error::Error>> {
	for t in &[ (0,"unknown"),
		(1,     "    1 B "),
		(5,     "    5 B "),
		(1024,  "    1 KB"),
		(2524,  "2.464 KB"),
		(1024*999,  "  999 KB"),
		(1024*1023, " 1023 KB"),
		(11usize << 40, "   11 TB")] {
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
	if s.ends_with(".") {
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


#[derive(Debug)]
pub struct IoSlicerStatus {
	pub bytes: AtomicUsize,
	pub files: AtomicUsize,
	pub curr_file: Mutex<String>
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
}

pub fn io_thread_slicer (
	recv_blocks: &crossbeam_channel::Receiver<Vec<u8>>,
	currfilename: &dyn Display,
	block_size: usize,
	recycle_io_blocks: bool,
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

	let mut block_count = 0;
	let mut bytes = 0;

	let mut holdover = vec![0u8; block_size];
	let mut left_len = 0;
	let mut last_left_len;
	let mut curr_pos = 0usize;
	loop {
		let mut block = if recycle_io_blocks {
			recv_blocks.recv()?
		} else {
			vec![0u8;block_size]
		};
		if left_len > 0 {
			block[0..left_len].copy_from_slice(&holdover[0..left_len]);
		}
		let (expected_sz, sz) = {
			let expected_sz = block.len() - left_len;
			let sz = handle.read(&mut block[left_len..])?;
			(expected_sz, sz)
		};

		let mut apparent_eof = false;

		if !(expected_sz == sz) {
			// eprintln!("<<<EOF expect {} size {}", expected_sz, sz);
			apparent_eof = true;
		}

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
					block: block,
					len: sz + last_left_len,
					index: block_count,
					filename: currfilename.to_string(),
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
					block: block,
					len: end + 1,
					index: block_count,
					filename: currfilename.to_string(),
				}))?;
				status.bytes.fetch_add(end + 1, Ordering::Relaxed);
				bytes += end + 1;
			}
		} else {
			if verbosity > 2 {
				eprintln!("sending tail len {} on file: {} ", left_len, currfilename);
			}
			send.send(Some(FileSlice {
				block: block,
				len: left_len,
				index: block_count,
				filename: currfilename.to_string(),
			}))?;
			bytes += left_len;
			status.bytes.fetch_add(left_len, Ordering::Relaxed);

			break;
		}
	}
	Ok((block_count, bytes))
}

pub fn per_file_thread(	recycle_io_blocks: bool, recv_blocks: &crossbeam_channel::Receiver<Vec<u8>>, recv_pathbuff: &crossbeam_channel::Receiver<Option<PathBuf>>,
					   send_fileslice: &crossbeam_channel::Sender<Option<FileSlice>>,
					   block_size: usize, verbosity: usize, mut io_status: Arc<IoSlicerStatus>) -> (usize,usize) {
	let mut block_count = 0usize;
	let mut bytes = 0usize;
	loop {
		let filename = match recv_pathbuff.recv().expect("thread failed to get next job from channel") {
			Some(path) => path,
			None => {
				if verbosity > 1 { eprintln!("slicer exit on None {}", thread::current().name().unwrap()); }
				return (block_count, bytes);
			}
		};
		let metadata = match fs::metadata(&filename) {
			Ok(m) => m,
			Err(err) => {
				eprintln!("skipping file \"{}\", could not get stats on it, cause: {}", filename.display(), err);
				continue;
			}
		};
		if !metadata.is_file() {
			continue;
		}

		if verbosity >= 1 {
			eprintln!("processing file: {}", filename.display());
		}
		let mut rdr = match DecompressionReader::new(&filename) {
			Ok(rdr) => rdr,
			Err(err) => {
				eprintln!("skipping file \"{}\", due to error: {}", filename.display(), err);
				continue;
			}
		};
		match io_thread_slicer(&recv_blocks, &filename.display(), block_size, recycle_io_blocks,
							   verbosity, &mut rdr, &mut io_status, &send_fileslice) {
			Ok((bc, by)) => { block_count += bc; bytes += by; },
			Err(err) => eprintln!("error io slicing {}, error: {}", &filename.display(), err),
		}
	}
}
