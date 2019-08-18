use std::fmt::Display;
use std::io::Read;

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

use crossbeam_channel as channel;

#[derive(Debug)]
pub struct FileChunk {
	pub block: Vec<u8>,
	pub len: usize,
	pub index: usize,
}

pub fn io_thread_swizzle(
	currfilename: &dyn Display,
	block_size: usize,
	verbosity: usize,
	handle: &mut dyn Read,
	send: &channel::Sender<Option<FileChunk>>,
) -> Result<usize, std::io::Error> {
	if verbosity >= 2 {
		eprintln!("Using block_size {} bytes", block_size);
	}

	let mut split_slop = 0;
	let mut block_count = 0;
	let mut bytes = 0;

	let mut holdover = vec![0u8; block_size];
	let mut left_len = 0;
	let mut last_left_len;
	let mut curr_pos = 0usize;
	loop {
		let mut block = vec![0u8; block_size];
		if left_len > 0 {
			split_slop += left_len;
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
				send.send(Some(FileChunk {
					block: block,
					len: sz + last_left_len,
					index: block_count,
				}));
				bytes += sz;

			// panic!("Cannot find end line marker in current block at pos {} from file {}", curr_pos, currfilename);
			} else {
				last_left_len = left_len;
				left_len = (sz + last_left_len) - (end + 1);
				holdover[0..left_len].copy_from_slice(&block[end + 1..sz + last_left_len]);

				if verbosity > 2 {
					eprintln!("sending found EOL at {} ", end + 1);
				}
				send.send(Some(FileChunk {
					block: block,
					len: end + 1,
					index: block_count,
				}));
				bytes += end + 1;
			}
		} else {
			if verbosity > 2 {
				eprintln!("sending tail len {} on file: {} ", left_len, currfilename);
			}
			send.send(Some(FileChunk {
				block: block,
				len: left_len,
				index: block_count,
			}));
			bytes += left_len;

			break;
		}
	}

	Ok(bytes)
}
