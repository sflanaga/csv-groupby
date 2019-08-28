use std::io::prelude::*;
use std::io::BufWriter;

fn main() {
    let stdout = std::io::stdout();
    let mut buffered = BufWriter::new(stdout.lock());
    for i in 1..100_000_000 {
        let j = i % 1000;
        let k = j % 10;
        let l = (j + 500) * 3;
        write!(buffered, "{},{},{},{}\n", i, j, k, l).unwrap();
    }
}
