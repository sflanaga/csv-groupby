use std::io::BufWriter;

fn main() {
    let stdout = std::io::stdout();
    let _buffered = BufWriter::new(stdout.lock());

    // create test output for review
    let mut input_str = String::new();
    for i in 1..1000 {
        let q = i % 10;
        let j = i * 10;
        let k = i as f64 * 2.0f64;
        input_str.push_str(&format!("{},{},{},{}", q, i, j, k));
        if i < 999 {
            input_str.push_str("\n");
        }
    }
    println!("{}", &input_str);

    /*
    for i in 1..100_000_000 {
        let j = i % 1000;
        let k = j % 10;
        let l = (j + 500) * 3;
        writeln!(buffered, "{},{},{},{}", i, j, k, l).unwrap();
    }
    */
}
