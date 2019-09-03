use std::io::prelude::*;

use cli::CliCfg;
use regex::Regex;

pub fn testre(c: &CliCfg) -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing RE");
    let sre = c.testre.as_ref().unwrap();
    let re = Regex::new(sre)?;

    if c.testlines.len() > 0 {
        for l in &c.testlines {
            teststr(&re, &l, &sre)?;
        }
    } else if c.files.len() <= 0 {
        eprintln!("<<< waiting on input lines");
        let stdin = std::io::stdin();
        for l in stdin.lock().lines().map(|l| l.unwrap()) {
                teststr(&re, &l, &sre)?;
        }
    }
    Ok(())
}

fn teststr(re: &Regex, l: &str, sre: &str) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(record) = re.captures(l) {
        println!("RE: \"{}\"\nline: \"{}\"", sre, l);
        for i in 0..record.len() {
            println!("\t{}: \"{}\"", i, &record[i]);
        }
        println!("");
    } else {
        eprintln!("NO match with RE \"{}\" against: \"{}\"", sre, l);
    }
    Ok(())
}