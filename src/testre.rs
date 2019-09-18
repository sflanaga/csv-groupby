use std::io::prelude::*;

use crate::cli::CliCfg;
//use regex::Regex;

use pcre2::bytes::{Regex, Captures, CaptureLocations};
use std::sync::atomic::compiler_fence;
use regex::internal::Input;
use std::ops::Index;

#[derive(Debug)]
struct CapWrap<'t> {
    pub cl: &'t CaptureLocations,
    pub text: &'t str,
}
//T: std::ops::Index<usize> + std::fmt::Debug,
//<T as std::ops::Index<usize>>::Output: AsRef<str>,

impl<'t> Index<usize> for CapWrap<'t> {
    type Output = str;

    fn index(&self, i: usize) -> &Self::Output {
        self.cl.get(i)
            .map(|m| &self.text[m.0..m.1])
            .unwrap_or_else(|| panic!("no group at index '{}'", i))
    }
}

impl<'t> CapWrap<'t> {
    fn len(&self) -> usize {
        self.cl.len()
    }
}

pub fn testre(c: &CliCfg) -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing RE");
    let sre = c.testre.as_ref().unwrap();
    let re = Regex::new(sre)?;

    if c.testlines.len() > 0 {
        for l in &c.testlines {
            teststr(&re, &l, &sre, c.verbose)?;
        }
    } else if c.files.len() <= 0 {
        eprintln!("<<< waiting on input lines");
        let stdin = std::io::stdin();
        for l in stdin.lock().lines().map(|l| l.unwrap()) {
                teststr(&re, &l, &sre, c.verbose)?;
        }
    }
    Ok(())
}

fn teststr(re: &Regex, l: &str, sre: &str, verbose: usize) -> Result<(), Box<dyn std::error::Error>> {
    let mut inner_caps = re.capture_locations();
    if let Some(record) = re.captures_read(&mut inner_caps, l.as_bytes())? {
        let mut caps = CapWrap{ cl:&mut inner_caps, text: l };
        println!("RE: \"{}\"\nline: \"{}\"", sre, l);
        if verbose > 0 { eprintln!("DBG: record: {:#?}", caps); }
        for i in 0..caps.len() {
            println!("\t{}: \"{}\"", i, &caps[i]);
        }
        println!("");
    } else {
        eprintln!("NO match with RE \"{}\" against: \"{}\"", sre, l);
    }
    Ok(())
}