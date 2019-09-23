#![allow(dead_code)]
#![allow(unused_imports)]

use crossbeam_channel::bounded;
use std::thread;
use regex::{RegexBuilder,Regex,Captures};
//use pcre2::bytes::{Regex, Captures, RegexBuilder};
use bstr::{BStr, ByteSlice};
use std::str::from_utf8;
use std::collections::HashMap;

pub mod gen;


fn main() {
//    let v = 18446744073686483000usize;
//    println!("v: {}  metric: {}   cmp: {}", v, gen::mem_metric_digit(v, 4), std::usize::MAX/2);

    let s = "dog,cat,cow;
milk,bread,flour
1,2,3
";
    let mut c = 0;
    let res = RegexBuilder::new(r#"(?m)cat.*^.*bread"#).multi_line(true).build().unwrap();
    //let re = Regex::new("^[^,]+,([^,]+),[^,]+$").unwrap();
    for caps in res.captures_iter(s) {
        println!("{:#?}", &caps);
        c += 1;
    }
    println!("ran {} matches", c);



}


fn test_mpmc() {

    let (s1, r1) = bounded(4);
    let (s2, r2) = bounded(2);

    let s1_c = s1.clone();
    let h1 = thread::spawn(move || {
        for i in 0..10 {
            //thread::sleep(Duration::from_millis(100));
            s1_c.send(Some(i)).expect("error in send");
        }
        println!("done sending");
    });
    let mut h_1 = vec![];

    for h in 0..4 {
        let r1_c = r1.clone();
        let s2_c = s2.clone();
        let hand = thread::spawn(move || loop {
            if let Some(v) = r1_c.recv().unwrap() {
                //thread::sleep(Duration::from_millis(100));
                println!("{}: mid  {}", h, v);
                s2_c.send(Some(v)).expect("error in send ph2");
            } else {
                println!("returning p1");
                return;
            }
        });
        h_1.push(hand);
    }

    let mut h_2 = vec![];
    for h in 0..4 {
        let r2_c = r2.clone();
        let hand = thread::spawn(move || loop {
            if let Some(v) = r2_c.recv().unwrap() {
                println!("{}: last {}", h, v);
            } else {
                println!("returning p2");
                return;
            }
        });
        h_2.push(hand);
    }
    h1.join().expect("error joining h1");
    println!("sending nones");
    for _ in 0..4  {
        s1.send(None).unwrap();
        s2.send(None).unwrap();
    }
    h_1.into_iter().for_each(|x| x.join().expect("error joining h_1"));
    h_2.into_iter().for_each(|x| x.join().expect("error joining h_2"));
}
