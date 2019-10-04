#![allow(dead_code)]
#![allow(unused_imports)]

use crossbeam_channel::bounded;
use regex::{Captures, Regex, RegexBuilder};
use std::thread;
//use pcre2::bytes::{Regex, Captures, RegexBuilder};
use bstr::{BStr, ByteSlice};
use std::collections::HashMap;
use std::rc::Rc;
use std::str::from_utf8;
use std::sync::Arc;

use std::cmp::{Ordering::*, Ord, Ordering};

pub mod gen;

fn main() {
    if let Err(e) = sort_parse_key() {
        println!("Application error: {}", e);
        std::process::exit(1);
    }
}

/*
9
dog
cat
aaaz
aaa
120
12.0
Pac
101
0
Cat

packet
*/

fn sort_parse_key() -> Result<(), std::io::Error> {
    let mut v = vec![];
    v.push("9");
    v.push("dog");
    v.push("cat");
    v.push("120");
    v.push("2|999|10");
    v.push("1|9|99");
    v.push("1|9|0");
    v.push("dog|1000|10");
    v.push("120");
    v.push("aaaz");
    v.push("aaa");
    v.push("12.0");
    v.push("Pac");
    v.push("101");
    v.push("0");
    v.push("");
    v.push("packet");
    v.push("Cat");

    let del = '|';

    fn str_cmp_ignore_case(a: &str, b: &str) -> Ordering {
        let mut acs = a.chars().flat_map(char::to_lowercase);
        let mut bcs = b.chars().flat_map(char::to_lowercase);
        loop {
            match (acs.next(), bcs.next()) {
                (Some(a), Some(b)) => {
                    let x = a.cmp(&b);
                    if x == Equal { continue; }
                    else { return x; }
                },
                (None, None) => return Equal,
                (Some(_), None) => return Greater,
                (None, Some(_)) => return Less,
            }
        }
    }

    let cmp_f64_str = |l_k: &&str,r_k: &&str| -> Ordering {
        for (l,r) in l_k.split(del).zip(r_k.split(del)) {
            let res = {
                // compare as numbers if you can
                let lf = l.parse::<f64>();
                let rf = r.parse::<f64>();
                match (lf, rf) {
                    (Ok(lv), Ok(rv)) => lv.partial_cmp(&rv).unwrap_or(Equal),
                    // fall back to string comparison
                    (Err(_), Err(_)) => str_cmp_ignore_case(&l, &r), // l.cmp(r),
                    // here Err means not a number while Ok means a number
                    // number is less than string
                    (Ok(_), Err(_)) => Less,
                    // string is greater than number
                    (Err(_), Ok(_)) => Greater,
                }
            };
            if res != Equal {
                return res;
            } // else keep going if this level is equal
        }
        Equal
    };

    let chosen_cmp = cmp_f64_str;

    v.sort_unstable_by(chosen_cmp);

    println!("{:#?}", v);
    Ok(())
}


fn test_regrow_str() -> Result<(), std::io::Error> {

    fn reused_str_vec<'a>(idx: usize, v: &mut Vec<& 'a str>, s:  & 'a str) {
        if idx+1 >= v.len() {
            eprintln!("grow for: {} vs {}", idx, v.len());
            let curr_len = v.len();
            for i in curr_len..idx+1 {
                eprintln!("grow i: {} ", i);
                v.push("");
            }
        }
        eprintln!("clear and set idx: {} with: {}", idx, s);
        v[idx] = "";
        v[idx] = s;
    }

    let strs = ["one", "two", "three", "four", "five"];

    let mut v = vec![];

    for i in (0..4).rev() {
        for j in (0..i).rev() {
            eprintln!("j: {} ", j);
            reused_str_vec(j, &mut v, strs[j]);
        }
        println!("vec: {:#?}", v);
        //v.clear();
    }
    Ok(())

}

fn test_regrow_string() -> Result<(), std::io::Error> {
    fn reused_str_vec(idx: usize, v: &mut Vec<String>, s: &str) {
        if idx+1 >= v.len() {
            eprintln!("grow for: {} vs {}", idx, v.len());
            let curr_len = v.len();
            for i in curr_len..idx+1 {
                eprintln!("grow i: {} ", i);
                v.push(String::new());
            }
        }
        eprintln!("clear and set idx: {} with: {}", idx, s);
        v[idx].clear();
        v[idx].push_str(s);
    }

    let strs = ["one", "two", "three", "four", "five"];

    let mut v = vec![];

    for i in (0..4).rev() {
        for j in (0..i).rev() {
            eprintln!("j: {} ", j);
            reused_str_vec(j, &mut v, strs[j]);
        }
        println!("vec: {:#?}", v);
        //v.clear();
    }
    Ok(())
}

fn main_vec_used() {
    let x = Arc::new({
        let mut x = vec![];
        for i in 0..5 {
            x.push(i * 2 + 100);
        }
        x
    });
    {
        let xx = x.clone();
        let _h = std::thread::spawn( move|| {
            for v in xx.iter() {
                println!("{}", v);
            }
        });
    }

    for v in x.iter() {
        println!("{}", v);
    }
}

fn test_pcre2_main() {
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
    for _ in 0..4 {
        s1.send(None).unwrap();
        s2.send(None).unwrap();
    }
    h_1.into_iter().for_each(|x| x.join().expect("error joining h_1"));
    h_2.into_iter().for_each(|x| x.join().expect("error joining h_2"));
}
