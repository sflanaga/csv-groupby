use std::collections::{HashMap, BTreeMap};
use std::time::Instant;
use crate::cli::CliCfg;
use crate::{KEY_DEL, MyMap};
use std::cmp::{min, max};


#[derive(Debug)]
pub struct KeySum {
    pub count: u64,
    pub nums: Vec<Option<f64>>,
    // sums, mins, maxs
    pub strs: Vec<Option<String>>,
    // min/max string???
    pub avgs: Vec<(f64, usize)>,
    pub distinct: Vec<HashMap<String, usize>>,
}

impl KeySum {
    pub fn new(num_len: usize, strs_len: usize, dist_len: usize, avg_len: usize) -> KeySum {
        KeySum {
            count: 0,
            nums: vec![None; num_len],
            strs: vec![None; strs_len],
            avgs: vec![(0f64, 0usize); avg_len],
            distinct: {
                let mut v = Vec::with_capacity(dist_len);
                for _ in 0..dist_len {
                    v.push(HashMap::new());
                }
                v
            },
        }
    }
}

pub fn store_rec<T>(ss: &mut String, line: &str, record: &T, rec_len: usize, map: &mut MyMap, cfg: &CliCfg, rowcount: &mut usize) -> usize
    where
        T: std::ops::Index<usize> + std::fmt::Debug,
        <T as std::ops::Index<usize>>::Output: AsRef<str>,
{
    //let mut ss: String = String::with_capacity(256);
    ss.clear();

    let mut fieldcount = 0usize;

    if cfg.verbose >= 3 {
        if line.len() > 0 {
            eprintln!("DBG:  {:?}  from: {}", &record, line);
        } else {
            eprintln!("DBG:  {:?}", &record);
        }
    }
    if cfg.key_fields.len() > 0 {
        fieldcount += rec_len;
        for i in 0..cfg.key_fields.len() {
            let index = cfg.key_fields[i];
            if index < rec_len {
                ss.push_str(&record[index].as_ref());
            } else {
                ss.push_str("NULL");
            }
            ss.push(KEY_DEL as char);
        }
        ss.pop(); // remove the trailing | instead of check each iteration
        // we know we get here because of the if above.
    } else {
        ss.push_str("NULL");
    }
    *rowcount += 1;

    let mut brec: &mut KeySum = {
        if let Some(v1) = map.get_mut(ss) {
            v1
        } else {
            let v2 = KeySum::new(cfg.sum_fields.len() + cfg.max_num_fields.len() + cfg.min_num_fields.len(),
                                 cfg.max_str_fields.len() + cfg.min_str_fields.len(), cfg.unique_fields.len(),
                                 cfg.avg_fields.len());
            map.insert(ss.clone(), v2);
            // TODO:  gree - just inserted but cannot use it right away instead of doing a lookup again?!!!
            // return v2 or &v2 does not compile
            map.get_mut(ss).unwrap()
        }
    };

    brec.count += 1;

    if cfg.sum_fields.len() > 0 {
        let start = 0;
        for (i, index) in cfg.sum_fields.iter().enumerate() {
            let place = i + start;
            if *index < rec_len {
                let v = &record[*index];
                match v.as_ref().parse::<f64>() {
                    Err(_) => {
                        if cfg.verbose > 2 {
                            eprintln!("error parsing string |{}| as a float for summary index: {} so pretending value is 0", v.as_ref(), index);
                        }
                    }
                    Ok(vv) => match &mut brec.nums[place] {
                        Some(x) => *x += vv,
                        None => brec.nums[place] = Some(vv),
                    }
                }
            }
        }
    }

    if cfg.min_num_fields.len() > 0 {
        let start = cfg.sum_fields.len();
        for (i, index) in cfg.min_num_fields.iter().enumerate() {
            if *index < rec_len {
                let dest = start + i;
                let v = &record[*index];
                match v.as_ref().parse::<f64>() {
                    Err(_) => {
                        if cfg.verbose > 2 {
                            eprintln!("error parsing string |{}| as a float for summary index: {} so pretending value is 0", v.as_ref(), index);
                        }
                    }
                    Ok(vv) => match &mut brec.nums[dest] {
                        Some(x) => if vv < *x { *x = vv; },
                        None => brec.nums[dest] = Some(vv),
                    }
                }
            }
        }
    }

    if cfg.max_num_fields.len() > 0 {
        let start = cfg.sum_fields.len() + cfg.min_num_fields.len();
        for (i, index) in cfg.max_num_fields.iter().enumerate() {
            if *index < rec_len {
                let v = &record[*index];
                let dest = start + i;
                match v.as_ref().parse::<f64>() {
                    Err(_) => {
                        if cfg.verbose > 2 {
                            eprintln!("error parsing string |{}| as a float for summary index: {} so pretending value is 0", v.as_ref(), index);
                        }
                    }
                    Ok(vv) => match &mut brec.nums[dest] {
                        Some(x) => if vv > *x { *x = vv; },
                        None => brec.nums[dest] = Some(vv),
                    }
                }
            }
        }
    }

    if cfg.avg_fields.len() > 0 {
        for i in 0..cfg.avg_fields.len() {
            let index = cfg.avg_fields[i];
            if index < rec_len {
                let v = &record[index];
                match v.as_ref().parse::<f64>() {
                    Err(_) => {
                        if cfg.verbose > 2 {
                            eprintln!("error parsing string |{}| as a float for summary index: {} so pretending value is 0", v.as_ref(), index);
                        }
                    }
                    Ok(vv) => {
                        brec.avgs[i].0 += vv;
                        brec.avgs[i].1 += 1;
                    }
                }
            }
        }
    }
    if cfg.unique_fields.len() > 0 {
        for i in 0..cfg.unique_fields.len() {
            let index = cfg.unique_fields[i];
            if index < rec_len {
                if !brec.distinct[i].contains_key(record[index].as_ref()) {
                    brec.distinct[i].insert(record[index].as_ref().to_string(), 1);
                } else {
                    let x = brec.distinct[i].get_mut(record[index].as_ref()).unwrap();
                    *x = *x + 1;
                }
            }
        }
    }

    if cfg.min_str_fields.len() > 0 {
        let start = 0;
        for (i, index) in cfg.min_str_fields.iter().enumerate() {
            let dest = i + start;
            if *index < rec_len {
                let v = &record[*index];
                match &mut brec.strs[dest] {
                    Some(x) => {
                        if x.as_str() > v.as_ref() {
                            x.clear();
                            x.push_str(v.as_ref());
                        }
                    }
                    None => brec.strs[dest] = Some(String::from(v.as_ref())),
                }
            }
        }
    }

    if cfg.max_str_fields.len() > 0 {
        let start = cfg.min_str_fields.len();
        for (i, index) in cfg.max_str_fields.iter().enumerate() {
            let dest = i + start;
            if *index < rec_len {
                let v = &record[*index];
                match &mut brec.strs[dest] {
                    Some(x) => {
                        if x.as_str() < v.as_ref() {
                            x.clear();
                            x.push_str(v.as_ref());
                        }
                    }
                    None => brec.strs[dest] = Some(String::from(v.as_ref())),
                }
            }
        }
    }

    fieldcount
}


fn merge_f64<F>(x: Option<f64>, y: Option<f64>, pickone: F) -> Option<f64>
    where F: Fn(f64, f64) -> f64
{
    return match (x,y) {
        (Some(old), Some(new)) => Some(pickone(new, old)),
        (Some(old), None) => Some(old),
        (None, Some(new)) => Some(new),
        (_, _) => None,
    }
}

fn merge_string<'a, F>(old: &'a mut Option<String>, new: &'a mut Option<String>, pickone:F) -> ()
    where F: Fn(&'a mut Option<String>, &'a mut Option<String>)
{
    match (&old,&new) {
        (Some(x), Some(y)) => pickone(new, old), //*new = old.take(), //f(new,old),
        (Some(x), None) => *new = old.take(),
        (None, Some(new)) => {},
        (_, _) => {},
    }
}

pub fn sum_maps(maps: &mut Vec<MyMap>, verbose: usize, cfg: &CliCfg) -> MyMap {
    let start = Instant::now();
    let lens = "NEEDTOFIXTHIS"; //join(maps.iter().map(|x:&MyMap| x.len().to_string()), ",");

    // println!("map count: {}", maps.len());
    // remove first map from list but keep / reuse it as a merge target
    let mut p_map = maps.remove(0);
    use itertools::join;
    for i in 0..maps.len() {
        for (k, old) in maps.get_mut(i).unwrap() {
            let new = p_map.entry(k.to_string()).or_insert(
                {
                    KeySum::new(old.nums.len(), old.strs.len(), old.distinct.len(),
                                old.avgs.len())
                });
            new.count += old.count;

            // need to provide proper sum, min, max operators

            let mut start = 0;
            for (i, index) in cfg.sum_fields.iter().enumerate() {
                let dest = start + i;
                new.nums[dest] = merge_f64(old.nums[dest], new.nums[dest], |x, y| -> f64 {x + y});
            }

            start += cfg.sum_fields.len();
            for (i, index) in cfg.min_num_fields.iter().enumerate() {
                let dest = start + i;
                new.nums[dest] = merge_f64(old.nums[dest], new.nums[dest], |x, y| -> f64 {x.min(y)});
            }

            start += cfg.min_num_fields.len();
            for (i, index) in cfg.max_num_fields.iter().enumerate() {
                let dest = start + i;
                new.nums[dest] = merge_f64(old.nums[dest], new.nums[dest], |x, y| -> f64 {x.max(y)});
            }

            for j in 0..old.avgs.len() {
                new.avgs[j].0 += old.avgs[j].0;
                new.avgs[j].1 += old.avgs[j].1;
            }

            start = 0;
            for (i, index) in cfg.min_str_fields.iter().enumerate() {
                let dest = start + i;
                merge_string(&mut old.strs[dest], &mut new.strs[dest], |old, new| {
                    if old < new { *new = old.take(); }
                });
            }

            start += cfg.min_str_fields.len();
            for (i, index) in cfg.max_str_fields.iter().enumerate() {
                let dest = start + i;
                merge_string(&mut old.strs[dest], &mut new.strs[dest], |old, new| {
                    if old > new { *new = old.take(); }
                });
            }

            for j in 0..old.distinct.len() {
                for (_ii, u) in old.distinct[j].iter().enumerate() {
                    if !new.distinct[j].contains_key(u.0) {
                        new.distinct[j].insert(u.0.clone(), *u.1);
                    } else {
                        let x = new.distinct[j].get_mut(u.0).unwrap();
                        *x = *x + *u.1;
                    }
                }
            }
        }
    }
    let end = Instant::now();
    let dur = end - start;
    if verbose > 0 {
        eprintln!("merge maps time: {:.3}s from map entry counts: [{}] to single map {} entries", dur.as_millis() as f64 / 1000.0f64,
                  lens, p_map.len());
    }

    p_map
}

/***
pub fn store_field<T>(ss: &mut String, line: &str, record: &T, rec_len: usize, brec: &mut KeySum, fieldmap: &Vec<usize>, start: usize, cfg: &CliCfg, parse: F) -> ()
    where
        T: std::ops::Index<usize> + std::fmt::Debug,
        F: Fn(&str) -> f64,
        <T as std::ops::Index<usize>>::Output: AsRef<str>,
{
    //
    let ll = record.iter().len();

    if fieldmap.len() > 0 {
        for (i, index) in fieldmap.iter().enumerate() {
            let place = i + start;
            if *index < rec_len {
                let v = &record[*index];

                let xv = parse_ref::<f64>(v.as_ref());

                match v.as_ref().parse::<f64>() {
                    Err(_) => {
                        if cfg.verbose > 2 {
                            eprintln!("error parsing string |{}| as a float for summary index: {} so pretending value is 0", v.as_ref(), index);
                        }
                    }
                    Ok(vv) => match &mut brec.nums[place] {
                        Some(x) => *x += vv,
                        None => brec.nums[place] = Some(vv),
                    }
                }
            }
        }
    }
}

***/
