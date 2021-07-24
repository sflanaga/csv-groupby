use std::collections::{HashMap, BTreeMap};
use std::time::Instant;
use crate::cli::CliCfg;
use crate::{KEY_DEL, MyMap};
use std::cmp::{min, max};
use prettytable::Table;
use itertools::Itertools;

#[derive(Debug)]
pub struct SchemaSample {
    pub matrix: Vec<Vec<String>>,
    pub num: u32,
}


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

pub fn parse_and_merge_f64<T, F>(_line: &str, record: &T, rec_len: usize, keysum: &mut KeySum, startpos: usize, fields: &Vec<usize>, cfg: &CliCfg, mergefield: F)
    where
        <T as std::ops::Index<usize>>::Output: AsRef<str>,
        T: std::ops::Index<usize> + std::fmt::Debug,
        F: Fn(&Option<f64>, f64) -> f64
{
    for (i, index) in fields.iter().enumerate() {
        let place = i + startpos;
        if *index < rec_len {
            let v = &record[*index].as_ref();

            match v.parse::<f64>() {
                Err(_) => {
                    if cfg.verbose > 2 {
                        eprintln!("error parsing string |{}| as a float for summary: {} so pretending value is 0", v, index);
                    }
                }
                Ok(vv) => keysum.nums[place] = Some(mergefield(&keysum.nums[place], vv)),
            }
        }
    }
    // before lambdas used... this worked too:
    /*

    parse_and_merge_f64(line, record, rec_len, &mut brec,
                        start, &cfg.min_num_fields,
                        cfg, |dest, new| -> (f64) {
            match dest {
                Some(x) => x.min(new),
                None => new,
            }
        });

     */
}


impl SchemaSample {
    pub fn new(num: u32) -> Self {
        SchemaSample {
            matrix: vec![],
            num,
        }
    }
    pub fn schema_rec<T>(self: &mut Self, record: &T, rec_len: usize)
        where
            <T as std::ops::Index<usize>>::Output: AsRef<str>,
            T: std::ops::Index<usize> + std::fmt::Debug,
    {
        if self.matrix.len() == 0 {
            for i in 1..rec_len+1 {
                let mut row = vec![];
                row.push(i.to_string());
                self.matrix.push(row);
            }
        }

        for i in 0 ..rec_len {
            let mut v = self.matrix.get_mut(i);
            if ( v.is_none()) {
                // just in case the "header" or other previous line is not as long as this one...
                self.matrix.push(vec![]);
                self.matrix.get_mut(i).unwrap().push(i.to_string());
            }
            self.matrix
                .get_mut(i)
                .expect("should not get here - just added i'th item to matrix")
                .push(String::from(record.index(i).as_ref()));
        }
    }

    pub fn print_schema(self: &mut Self, cfg: &CliCfg) {
        if self.matrix.len() <= 0 {
            return;
        } else {
            let mut padding = vec![];

            let mut header = vec![];
            header.push("index".to_string());
            let num_cols = self.matrix.get(0).unwrap().len();
            for c in 1..num_cols {
                header.push(format!("line{}", c));
            }
            self.matrix.insert(0,header);

            for r in 0..self.matrix.len() {
                for c in 0..num_cols {
                    let this_len = self.matrix.get(r).unwrap().get(c).unwrap_or(&cfg.null).len();
                    match padding.get_mut(c) {
                        Some(curr) => *curr = max(*curr,this_len),
                        None => padding.push(this_len),
                    }
                }
            }

            let null_str = String::from("NULL");

            for r in 0..self.matrix.len() {
                let num_cols = self.matrix.get(0).unwrap().len();
                for c in 0..num_cols {
                    print!("{:>padding$}", self.matrix.get(r).unwrap().get(c).unwrap_or(&cfg.null), padding=padding.get(c).unwrap());
                    if c < num_cols-1 {
                        print!("{} ", cfg.od);
                    } else {
                        println!();
                    }
                }
            }
        }
        std::process::exit(0);
    }

    pub fn done(self: &Self) -> bool {
        if  self.matrix.len() <= 0 {
            return false;
        } else {
            return self.matrix.get(0).unwrap().len() > self.num as usize
        }
    }

}

pub fn schema_rec<T>(matrix: &mut Vec<Vec<String>>, ss: &mut String, line: &str, record: &T, rec_len: usize, map: &mut MyMap, cfg: &CliCfg, rowcount: &mut usize) -> (usize,usize)
    where
        <T as std::ops::Index<usize>>::Output: AsRef<str>,
        T: std::ops::Index<usize> + std::fmt::Debug,
{
    if matrix.len() == 0 {
        for i in 0..rec_len {
            let mut row = vec![];
            row.push(i.to_string());
            matrix.push(row);
        }
    }

    for v in matrix {
        for i in 0 ..rec_len {
            v.push(String::from(record.index(i).as_ref()));
        }
    }

    (0,0)
}

pub fn store_rec<T>(ss: &mut String, line: &str, record: &T, rec_len: usize, map: &mut MyMap, cfg: &CliCfg, rowcount: &mut usize) -> (usize,usize,usize)
    where
        <T as std::ops::Index<usize>>::Output: AsRef<str>,
        T: std::ops::Index<usize> + std::fmt::Debug,
{
    //let mut ss: String = String::with_capacity(256);
    ss.clear();

    let mut fieldcount = 0usize;
    let mut skip_parse_fields = 0usize;
    let mut lines_filtered = 0usize;
    use pcre2::bytes::{CaptureLocations as CaptureLocations_pcre2, Captures as Captures_pcre2, Regex as Regex_pre2};

    if cfg.verbose >= 3 {
        if line.len() > 0 {
            eprintln!("DBG:  {:?}  from: {}", &record, line);
        } else {
            eprintln!("DBG:  {:?}", &record);
        }
    }

    if let Some(ref where_re) = cfg.where_re {
        for (fi, re) in where_re {
            if *fi < rec_len {
                match re.is_match(record[*fi].as_ref().as_bytes()) {
                    Err(e) => eprintln!("error trying to match \"{}\" with RE \"{}\" for where check, error: {}", record[*fi].as_ref(), re.as_str(), &e),
                    Ok(b) => if !b { return (0, 0, 1) },
                }
            }
        }
    }

    if let Some(ref where_not_re) = cfg.where_not_re {
        for (fi, re) in where_not_re {
            if *fi < rec_len {
                match re.is_match(record[*fi].as_ref().as_bytes()) {
                    Err(e) => eprintln!("error trying to match \"{}\" with RE \"{}\" for where_not check, error: {}", record[*fi].as_ref(), re.as_str(), &e),
                    Ok(b) => if b { return (0, 0, 1) },
                }
            }
        }
    }

    if cfg.key_fields.len() > 0 {
        fieldcount += rec_len;
        for i in 0..cfg.key_fields.len() {
            let index = cfg.key_fields[i];
            if index < rec_len {
                // re.is_match(&record[index].as_ref().as_bytes());
                ss.push_str(&record[index].as_ref());
            } else {
                ss.push_str(&cfg.null);
            }
            ss.push(KEY_DEL as char);
        }
        ss.pop(); // remove the trailing KEY_DEL instead of check each iteration
        // we know we get here because of the if above.
    } else {
        ss.push_str(&cfg.null);
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

    let mut mergefields = |fields: &Vec<usize>, startpos: usize, comment: &str, mergefield: &dyn Fn(&Option<f64>, f64) -> f64| -> () {
        for (i, index) in fields.iter().enumerate() {
            let place = i + startpos;
            let v = &record[*index].as_ref();
            match v.parse::<f64>() {
                Err(_) => {
                    skip_parse_fields += 1;
                    if cfg.verbose > 1 {
                        eprintln!("Error parsing string \"{}\" as a float so skipping it. Intended for {} slot: {}", v, comment, place);
                    }
                }
                Ok(vv) => brec.nums[place] = Some(mergefield(&brec.nums[place], vv)),
            }
        }
    };

    let sumf64 = |dest:&Option<f64>, new:f64| -> f64 {
        match dest {
            Some(x) => *x + new,
            None => new,
        }
    };

    let minf64 = |dest:&Option<f64>, new:f64| -> f64 {
        match dest {
            Some(x) => x.min(new),
            None => new,
        }
    };

    let maxf64 = |dest:&Option<f64>, new:f64| -> f64 {
        match dest {
            Some(x) => x.max(new),
            None => new,
        }
    };

    if cfg.sum_fields.len() > 0 {
        mergefields(&cfg.sum_fields, 0, "sum", &sumf64);
    }
    if cfg.min_num_fields.len() > 0 {
        mergefields(&cfg.min_num_fields,cfg.sum_fields.len(),"min", &minf64);
    }
    if cfg.max_num_fields.len() > 0 {
        mergefields(&cfg.max_num_fields, cfg.min_num_fields.len() + cfg.sum_fields.len(), "max", &maxf64);
    }

    if cfg.avg_fields.len() > 0 {
        for i in 0..cfg.avg_fields.len() {
            let index = cfg.avg_fields[i];
            if index < rec_len {
                let v = &record[index];
                match v.as_ref().parse::<f64>() {
                    Err(_) => {
                        skip_parse_fields += 1;
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

    (fieldcount,skip_parse_fields, lines_filtered)
}

fn merge_f64<F>(x: Option<f64>, y: Option<f64>, pickone: F) -> Option<f64>
    where F: Fn(f64, f64) -> f64
{
    return match (x, y) {
        (Some(old), Some(new)) => Some(pickone(old, new)),
        (Some(old), None) => Some(old),
        (None, Some(new)) => Some(new),
        (_, _) => None,
    };
}

fn merge_string<'a, F>(old: &'a mut Option<String>, new: &'a mut Option<String>, pickone: F) -> ()
    where F: Fn(&'a mut Option<String>, &'a mut Option<String>)
{
    match (&old, &new) {
        (Some(_), Some(_)) => pickone(old,new), //*new = old.take(), //f(new,old),
        (Some(_), None) => *new = old.take(),
        (None, Some(_)) => {}
        (_, _) => {}
    }
}

pub fn sum_maps(maps: &mut Vec<MyMap>, verbose: usize, cfg: &CliCfg) -> MyMap {
    let start = Instant::now();
    let lens = join(maps.iter().map(|x:&MyMap| x.len().to_string()), ",");

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
            for i in 0 .. cfg.sum_fields.len() {
                let dest = start + i;
                new.nums[dest] = merge_f64(old.nums[dest], new.nums[dest], |x, y| -> f64 { x + y });
            }

            start += cfg.sum_fields.len();
            for i in 0 .. cfg.min_num_fields.len() {
                let dest = start + i;
                new.nums[dest] = merge_f64(old.nums[dest], new.nums[dest], |x, y| -> f64 { x.min(y) });
            }

            start += cfg.min_num_fields.len();
            for i in 0 .. cfg.max_num_fields.len() {
                let dest = start + i;
                new.nums[dest] = merge_f64(old.nums[dest], new.nums[dest], |x, y| -> f64 { x.max(y) });
            }

            for j in 0..old.avgs.len() {
                new.avgs[j].0 += old.avgs[j].0;
                new.avgs[j].1 += old.avgs[j].1;
            }

            start = 0;
            for i in 0 .. cfg.min_str_fields.len() {
                let dest = start + i;
                merge_string(&mut old.strs[dest], &mut new.strs[dest], |old, new| {
                    if old < new { *new = old.take(); }
                });
            }

            start += cfg.min_str_fields.len();
            for i in 0 .. cfg.max_str_fields.len() {
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
