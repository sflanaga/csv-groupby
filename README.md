# csv - cheesey select-group-by on delimited files.

* Known bug - it may not report empty strings properly vs null etc.

csv is a command that takes a character delimited file [or stdin] and does a kind of 
SQL select-group-by on delimited data as a table.

Alternatively, you can use a regular expression (see -r option) against the lines where the groups captured become fields.

I wrote many "quick" perl one-liners to do this kind of thing, so I wrote this utility to simplify and speed things up, and to learn rust.

The delimited mode is super fast and memory efficient thanks to [rust csv library](https://github.com/BurntSushi/rust-csv) 
and rust collections in general.
The regular expression mode is now comparable per core to perl but effectively much faster as it will divide and conquer a midsized to larger file into several threads.  pcre2-jit 

## How-To:

This parses somefile.csv as a pipe delimited file and does a ```select f1,f4,count(*) from X group by f1,f4;```
```bash
csv -d '|' -k 0,3 -i somefile.csv
```

This does the same thing but from standard input.
```bash
cat somefile.csv | csv -k 0,3
```

```-c``` causes delimited format to be written instead of the default table auto-aligned format.
```bash
csv -k 0,1 -c ....
```


TODO/ideas:  

 Use stdin as a filelist source in addition to stdin as a data source
- additional aggregate functions?:  avg, min, max, empty_count, number_count, zero_count
  - avg done
- do more work to multi-line re mode - not sure how it should really work yet
- flat mode - no summary but just write X fields to the output as found - a kind s/()..()..()/$1,$2,$3,.../;

- ticker interval
- ticker off

- use RE find/search for matches instead of line bound interface?
- pcre2 weird failure - better handling really

- DONE - add distribution to -u option where you have topN and bottomN displayed horizontally
  (v1 x count1)(v2 x count2)... MIDCOUNT ...(vN-1 x countN-1)(vN-1 x countN-1)
-- DONE: Read from compressed files automatically - see grep-cli
- DONE - made 4x improvement on typical captures - pcre2 usage?  - ripgrep uses it - why?
- Done - Optimize allocations in store_rec - avoid that ss.clone()
- Done - Multithread io-swizzle to read more than one file at a time - good for many small files over nfs maybe
- irrelevenat due to pcre2 - bstr mode for re?  does it help - who cares?

