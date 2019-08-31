# csv - cheesey select-group-by on delimited files.

* Known bug - it does not report empty strings properly.

csv is a command that takes a character delimited file [or stdin] and does a kind of 
SQL select-group-by on delimited data as a table.

Alternatively, you can use a regular expression (see -re option) against the lines where the groups captured become fields.

I was always writing a "quick" perl one-liner to do this kind of thing, so I wrote this utility to simplify and speed things up.

The delimited mode is super fast and memory efficient thanks to [rust csv library](https://github.com/BurntSushi/rust-csv) 
and rust collections in general.
The regular expression mode is not as fast per core as the perl equivalent but is now threaded to compensate.  

## How-To:

```bash
csv -d '|' -f 0,3 -i somefile.csv
```

This parses somefile.csv as a pipe delimited file and does a ```select f1,f4,count(*) from X group by f1,f4;```

```bash
cat somefile.csv | csv -f 0,3
```
This does the same thing but from standard input.

```bash
csv -f 0,1 -a
```

```-a``` causes delimited format to be written instead of the default table auto-aligned format.

TODO:  

- Read from compressed files automatically - see grep-cli
- Use stdin as a filelist source in addition to stdin as a data source
- Multithread io-swizzle to read more than one file at a time - good for many small files over nfs maybe
- additional aggregate functions?:  avg, min, max, empty_count, number_count, zero_count
- do more work to multi-line re mode - not sure how it should really work yet
- bstr mode for re?  does it help - who cares?
- pcre2 usage?  - ripgrep uses it - why?


