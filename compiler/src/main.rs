extern crate getopts;
extern crate byteorder;
extern crate crossbeam;

use std::{io, env, process};
use std::io::{Read, Write, Seek, SeekFrom, BufReader, Cursor};
use std::fs::{File, OpenOptions};
use std::num::ParseIntError;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use getopts::{Options, Matches};
use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};

#[derive(Debug)]
enum CmdArgsError {
    Getopts(getopts::Fail),
    NoInDbFileProvided,
    NoOutDbFileProvided,
    NoCalcCacheFileProvided,
    InvalidDivStartValue(String, ParseIntError),
    InvalidThreadsValue(String, ParseIntError),
}

#[derive(Debug)]
enum Error {
    CmdArgs(CmdArgsError),
    InDbOpen(io::Error),
    InDbMeta(io::Error),
    InDbSeek(io::Error),
    InDbRead(io::Error),
    OutDbCreate(io::Error),
    OutDbWrite(io::Error),
    CacheOpen(io::Error),
    CacheRead(io::Error),
    CacheWrite(io::Error),
}

fn entrypoint(maybe_matches: getopts::Result) -> Result<(), Error> {
    let matches = try!(maybe_matches.map_err(|e| Error::CmdArgs(CmdArgsError::Getopts(e))));
    run(matches)
}

fn run(matches: Matches) -> Result<(), Error> {
    let in_db_filename = try!(matches.opt_str("db-in").ok_or(Error::CmdArgs(CmdArgsError::NoInDbFileProvided)));
    let out_db_filename = try!(matches.opt_str("db-out").ok_or(Error::CmdArgs(CmdArgsError::NoOutDbFileProvided)));
    let calc_cache_filename = try!(matches.opt_str("calc-cache").ok_or(Error::CmdArgs(CmdArgsError::NoCalcCacheFileProvided)));
    let threads_count: usize = {
        let threads_str = matches.opt_str("threads").unwrap_or("4".to_string());
        try!(threads_str.parse().map_err(|e| Error::CmdArgs(CmdArgsError::InvalidThreadsValue(threads_str, e))))
    };
    let div_start: usize = {
        let div_start_str = matches.opt_str("div-start").unwrap_or("1".to_string());
        try!(div_start_str.parse().map_err(|e| Error::CmdArgs(CmdArgsError::InvalidDivStartValue(div_start_str, e))))
    };

    let in_db = try!(File::open(&in_db_filename).map_err(Error::InDbOpen));
    let metadata = try!(in_db.metadata().map_err(Error::InDbMeta));
    let in_db_size = metadata.len();
    let words_count = (in_db_size / 45000 / 4) as usize;
    let mut in_db_r = BufReader::new(in_db);

    let mut cache = try!(OpenOptions::new().read(true).write(true).create(true).open(&calc_cache_filename).map_err(Error::CacheOpen));
    
    println!("Running: in_db_filename = {} (size = {}, words = {}), out_db_filename = {}, calc_cache_filename = {}, threads_count = {}, div_start = {}",
             in_db_filename, in_db_size, words_count, out_db_filename, calc_cache_filename, threads_count, div_start);

    let mut divs_found = Vec::new();
    let mut min_div = std::i32::MAX;
    let mut max_div = -1;

    let chunk_size = 8000;
    let chunk_limit = 32000 / chunk_size;

    let mut read_buf: Vec<u8> = (0 .. 45000 * 4).map(|_| 0).collect();

    for chunk_index in 0 .. chunk_limit {
        println!(" ;; READ chunk_index = {}/{} by {}, current min = {}, max = {}, divs_found = {}",
                 chunk_index, chunk_limit, chunk_size, min_div, max_div, divs_found.len());

        let mut seed_sample = Vec::with_capacity(chunk_size * words_count);

        try!(in_db_r.seek(SeekFrom::Start(0u64)).map_err(Error::InDbSeek));
        for _ in 0 .. words_count {
            try!(in_db_r.read_exact(&mut read_buf).map_err(Error::InDbRead));
            let mut curr = Cursor::new(&read_buf);
            let offset = chunk_index as u64 * chunk_size as u64;
            try!(curr.seek(SeekFrom::Start(offset * 4)).map_err(Error::InDbSeek));
            for _chunk in 0 .. chunk_size {
                let hash = try!(curr.read_i32::<NativeEndian>().map_err(Error::InDbRead));
                seed_sample.push(hash);
            }
        }

        for chunk in 0 .. chunk_size {

            if chunk % 50 == 0 {
                println!(" ;; RUN chunk_index = {}/{} by {}, chunk N{}, current min = {}, max = {}, divs_found = {}",
                         chunk_index, chunk_limit, chunk_size, chunk, min_div, max_div, divs_found.len());
            }

            match cache.read_i32::<NativeEndian>() {
                Ok(cached_div) => {
                    if cached_div > 0 && cached_div < min_div {
                        min_div = cached_div;
                    }
                    if cached_div > 0 && cached_div > max_div {
                        max_div = cached_div;
                    }
                    divs_found.push(cached_div);
                    continue;
                },
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof =>
                    (),
                Err(e) =>
                    return Err(Error::CacheRead(e)),
            }

            let mut zero_found = false;
            for i in 0 .. words_count {
                let hash = seed_sample[i * chunk_size + chunk];
                if hash == 0 {
                    zero_found = true;
                    break;
                }
            }

            if zero_found {
                divs_found.push(0);
                try!(cache.write_i32::<NativeEndian>(0).map_err(Error::CacheWrite));
                try!(cache.flush().map_err(Error::CacheWrite));
                continue;
            }

            let div = AtomicIsize::new(div_start as isize);
            let pass = AtomicBool::new(false);
            let rdiv = AtomicIsize::new(0);

            crossbeam::scope(|scope| {
                for _ in 0 .. threads_count {
                    scope.spawn(|| {
                        while !pass.load(Ordering::Relaxed) {
                            let current_div = div.fetch_add(1, Ordering::Relaxed) as i32;
                            // if current_div >= std::u16::MAX as i32 * 2 {
                            //     pass.store(true, Ordering::SeqCst);
                            //     rdiv.store(max_div as isize, Ordering::SeqCst);
                            //     break;
                            // }
                            // if current_div % 1000 == 0 {
                            //     println!(" ;; currently trying div = {}", current_div);
                            // }

                            let mut found = false;
                            for i in 0 .. words_count {
                                let hash = seed_sample[i * chunk_size + chunk];
                                if hash % current_div == 0 {
                                    found = true;
                                    break;
                                }
                            }

                            if !found {
                                pass.store(true, Ordering::SeqCst);
                                rdiv.store(current_div as isize, Ordering::SeqCst);
                            }
                        }
                    });
                }
            });

            let result_div = rdiv.load(Ordering::Relaxed) as i32;
            if result_div < min_div {
                min_div = result_div;
            }
            if result_div > max_div {
                max_div = result_div;
            }
            divs_found.push(result_div);
            try!(cache.write_i32::<NativeEndian>(result_div).map_err(Error::CacheWrite));
            try!(cache.flush().map_err(Error::CacheWrite));
        }
    }

    println!("OVERALL base div = {}", min_div);

    let mut out_db = try!(File::create(out_db_filename).map_err(Error::OutDbCreate));
    for div in divs_found {
        let value = if div == 0 {
            0
        } else {
            div - min_div + 1
        };
        assert!(value <= std::u16::MAX as i32);
        try!(out_db.write_u16::<NativeEndian>(value as u16).map_err(Error::OutDbWrite));
    }

    Ok(())
}

fn main() {
    let mut args = env::args();
    let cmd_proc = args.next().unwrap();
    let mut opts = Options::new();

    opts.optopt("i", "db-in", "in file for input binary data db", "INDB");
    opts.optopt("o", "db-out", "output file for out binary data db", "OUTDB");
    opts.optopt("c", "calc-cache", "cache file used during calculations", "CACHE");
    opts.optopt("t", "threads", "total concurrent threads to use (opt, default: 4)", "THREADS");
    opts.optopt("d", "div-start", "div start value (opt, default: 1)", "DIVSTART");
    match entrypoint(opts.parse(args)) {
        Ok(()) => (),
        Err(cause) => {
            let _ = writeln!(&mut io::stderr(), "Error: {:?}", cause);
            let usage = format!("Usage: {}", cmd_proc);
            let _ = writeln!(&mut io::stderr(), "{}", opts.usage(&usage[..]));
            process::exit(1);
        }
    }
}
