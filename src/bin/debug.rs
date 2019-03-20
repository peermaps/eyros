#![feature(int_to_from_bytes)]
extern crate eyros;
extern crate failure;
extern crate random_access_disk;
extern crate random_access_storage;

use eyros::{DB,Row};
use failure::{Error,bail};
use random_access_disk::RandomAccessDisk;
use random_access_storage::RandomAccess;
use std::path::PathBuf;
use std::env;
use std::mem::size_of;

#[path="../read_block.rs"]
mod read_block;
use read_block::read_block;
use eyros::Point;

type P = ((f32,f32),(f32,f32),f32);
type V = u32;

fn main() -> Result<(),Error> {
  let args: Vec<String> = env::args().collect();
  if args.len() < 3 {
    bail!["usage: debug DBPATH COMMAND {...}"];
  }
  let mut db: DB<_,_,P,V> = DB::open(|name| {
    let mut p = PathBuf::from(&args[1]);
    p.push(name);
    Ok(RandomAccessDisk::open(p)?)
  })?;
  if args[2] == "info" {
    let mut dstore = db.data_store.try_borrow_mut()?;
    println!["# data\n{} bytes", dstore.bytes()?];
    println!["# staging\n{} bytes\n{} records",
      db.staging.bytes()?, db.staging.len()?];
    println!["# trees"];
    for (i,tree) in db.trees.iter().enumerate() {
      if tree.bytes == 0 {
        println!["[{}] empty", i];
      } else {
        println!["[{}] {} bytes", i, tree.bytes];
      }
    }
  } else if args[2] == "branch" {
    let i = args[3].parse::<usize>()?;
    let j = args[4].parse::<u64>()?;
    let depth = args[5].parse::<usize>()?;
    let len = db.trees[i].store.len()? as u64;
    let buf = read_block(&mut db.trees[i].store, j, len, 1024)?;
    let bf = 9;
    let n = bf*2-3;

    let psize = P::pivot_size_at(depth % P::dim());
    let p_start = 0;
    let d_start = p_start + n*psize;
    let i_start = d_start + (n+bf+7)/8;
    let b_start = i_start + n*size_of::<u64>();
    let b_end = b_start+bf*size_of::<u64>();
    assert_eq!(b_end, buf.len(), "unexpected block length");

    let mut fpivots = vec![];
    for i in 0..n {
      let pbuf = &buf[p_start+i*psize..p_start+(i+1)*psize];
      fpivots.push(P::format_at(pbuf, depth)?);
    }
    let intersecting: Vec<(bool,u64)> = (0..n).map(|i| {
      let is_data = ((buf[d_start+i/8]>>(i%8))&1) == 1;
      let i_offset = i_start + i*8;
      let offset = u64::from_be_bytes([
        buf[i_offset+0], buf[i_offset+1],
        buf[i_offset+2], buf[i_offset+3],
        buf[i_offset+4], buf[i_offset+5],
        buf[i_offset+6], buf[i_offset+7],
      ]);
      (is_data,offset)
    }).collect();
    let buckets: Vec<(bool,u64)> = (0..bf).map(|i| {
      let j = i+n;
      let is_data = ((buf[d_start+j/8]>>(j%8))&1) == 1;
      let b_offset = b_start + i*8;
      let offset = u64::from_be_bytes([
        buf[b_offset+0], buf[b_offset+1],
        buf[b_offset+2], buf[b_offset+3],
        buf[b_offset+4], buf[b_offset+5],
        buf[b_offset+6], buf[b_offset+7],
      ]);
      (is_data,offset)
    }).collect();
    println!("# pivots");
    for (i,p) in fpivots.iter().enumerate() {
      println!("[{}] {}", i, p);
    }
    println!["# intersecting"];
    for (i,(is_data,offset)) in intersecting.iter().enumerate() {
      if *offset == 0 {
        println!["[{}] NULL", i];
      } else {
        println!("[{}] {} {}",
          i, offset-1,
          if *is_data { "[DATA]" } else { "" }
        );
      }
    }
    println!["# buckets"];
    for (i,(is_data,offset)) in buckets.iter().enumerate() {
      if *offset == 0 {
        println!["[{}] NULL", i];
      } else {
        println!("[{}] {} {}",
          i, offset-1,
          if *is_data { "[DATA]" } else { "" }
        );
      }
    }
  } else if args[2] == "data" {
    let i = args[3].parse::<u64>()?;
    let mut dstore = db.data_store.try_borrow_mut()?;
    let points = dstore.list(i)?;
    for p in points {
      println!["{:?}", p];
    }
  } else if args[2] == "staging-data" {
    for p in db.staging.rows {
      match p {
        Row::Insert(p,v) => println!["{:?}", (p,v)],
        Row::Delete(p,v) => println!["{:?} [DELETE]", (p,v)],
      }
    }
  } else {
    bail!["COMMAND not recognized"];
  }
  Ok(())
}
