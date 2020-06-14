#![feature(async_closure)]
extern crate eyros;
extern crate failure;
extern crate random_access_disk;
extern crate random_access_storage;
use desert::CountBytes;

#[path="../ensure.rs"]
#[macro_use] mod ensure;

use eyros::{Setup,DB};
use failure::{Error,bail};
use random_access_disk::RandomAccessDisk;
use random_access_storage::RandomAccess;
use std::path::PathBuf;
use std::env;
use std::mem::size_of;
use std::time;
use async_std::{prelude::*,future::Future};

#[path="../read_block.rs"]
mod read_block;
use read_block::read_block;
use eyros::Point;

type P = ((f32,f32),(f32,f32));
type V = u32;

#[async_std::main]
async fn main() -> Result<(),Error> {
  let args: Vec<String> = env::args().collect();
  if args.len() < 3 {
    bail!["usage: debug DBPATH COMMAND {...}"];
  }
  let mut db = Setup::new(Box::new(async move |name| {
    let mut p = PathBuf::from(&args[1]);
    p.push(name);
    Ok(RandomAccessDisk::open(p).await?)
  }))
    .branch_factor(5_usize)
    .max_data_size(3_000_usize)
    .base_size(1_000_usize)
    .build().await?;
  if args[2] == "info" {
    let mut dstore = db.data_store.try_borrow_mut()?;
    println!["# data\n{} bytes", dstore.bytes()?];
    println!["# staging\n{} bytes\n{} records",
      db.staging.bytes()?, db.staging.len()?];
    println!["# trees"];
    for (i,tree) in db.trees.iter().enumerate() {
      let bytes = tree.try_borrow()?.bytes;
      if bytes == 0 {
        println!["[{}] empty", i];
      } else {
        println!["[{}] {} bytes", i, bytes];
      }
    }
  } else if args[2] == "branch" {
    let i = args[3].parse::<usize>()?;
    let j = args[4].parse::<u64>()?;
    let depth = args[5].parse::<usize>()?;
    let b = read_branch(&mut db, i, j, depth).await?;
    println!("# pivots");
    for (i,p) in b.str_pivots.iter().enumerate() {
      println!("[{}] {}", i, p);
    }
    println!["# intersecting"];
    for (i,(is_data,offset)) in b.intersecting.iter().enumerate() {
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
    for (i,(is_data,offset)) in b.buckets.iter().enumerate() {
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
    for pv in db.staging.inserts.try_borrow()?.iter() {
      println!["{:?}", pv];
    }
    for loc in db.staging.deletes.try_borrow()?.iter() {
      println!["{:?} [DELETE]", loc];
    }
  } else if args[2] == "time-query" {
    let bbox = (
      (args[3].parse::<f32>()?,args[4].parse::<f32>()?),
      (args[5].parse::<f32>()?,args[6].parse::<f32>()?)
    );
    let mut results = vec![];
    let start = time::Instant::now();
    let stream = db.query(&bbox).await?;
    while let Some(result) = stream.next().await {
      results.push(result?);
    }
    let elapsed = start.elapsed().as_secs_f64();
    println!["{} results in {} seconds", results.len(), elapsed];
  } else if args[2] == "branches" {
    let i = args[3].parse::<usize>()?;
    let mut queue = vec![(0,0)];
    while !queue.is_empty() {
      let (offset,depth) = queue.pop().unwrap();
      let b = read_branch(&mut db, i, offset, depth).await?;
      for (is_data,i_offset) in b.intersecting {
        if !is_data && i_offset > 0 {
          queue.push((i_offset-1,depth+1))
        }
      }
      for (is_data,b_offset) in b.buckets {
        if !is_data && b_offset > 0 {
          queue.push((b_offset-1,depth+1))
        }
      }
    }
  } else {
    bail!["COMMAND not recognized"];
  }
  Ok(())
}

pub struct Branch {
  pub str_pivots: Vec<String>,
  pub intersecting: Vec<(bool,u64)>,
  pub buckets: Vec<(bool,u64)>
}

async fn read_branch<S,U> (db: &mut DB<S,U,P,V>, tree_i: usize,
offset: u64, depth: usize) -> Result<Branch,Error>
where S: RandomAccess<Error=Error>+Send+Sync,
U: (Fn(&str) -> Box<dyn Future<Output=Result<S,Error>>+Unpin>) {
  let len = db.trees[tree_i].lock().await.store.len().await? as u64;
  let buf = read_block(
    &mut db.trees[tree_i].lock().await.store, offset, len, 1024
  ).await?;
  let bf = db.fields.branch_factor;
  let n = bf*2-3;

  let mut offset = 0;
  let mut str_pivots = vec![];
  for _i in 0..n {
    let size = P::count_from_bytes(&buf[offset..])?;
    str_pivots.push(P::format_at(
      &buf[offset..offset+size],
      depth
    )?);
    offset += size;
  }
  let d_start = offset;
  let i_start = d_start + (n+bf+7)/8;
  let b_start = i_start + n*size_of::<u64>();
  let b_end = b_start+bf*size_of::<u64>();
  assert_eq!(b_end, buf.len(), "unexpected block length");

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
  Ok(Branch { str_pivots, buckets, intersecting })
}
