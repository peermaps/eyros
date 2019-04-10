#![feature(duration_float)]
extern crate eyros;
extern crate failure;
extern crate rand;
extern crate random_access_disk;

use eyros::{Setup,Row};
use rand::random;
use failure::Error;
use random_access_disk::RandomAccessDisk;
use std::path::PathBuf;
use std::time;

fn main() -> Result<(),Error> {
  let mut db = Setup::new(storage)
    .branch_factor(9)
    .max_data_size(1_000)
    .base_size(9_000)
    .build()?;
  let size = 10_000;
  let mut count = 0_u64;
  let mut total = 0f64;
  for _ in 0..10_000 {
    let rows = (0..size).map(|_| {
      let xmin: f32 = random::<f32>()*2.0-1.0;
      let xmax: f32 = xmin + random::<f32>().powf(64.0)*(1.0-xmin);
      let ymin: f32 = random::<f32>()*2.0-1.0;
      let ymax: f32 = ymin + random::<f32>().powf(64.0)*(1.0-ymin);
      let value: u32 = random();
      let point = ((xmin,xmax),(ymin,ymax));
      Row::Insert(point, value)
    }).collect();
    let batch_start = time::Instant::now();
    db.batch(&rows)?;
    let batch_elapsed = batch_start.elapsed().as_float_secs();
    count += size;
    total += batch_elapsed;
    println!["{}: batch for {} records in {} seconds",
      count, size, batch_elapsed];
  }
  println!["# wrote {} records in {} seconds\n# {} records / second",
    count, total, (count as f64) / total];
  Ok(())
}

fn storage(name:&str) -> Result<RandomAccessDisk,Error> {
  let mut p = PathBuf::from("/tmp/eyros-db/");
  p.push(name);
  Ok(RandomAccessDisk::open(p)?)
}
