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
  let args: Vec<String> = std::env::args().collect();
  let base = PathBuf::from(args[1].clone());
  let mut db = Setup::new(|name| {
    let mut p = base.clone();
    p.push(name);
    Ok(RandomAccessDisk::open(p)?)
  })
    .branch_factor(5)
    .max_data_size(3_000)
    .base_size(1_000)
    .build()?;
  let batch_size = 10_000;
  let mut count = 0_u64;
  let mut total = 0f64;
  for _ in 0..100 {
    let rows = (0..batch_size).map(|_| {
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
    let batch_elapsed = batch_start.elapsed().as_secs_f64();
    count += batch_size;
    total += batch_elapsed;
    println!["{}: batch for {} records in {} seconds",
      count, batch_size, batch_elapsed];
  }
  println!["# wrote {} records in {} seconds\n# {} records / second",
    count, total, (count as f64) / total];
  Ok(())
}
