extern crate eyros;
extern crate failure;
extern crate rand;
extern crate random_access_disk;

use eyros::{DB,Point};
use rand::random;
use failure::Error;
use random_access_disk::RandomAccessDisk;
use std::path::PathBuf;

fn main() -> Result<(),Error> {
  let mut db = DB::open(storage)?;
  let polygons = (0..8_u32.pow(4)).map(|_| {
  //let polygons = (0..8_u32.pow(1)).map(|_| {
    let xmin: f32 = random::<f32>()*2.0-1.0;
    let xmax: f32 = xmin + random::<f32>().powf(16.0)*(1.0-xmin);
    let ymin: f32 = random::<f32>()*2.0-1.0;
    let ymax: f32 = ymin + random::<f32>().powf(16.0)*(1.0-ymin);
    //let time: f32 = random::<f32>()*1000.0;
    let value: u32 = random();
    //let point = (Range(xmin,xmax),Range(ymin,ymax),Point(time));
    let point = ((xmin,xmax),(ymin,ymax));
    (point, value)
  }).collect();
  db.batch(&polygons)?;
  Ok(())
}

fn storage(name:&str) -> Result<RandomAccessDisk,Error> {
  let mut p = PathBuf::from("/tmp/eyros-db/");
  p.push(name);
  Ok(RandomAccessDisk::open(p)?)
}
