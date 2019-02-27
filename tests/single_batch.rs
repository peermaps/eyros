extern crate eyros;
extern crate failure;
extern crate random;
extern crate random_access_disk;
extern crate tempfile;

use eyros::{DB,Row};
use failure::Error;
use random_access_disk::RandomAccessDisk;
use random::{Source,default as rand};
use tempfile::Builder as Tmpfile;

use std::path::PathBuf;
use std::rc::Rc;
use std::cell::RefCell;

#[test]
fn single_batch() -> Result<(),Error> {
  let dir = Tmpfile::new().prefix("eyros").tempdir()?;
  let mut db: DB<_,_,((f32,f32),(f32,f32),f32),u32> = DB::open(
    |name: &str| -> Result<RandomAccessDisk,Error> {
      let p = dir.path().join(name);
      Ok(RandomAccessDisk::open(p)?)
    }
  )?;
  let mut r = rand().seed([13,12]);
  let size = 800;
  let polygons = (0..size).map(|_| {
    let xmin: f32 = r.read::<f32>()*2.0-1.0;
    let xmax: f32 = xmin + r.read::<f32>().powf(64.0)*(1.0-xmin);
    let ymin: f32 = r.read::<f32>()*2.0-1.0;
    let ymax: f32 = ymin + r.read::<f32>().powf(64.0)*(1.0-ymin);
    let time: f32 = r.read::<f32>()*1000.0;
    let value: u32 = r.read();
    let point = ((xmin,xmax),(ymin,ymax),time);
    Row::Insert(point, value)
  }).collect();
  db.batch(&polygons)?;

  let bbox = ((-1.0,-1.0,0.0),(1.0,1.0,1000.0));
  let mut results = vec![];
  for result in db.query(&bbox)? {
    results.push(result?);
  }
  assert_eq!(results.len(), size);
  Ok(())
}
