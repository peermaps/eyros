#![feature(duration_float)]

extern crate eyros;
extern crate failure;
extern crate random;
extern crate random_access_disk;
extern crate tempfile;

use eyros::{Setup,Row};
use failure::Error;
use random_access_disk::RandomAccessDisk;
use random::{Source,default as rand};
use tempfile::Builder as Tmpfile;
use std::time;

use std::cmp::Ordering;

#[test]
fn single_4k_batch_xxyyz_9_bf_10_data_16_base() -> Result<(),Error> {
  from_params(4000, 9, 10, 16)
}

#[test]
fn single_4k_batch_xxyyz_5_bf_10_data_16_base() -> Result<(),Error> {
  from_params(4000, 5, 10, 16)
}

#[test]
fn single_4k_batch_xxyyz_17_bf_10_data_16_base() -> Result<(),Error> {
  from_params(4000, 17, 10, 16)
}

#[test]
fn single_32k_batch_xxyyz_9_bf_400_data_1k_base() -> Result<(),Error> {
  from_params(32_000, 9, 400, 1000)
}

#[test]
fn single_200k_batch_xxyyz_9_bf_400_data_1k_base() -> Result<(),Error> {
  from_params(200_000, 9, 400, 1000)
}

fn from_params (size: usize, bf: usize, max_data_size: usize,
base_size: usize) -> Result<(),Error> {
  let dir = Tmpfile::new().prefix("eyros").tempdir()?;
  let open = |name: &str| -> Result<RandomAccessDisk,Error> {
    let p = dir.path().join(name);
    Ok(RandomAccessDisk::open(p)?)
  };
  let mut db = Setup::new(open)
    .branch_factor(bf)
    .max_data_size(max_data_size)
    .base_size(base_size)
    .build()?;
  let mut r = rand().seed([13,12]);
  let inserts = (0..size).map(|_| {
    let xmin: f32 = r.read::<f32>()*2.0-1.0;
    let xmax: f32 = xmin + r.read::<f32>().powf(64.0)*(1.0-xmin);
    let ymin: f32 = r.read::<f32>()*2.0-1.0;
    let ymax: f32 = ymin + r.read::<f32>().powf(64.0)*(1.0-ymin);
    let time: f32 = r.read::<f32>()*1000.0;
    let value: u32 = r.read();
    let point = ((xmin,xmax),(ymin,ymax),time);
    Row::Insert(point, value)
  }).collect();
  {
    let start = time::Instant::now();
    db.batch(&inserts)?;
    eprintln!["batch write for {} records in {} seconds",
      size, start.elapsed().as_float_secs()];
  }

  {
    let bbox = ((-1.0,-1.0,0.0),(1.0,1.0,1000.0));
    let mut results = vec![];
    let start = time::Instant::now();
    for result in db.query(&bbox)? {
      results.push(result?);
    }
    eprintln!["batch query for {} records in {} seconds",
      results.len(), start.elapsed().as_float_secs()];
    assert_eq!(results.len(), size, "incorrect length for full region");
    let mut expected: Vec<(((f32,f32),(f32,f32),f32),u32)>
    = inserts.iter().map(|r| {
      match r {
        Row::Insert(point,value) => (*point,*value),
        _ => panic!["unexpected row type"]
      }
    }).collect();
    results.sort_unstable_by(cmp);
    expected.sort_unstable_by(cmp);
    assert_eq!(results, expected, "incorrect results for full region");
  }

  {
    let bbox = ((-0.8,0.1,0.0),(0.2,0.5,500.0));
    let mut results = vec![];
    let start = time::Instant::now();
    for result in db.query(&bbox)? {
      results.push(result?);
    }
    eprintln!["batch query for {} records in {} seconds",
      results.len(), start.elapsed().as_float_secs()];
    let mut expected: Vec<(((f32,f32),(f32,f32),f32),u32)>
    = inserts.iter()
      .map(|r| {
        match r {
          Row::Insert(point,value) => (*point,*value),
          _ => panic!["unexpected row type"]
        }
      })
      .filter(|r| {
        contains_iv((bbox.0).0,(bbox.1).0, (r.0).0)
        && contains_iv((bbox.0).1,(bbox.1).1, (r.0).1)
        && contains_pt((bbox.0).2,(bbox.1).2, (r.0).2)
      })
      .collect();
    results.sort_unstable_by(cmp);
    expected.sort_unstable_by(cmp);
    assert_eq!(results.len(), expected.len(),
      "incorrect length for partial region");
    assert_eq!(results, expected, "incorrect results for partial region");
  }
  Ok(())
}

fn cmp<T> (a: &T, b: &T) -> Ordering where T: PartialOrd {
  match a.partial_cmp(b) {
    Some(o) => o,
    None => panic!["comparison failed"]
  }
}

fn contains_iv<T> (min: T, max: T, iv: (T,T)) -> bool where T: PartialOrd {
  min <= iv.1 && iv.0 <= max
}
fn contains_pt<T> (min: T, max: T, pt: T) -> bool where T: PartialOrd {
  min <= pt && pt <= max
}
