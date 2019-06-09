#![feature(duration_float)]
extern crate eyros;
extern crate failure;
extern crate random;
extern crate random_access_disk;
extern crate tempfile;

use eyros::{DB,Row,Setup};
use failure::Error;
use random_access_disk::RandomAccessDisk;
use random::{Source,default as rand};
use tempfile::Builder as Tmpfile;
use std::time;

use std::cmp::Ordering;

type P = ((f32,f32),(f32,f32),f32);
type V = u32;

#[test]
fn multi_batch() -> Result<(),Error> {
  let dir = Tmpfile::new().prefix("eyros").tempdir()?;
  let storage = |name: &str| -> Result<RandomAccessDisk,Error> {
    let p = dir.path().join(name);
    Ok(RandomAccessDisk::builder(p)
      .auto_sync(false)
      .build()?)
  };
  let mut db: DB<_,_,P,V> = Setup::new(storage)
    .branch_factor(5)
    .max_data_size(3_000)
    .base_size(1_000)
    .build()?;
  let mut r = rand().seed([13,12]);
  let size = 1_000_000;
  let inserts: Vec<Row<P,V>> = (0..size).map(|_| {
    let xmin: f32 = r.read::<f32>()*2.0-1.0;
    let xmax: f32 = xmin + r.read::<f32>().powf(64.0)*(1.0-xmin);
    let ymin: f32 = r.read::<f32>()*2.0-1.0;
    let ymax: f32 = ymin + r.read::<f32>().powf(64.0)*(1.0-ymin);
    let time: f32 = r.read::<f32>()*1000.0;
    let value: u32 = r.read();
    let point = ((xmin,xmax),(ymin,ymax),time);
    Row::Insert(point, value)
  }).collect();
  let mut count = 0u64;
  let mut total = 0f64;
  let n = 100;
  let batches: Vec<Vec<Row<P,V>>> = (0..n).map(|i| {
    inserts[size/n*i..size/n*(i+1)].to_vec()
  }).collect();
  for batch in batches {
    let batch_start = time::Instant::now();
    db.batch(&batch)?;
    let batch_elapsed = batch_start.elapsed().as_secs_f64();
    count += batch.len() as u64;
    total += batch_elapsed;
    eprintln!["{}: batch for {} records in {} seconds",
      count, batch.len(), batch_elapsed];
  }
  eprintln!["# wrote {} records in {} seconds\n# {} records / second",
    count, total, (count as f64) / total];
  {
    let bbox = ((-1.0,-1.0,0.0),(1.0,1.0,1000.0));
    let mut results = vec![];
    for result in db.query(&bbox)? {
      results.push(result?);
    }
    assert_eq!(results.len(), size, "incorrect length for full region");
    let mut expected: Vec<(P,V)>
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
    for result in db.query(&bbox)? {
      results.push(result?);
    }
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
