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

use std::cmp::Ordering;

#[test]
fn multi_batch_f32_f64_u16() -> Result<(),Error> {
  type P = (f32,f64);
  type V = u16;
  let dir = Tmpfile::new().prefix("eyros").tempdir()?;
  let mut db: DB<_,_,P,V> = DB::open(
    |name: &str| -> Result<RandomAccessDisk,Error> {
      let p = dir.path().join(name);
      Ok(RandomAccessDisk::open(p)?)
    }
  )?;
  let mut r = rand().seed([13,12]);
  let size = 10_000;
  let inserts: Vec<Row<P,V>> = (0..size).map(|_| {
    let x: f32 = (r.read::<f32>()*2.0-1.0)*1000.0;
    let y: f64 = r.read::<f64>()*4000.0+2000.0;
    let value: u16 = r.read();
    Row::Insert((x,y), value)
  }).collect();
  let n = 5;
  let batches: Vec<Vec<Row<P,V>>> = (0..n).map(|i| {
    inserts[size/n*i..size/n*(i+1)].to_vec()
  }).collect();
  for batch in batches {
    db.batch(&batch)?;
  }

  {
    let bbox = ((-1000.0,2000.0),(1000.0,6000.0));
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
    let bbox = ((-800.0,1000.0),(-400.0,4000.0));
    let mut results = vec![];
    for result in db.query(&bbox)? {
      results.push(result?);
    }
    let mut expected: Vec<(P,V)> = inserts.iter()
      .map(|r| {
        match r {
          Row::Insert(point,value) => (*point,*value),
          _ => panic!["unexpected row type"]
        }
      })
      .filter(|r| {
        contains_pt((bbox.0).0,(bbox.1).0, (r.0).0)
        && contains_pt((bbox.0).1,(bbox.1).1, (r.0).1)
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

#[test]
fn multi_batch_f64iv_f32_u16() -> Result<(),Error> {
  type P = ((f64,f64),f32);
  type V = u16;
  let dir = Tmpfile::new().prefix("eyros").tempdir()?;
  let mut db: DB<_,_,P,V> = DB::open(
    |name: &str| -> Result<RandomAccessDisk,Error> {
      let p = dir.path().join(name);
      Ok(RandomAccessDisk::open(p)?)
    }
  )?;
  let mut r = rand().seed([13,12]);
  let size = 10_000;
  let inserts: Vec<Row<P,V>> = (0..size).map(|_| {
    let x0: f64 = (r.read::<f64>()*2.0-1.0)*1000.0;
    let x1: f64 = x0 + ((r.read::<f64>().powf(64.0))*2.0-1.0)*500.0;
    let y: f32 = r.read::<f32>()*4000.0+2000.0;
    let value: u16 = r.read();
    Row::Insert(((x0,x1),y), value)
  }).collect();
  let n = 5;
  let batches: Vec<Vec<Row<P,V>>> = (0..n).map(|i| {
    inserts[size/n*i..size/n*(i+1)].to_vec()
  }).collect();
  for batch in batches {
    db.batch(&batch)?;
  }

  {
    let bbox = ((-1500.0,2000.0),(1500.0,6000.0));
    let mut results = vec![];
    for result in db.query(&bbox)? {
      results.push(result?);
    }
    assert_eq!(results.len(), size, "incorrect length for full region");
    let mut expected: Vec<(P,V)> = inserts.iter().map(|r| {
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
    let bbox = ((-800.0,1000.0),(-400.0,4000.0));
    let mut results = vec![];
    for result in db.query(&bbox)? {
      results.push(result?);
    }
    let mut expected: Vec<(P,V)> = inserts.iter()
      .map(|r| {
        match r {
          Row::Insert(point,value) => (*point,*value),
          _ => panic!["unexpected row type"]
        }
      })
      .filter(|r| {
        contains_iv((bbox.0).0,(bbox.1).0, (r.0).0)
        && contains_pt((bbox.0).1,(bbox.1).1, (r.0).1)
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
