extern crate eyros;
extern crate failure;
extern crate random;
extern crate random_access_disk;
extern crate random_access_storage;
extern crate tempfile;

use eyros::{DB,Row};
use failure::Error;
use random_access_disk::RandomAccessDisk;
use random_access_storage::RandomAccess;
use random::{Source,default as rand};
use tempfile::Builder as Tmpfile;

use std::cmp::Ordering;

type P = ((f32,f32),(f64,f64),f32);
type V = u32;

#[test]
fn load() -> Result<(),Error> {
  let dir = Tmpfile::new().prefix("eyros").tempdir()?;
  let size = 4000;
  let mut r = rand().seed([13,12]);
  let mut inserts: Vec<Row<P,V>> = (0..size).map(|_| {
    let xmin: f32 = r.read::<f32>()*2.0-1.0;
    let xmax: f32 = xmin + r.read::<f32>().powf(64.0)*(1.0-xmin);
    let ymin: f64 = r.read::<f64>()*2.0-1.0;
    let ymax: f64 = ymin + r.read::<f64>().powf(64.0)*(1.0-ymin);
    let time: f32 = r.read::<f32>()*1000.0;
    let value: u32 = r.read();
    let point = ((xmin,xmax),(ymin,ymax),time);
    Row::Insert(point, value)
  }).collect();
  {
    // seed the db
    let mut db: DB<_,_,P,V> = DB::open(
      |name: &str| -> Result<RandomAccessDisk,Error> {
        let p = dir.path().join(name);
        Ok(RandomAccessDisk::builder(p)
          .auto_sync(false)
          .build()?)
      }
    )?;
    let n = 4;
    let batches: Vec<Vec<Row<P,V>>> = (0..n).map(|i| {
      inserts[size/n*i..size/n*(i+1)].to_vec()
    }).collect();
    for batch in batches {
      db.batch(&batch)?;
    }
  }
  {
    // let the previous db fall out of scope and create a new one
    // so it loads records from zero
    let mut db: DB<_,_,P,V> = DB::open(
      |name: &str| -> Result<RandomAccessDisk,Error> {
        let p = dir.path().join(name);
        Ok(RandomAccessDisk::builder(p)
          .auto_sync(false)
          .build()?)
      }
    )?;
    check(&mut db, &inserts, size)?;
  }
  // create more records
  let newsize = 2000;
  for _ in 0..newsize {
    let xmin: f32 = r.read::<f32>()*2.0-1.0;
    let xmax: f32 = xmin + r.read::<f32>().powf(64.0)*(1.0-xmin);
    let ymin: f64 = r.read::<f64>()*2.0-1.0;
    let ymax: f64 = ymin + r.read::<f64>().powf(64.0)*(1.0-ymin);
    let time: f32 = r.read::<f32>()*1000.0;
    let value: u32 = r.read();
    let point = ((xmin,xmax),(ymin,ymax),time);
    inserts.push(Row::Insert(point, value));
  }
  {
    // batch insert the records on a new db
    let mut db: DB<_,_,P,V> = DB::open(
      |name: &str| -> Result<RandomAccessDisk,Error> {
        let p = dir.path().join(name);
        Ok(RandomAccessDisk::builder(p)
          .auto_sync(false)
          .build()?)
      }
    )?;
    let n = 5;
    let batches: Vec<Vec<Row<P,V>>> = (0..n).map(|i| {
      inserts[size+newsize/n*i..size+newsize/n*(i+1)].to_vec()
    }).collect();
    for batch in batches {
      db.batch(&batch)?;
    }
    check(&mut db, &inserts, size + newsize)?;
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

fn check<S,U> (db: &mut DB<S,U,P,V>, inserts: &Vec<Row<P,V>>, size: usize)
-> Result<(),Error> where
S: RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>) {
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
    let mut expected: Vec<(((f32,f32),(f64,f64),f32),u32)>
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
