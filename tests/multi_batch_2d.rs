use eyros::{Coord,Scalar,Row,Error};
use random::{Source,default as rand};
use tempfile::Builder as Tmpfile;
use std::cmp::Ordering;
use async_std::prelude::*;

#[async_std::test]
async fn multi_batch_f32_f64_u16() -> Result<(),Error> {
  type P = (Coord<f32>,Coord<f64>);
  type V = u16;
  let dir = Tmpfile::new().prefix("eyros").tempdir()?;
  let mut db = eyros::open_from_path2(dir.path()).await?;
  let mut r = rand().seed([13,12]);
  let size = 10_000;
  let inserts: Vec<Row<P,V>> = (0..size).map(|_| {
    let x: f32 = (r.read::<f32>()*2.0-1.0)*1000.0;
    let y: f64 = r.read::<f64>()*4000.0+2000.0;
    let value: u16 = r.read();
    Row::Insert((Coord::Scalar(x),Coord::Scalar(y)), value)
  }).collect();
  let n = 5;
  let batches: Vec<Vec<Row<P,V>>> = (0..n).map(|i| {
    inserts[size/n*i..size/n*(i+1)].to_vec()
  }).collect();
  for batch in batches {
    db.batch(&batch).await?;
  }
  db.sync().await?;

  {
    let bbox = ((-1000.0,2000.0),(1000.0,6000.0));
    let mut results = vec![];
    let mut stream = db.query(&bbox).await?;
    while let Some(result) = stream.next().await {
      let r = result?;
      results.push((r.0,r.1));
    }
    assert_eq!(results.len(), size, "incorrect length for full region");
    let mut expected: Vec<(P,V)>
    = inserts.iter().map(|r| {
      match r {
        Row::Insert(point,value) => (point.clone(),value.clone()),
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
    let mut stream = db.query(&bbox).await?;
    while let Some(result) = stream.next().await {
      let r = result?;
      results.push((r.0,r.1));
    }
    let mut expected: Vec<(P,V)> = inserts.iter()
      .map(|r| {
        match r {
          Row::Insert(point,value) => (point.clone(),value.clone()),
          _ => panic!["unexpected row type"]
        }
      })
      .filter(|r| {
        contains(&(bbox.0).0,&(bbox.1).0,&(r.0).0)
        && contains(&(bbox.0).1,&(bbox.1).1,&(r.0).1)
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

#[async_std::test]
async fn multi_batch_f64iv_f32_u16() -> Result<(),Error> {
  type P = (Coord<f64>,Coord<f32>);
  type V = u16;
  let dir = Tmpfile::new().prefix("eyros").tempdir()?;
  let mut db = eyros::open_from_path2(dir.path()).await?;
  let mut r = rand().seed([13,12]);
  let size = 10_000;
  let inserts: Vec<Row<P,V>> = (0..size).map(|_| {
    let x0: f64 = (r.read::<f64>()*2.0-1.0)*1000.0;
    let x1: f64 = x0 + (r.read::<f64>().powf(64.0))*500.0;
    let y: f32 = r.read::<f32>()*4000.0+2000.0;
    let value: u16 = r.read();
    Row::Insert((Coord::Interval(x0,x1),Coord::Scalar(y)), value)
  }).collect();
  let n = 5;
  let batches: Vec<Vec<Row<P,V>>> = (0..n).map(|i| {
    inserts[size/n*i..size/n*(i+1)].to_vec()
  }).collect();
  for batch in batches {
    db.batch(&batch).await?;
  }
  db.sync().await?;

  {
    let bbox = ((-1500.0,2000.0),(1500.0,6000.0));
    let mut results = vec![];
    let mut stream = db.query(&bbox).await?;
    while let Some(result) = stream.next().await {
      let r = result?;
      results.push((r.0,r.1));
    }
    assert_eq!(results.len(), size, "incorrect length for full region");
    let mut expected: Vec<(P,V)> = inserts.iter().map(|r| {
      match r {
        Row::Insert(point,value) => (point.clone(),value.clone()),
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
    let mut stream = db.query(&bbox).await?;
    while let Some(result) = stream.next().await {
      let r = result?;
      results.push((r.0,r.1));
    }
    let mut expected: Vec<(P,V)> = inserts.iter()
      .map(|r| {
        match r {
          Row::Insert(point,value) => (point.clone(),value.clone()),
          _ => panic!["unexpected row type"]
        }
      })
      .filter(|r| {
        contains(&(bbox.0).0,&(bbox.1).0,&(r.0).0)
        && contains(&(bbox.0).1,&(bbox.1).1,&(r.0).1)
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

fn contains<T> (min: &T, max: &T, c: &Coord<T>) -> bool where T: Scalar {
  match c {
    Coord::Interval(x0,x1) => min <= x1 && x0 <= max,
    Coord::Scalar(x) => min <= x && x <= max
  }
}
