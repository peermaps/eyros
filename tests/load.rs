use eyros::{DB,Tree,Scalar,Coord,Row,Error};
use random_access_storage::RandomAccess;
use random::{Source,default as rand};
use tempfile::Builder as Tmpfile;
use async_std::prelude::*;

use std::cmp::Ordering;

type P = (Coord<f32>,Coord<f32>,Coord<f32>);
type V = u32;

#[async_std::test]
async fn load() -> Result<(),Error> {
  let dir = Tmpfile::new().prefix("eyros").tempdir()?;
  let size = 4000;
  let mut r = rand().seed([13,12]);
  let mut inserts: Vec<Row<P,V>> = (0..size).map(|_| {
    let xmin: f32 = r.read::<f32>()*2.0-1.0;
    let xmax: f32 = xmin + r.read::<f32>().powf(64.0)*(1.0-xmin);
    let ymin: f32 = r.read::<f32>()*2.0-1.0;
    let ymax: f32 = ymin + r.read::<f32>().powf(64.0)*(1.0-ymin);
    let time: f32 = r.read::<f32>()*1000.0;
    let value: u32 = r.read();
    let point = (
      Coord::Interval(xmin,xmax),
      Coord::Interval(ymin,ymax),
      Coord::Scalar(time)
    );
    Row::Insert(point, value)
  }).collect();
  {
    // seed the db
    let mut db = eyros::open_from_path3(dir.path()).await?;
    let n = 4;
    let batches: Vec<Vec<Row<P,V>>> = (0..n).map(|i| {
      inserts[size/n*i..size/n*(i+1)].to_vec()
    }).collect();
    for batch in batches {
      db.batch(&batch).await?;
    }
    db.sync().await?;
  }
  {
    // let the previous db fall out of scope and create a new one
    // so it loads records from zero
    let mut db = eyros::open_from_path3(dir.path()).await?;
    check(&mut db, &inserts, size).await?;
  }
  // create more records
  let newsize = 2000;
  for _ in 0..newsize {
    let xmin: f32 = r.read::<f32>()*2.0-1.0;
    let xmax: f32 = xmin + r.read::<f32>().powf(64.0)*(1.0-xmin);
    let ymin: f32 = r.read::<f32>()*2.0-1.0;
    let ymax: f32 = ymin + r.read::<f32>().powf(64.0)*(1.0-ymin);
    let time: f32 = r.read::<f32>()*1000.0;
    let value: u32 = r.read();
    let point = (
      Coord::Interval(xmin,xmax),
      Coord::Interval(ymin,ymax),
      Coord::Scalar(time)
    );
    inserts.push(Row::Insert(point, value));
  }
  {
    // batch insert the records on a new db
    let mut db = eyros::open_from_path3(dir.path()).await?;
    let n = 5;
    let batches: Vec<Vec<Row<P,V>>> = (0..n).map(|i| {
      inserts[size+newsize/n*i..size+newsize/n*(i+1)].to_vec()
    }).collect();
    for batch in batches {
      db.batch(&batch).await?;
    }
    db.sync().await?;
    check(&mut db, &inserts, size + newsize).await?;
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

async fn check<S,T> (db: &mut DB<S,T,P,V>, inserts: &Vec<Row<P,V>>, size: usize) -> Result<(),Error>
where S: RandomAccess<Error=Error>+Send+Sync+Unpin+'static, T: Tree<P,V> {
  {
    let bbox = ((-1.0,-1.0,0.0),(1.0,1.0,1000.0));
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
    let bbox = ((-0.8,0.1,0.0),(0.2,0.5,500.0));
    let mut results: Vec<(P,V)> = vec![];
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
        && contains(&(bbox.0).2,&(bbox.1).2,&(r.0).2)
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
