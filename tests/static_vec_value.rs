use eyros::{DB,Coord,Row,Scalar,Error};
use random::{Source,default as rand};
use tempfile::Builder as Tmpfile;
use async_std::prelude::*;

use std::cmp::Ordering;

type P = (Coord<f32>,Coord<f32>,Coord<f32>);
type V = Vec<u8>;

#[async_std::test]
async fn var_size_vec_value() -> Result<(),Error> {
  let dir = Tmpfile::new().prefix("eyros").tempdir()?;
  let mut db: DB<_,_,P,V> = eyros::open_from_path3(dir.path()).await?;
  let mut r = rand().seed([13,12]);
  let size = 40_000;
  let inserts: Vec<Row<P,V>> = (0..size).map(|_| {
    let xmin: f32 = r.read::<f32>()*2.0-1.0;
    let xmax: f32 = xmin + r.read::<f32>().powf(64.0)*(1.0-xmin);
    let ymin: f32 = r.read::<f32>()*2.0-1.0;
    let ymax: f32 = ymin + r.read::<f32>().powf(64.0)*(1.0-ymin);
    let time: f32 = r.read::<f32>()*1000.0;
    let mut value = vec![0u8;20];
    for i in 0..20 {
      value[i] = r.read::<u8>();
    }
    let point = (
      Coord::Interval(xmin,xmax),
      Coord::Interval(ymin,ymax),
      Coord::Scalar(time)
    );
    Row::Insert(point, value)
  }).collect();
  let n = 4;
  let batches: Vec<Vec<Row<P,V>>> = (0..n).map(|i| {
    inserts[size/n*i..size/n*(i+1)].to_vec()
  }).collect();
  for batch in batches {
    db.batch(&batch).await?;
  }

  {
    let bbox = ((-1.0,-1.0,0.0),(1.0,1.0,1000.0));
    let mut results: Vec<(P,V)> = vec![];
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
    let bbox = ((-0.8,0.1,0.0),(0.2,0.5,500.0));
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
        intersect_coord(&(bbox.0).0,&(bbox.1).0,&(r.0).0)
        && intersect_coord(&(bbox.0).1,&(bbox.1).1,&(r.0).1)
        && intersect_coord(&(bbox.0).2,&(bbox.1).2,&(r.0).2)
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

fn cmp(a: &(P,V), b: &(P,V)) -> std::cmp::Ordering {
  (match coord_cmp(&(a.0).0,&(b.0).0) {
    None => None,
    Some(Ordering::Less) => Some(Ordering::Less),
    Some(Ordering::Greater) => Some(Ordering::Greater),
    _ => coord_cmp(&(a.0).1,&(b.0).1),
  }).unwrap()
}
fn coord_cmp<X>(x: &Coord<X>, y: &Coord<X>) -> Option<std::cmp::Ordering> where X: Scalar {
  match (x,y) {
    (Coord::Scalar(a),Coord::Scalar(b)) => a.partial_cmp(b),
    (Coord::Scalar(a),Coord::Interval(b,_)) => a.partial_cmp(b),
    (Coord::Interval(a,_),Coord::Scalar(b)) => a.partial_cmp(b),
    (Coord::Interval(a,_),Coord::Interval(b,_)) => a.partial_cmp(b),
  }
}

fn intersect_coord<X>(low: &X, high: &X, c: &Coord<X>) -> bool where X: Scalar {
  match c {
    Coord::Scalar(x) => low <= x && x <= high,
    Coord::Interval(x,y) => y >= low && x <= high,
  }
}
