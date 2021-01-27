use eyros::{DB,Tree3,Coord,Scalar,Setup,Row,Error};
use random::{Source,default as rand};
use tempfile::Builder as Tmpfile;
use std::time;
use std::cmp::Ordering;
use async_std::prelude::*;

type P = (Coord<f32>,Coord<f32>,Coord<f32>);
type V = u32;

#[async_std::test]
async fn single_4k_batch_xxyyz_9_bf_10_data_16_base() -> Result<(),Error> {
  from_params(4000, 9, 10).await
}

#[async_std::test]
async fn single_4k_batch_xxyyz_5_bf_10_data_16_base() -> Result<(),Error> {
  from_params(4000, 5, 10).await
}

#[async_std::test]
async fn single_4k_batch_xxyyz_17_bf_10_data_16_base() -> Result<(),Error> {
  from_params(4000, 17, 10).await
}

#[async_std::test]
async fn single_32k_batch_xxyyz_9_bf_400_data_1k_base() -> Result<(),Error> {
  from_params(32_000, 9, 400).await
}

#[async_std::test]
async fn single_64k_batch_xxyyz_9_bf_1k_data_16k_base() -> Result<(),Error> {
  from_params(64_000, 9, 1_000).await
}

#[async_std::test]
async fn single_200k_batch_xxyyz_9_bf_400_data_1k_base() -> Result<(),Error> {
  from_params(200_000, 9, 400).await
}

async fn from_params (size: usize, bf: usize, max_records: usize) -> Result<(),Error> {
  let dir = Tmpfile::new().prefix("eyros").tempdir()?;
  let mut db: DB<_,Tree3<f32,f32,f32,V>,P,V> = Setup::from_path(dir.path())
    .branch_factor(bf)
    .max_records(max_records)
    .build()
    .await?;
  let mut r = rand().seed([13,12]);
  let inserts: Vec<Row<P,V>> = (0..size).map(|_| {
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
    let start = time::Instant::now();
    db.batch(&inserts).await?;
    eprintln!["batch write for {} records in {} seconds",
      size, start.elapsed().as_secs_f64()];
  }

  {
    let bbox = ((-1.0,-1.0,0.0),(1.0,1.0,1000.0));
    let mut results = vec![];
    let start = time::Instant::now();
    let mut stream = db.query(&bbox).await?;
    while let Some(result) = stream.next().await {
      let r = result?;
      results.push((r.0,r.1));
    }
    eprintln!["batch query for {} records in {} seconds",
      results.len(), start.elapsed().as_secs_f64()];
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
    let start = time::Instant::now();
    let mut stream = db.query(&bbox).await?;
    while let Some(result) = stream.next().await {
      let r = result?;
      results.push((r.0,r.1));
    }
    eprintln!["batch query for {} records in {} seconds",
      results.len(), start.elapsed().as_secs_f64()];
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
