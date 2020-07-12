use eyros::{DB,Row,Point,Mix,Mix4,Error};
use random::{Source,default as rand};
use tempfile::Builder as Tmpfile;
use std::cmp::Ordering;
use async_std::prelude::*;

type P = Mix4<f32,f32,u64,u8>;
type V = u32;

#[async_std::test]
async fn mix4() -> Result<(),Error> {
  let dir = Tmpfile::new().prefix("eyros").tempdir()?;
  let mut db: DB<_,P,V> = DB::open_from_path(dir.path()).await?;
  let mut inserted: Vec<(P,V)> = vec![];
  let mut r = rand().seed([13,12]);
  for _n in 0..50 {
    let batch: Vec<Row<P,V>> = (0..1_000).map(|_| {
      let (point,value) = {
        let z = if r.read::<f32>() < 0.5 {
          Mix::Scalar(r.read::<u64>())
        } else {
          let zmin: u64 = r.read::<u64>();
          let zmax: u64 = zmin + (u64::MAX - zmin) / r.read::<u64>().max(1);
          Mix::Interval(zmin,zmax)
        };
        let w = if r.read::<f32>() < 0.5 {
          Mix::Scalar(r.read::<u8>())
        } else {
          let wmin: u8 = r.read::<u8>();
          let wmax: u8 = wmin + (u8::MAX - wmin) / r.read::<u8>().max(1);
          Mix::Interval(wmin,wmax)
        };
        if r.read::<f32>() > 0.5 {
          let xmin: f32 = r.read::<f32>()*2.0-1.0;
          let xmax: f32 = xmin + r.read::<f32>().powf(2.0)*(1.0-xmin);
          let ymin: f32 = r.read::<f32>()*2.0-1.0;
          let ymax: f32 = ymin + r.read::<f32>().powf(2.0)*(1.0-ymin);
          (
            P::new(
              Mix::Interval(xmin,xmax),
              Mix::Interval(ymin,ymax),
              z,
              w
            ),
            r.read::<u32>()
          )
        } else {
          let x: f32 = r.read::<f32>()*2.0-1.0;
          let y: f32 = r.read::<f32>()*2.0-1.0;
          (
            P::new(
              Mix::Scalar(x),
              Mix::Scalar(y),
              z,
              w
            ),
            r.read::<u32>()
          )
        }
      };
      inserted.push((point,value));
      Row::Insert(point,value)
    }).collect();
    db.batch(&batch).await?;
  }
  let bbox = (
    (-0.5,-0.8,6148914691236517205u64,20u8),
    (0.3,-0.5,14757395258967641292u64,220u8)
  );
  let mut expected: Vec<(P,V)> = inserted.iter()
    .filter(|(p,_v)| { contains(p, &bbox) })
    .map(|(p,v)| (*p,*v))
    .collect();
  let mut results = vec![];
  let mut stream = db.query(&bbox).await?;
  while let Some(result) = stream.next().await {
    let r = result?;
    results.push((r.0,r.1));
  }
  results.sort_unstable_by(cmp);
  expected.sort_unstable_by(cmp);
  assert_eq![results.len(), expected.len(), "expected number of results"];
  assert_eq![results, expected, "incorrect results"];
  Ok(())
}

fn contains (point: &P, bbox: &<P as Point>::Bounds) -> bool {
  (match point.v0 {
    Mix::Scalar(x) => contains_pt((bbox.0).0, (bbox.1).0, x),
    Mix::Interval(x0,x1) => contains_iv((bbox.0).0, (bbox.1).0, x0, x1),
  }) && (match point.v1 {
    Mix::Scalar(x) => contains_pt((bbox.0).1, (bbox.1).1, x),
    Mix::Interval(x0,x1) => contains_iv((bbox.0).1, (bbox.1).1, x0, x1),
  }) && (match point.v2 {
    Mix::Scalar(x) => contains_pt((bbox.0).2, (bbox.1).2, x),
    Mix::Interval(x0,x1) => contains_iv((bbox.0).2, (bbox.1).2, x0, x1),
  }) && (match point.v3 {
    Mix::Scalar(x) => contains_pt((bbox.0).3, (bbox.1).3, x),
    Mix::Interval(x0,x1) => contains_iv((bbox.0).3, (bbox.1).3, x0, x1),
  })
}

fn contains_iv<T> (min: T, max: T, iv0: T, iv1: T) -> bool where T: PartialOrd {
  min <= iv1 && iv0 <= max
}
fn contains_pt<T> (min: T, max: T, pt: T) -> bool where T: PartialOrd {
  min <= pt && pt <= max
}

fn cmp (a: &(P,V), b: &(P,V)) -> Ordering {
  let xcmp = match ((a.0).v0,(b.0).v0) {
    (Mix::Scalar(a0),Mix::Scalar(b0)) => a0.partial_cmp(&b0).unwrap(),
    (Mix::Interval(a0,a1),Mix::Interval(b0,b1)) => {
      match a0.partial_cmp(&b0) {
        Some(Ordering::Equal) => a1.partial_cmp(&b1).unwrap(),
        Some(x) => x,
        None => panic!["comparison failed"],
      }
    },
    (Mix::Scalar(_),Mix::Interval(_,_)) => Ordering::Less,
    (Mix::Interval(_,_),Mix::Scalar(_)) => Ordering::Greater,
  };
  if xcmp != Ordering::Equal { return xcmp }
  let ycmp = match ((a.0).v1,(b.0).v1) {
    (Mix::Scalar(a0),Mix::Scalar(b0)) => a0.partial_cmp(&b0).unwrap(),
    (Mix::Interval(a0,a1),Mix::Interval(b0,b1)) => {
      match a0.partial_cmp(&b0) {
        Some(Ordering::Equal) => a1.partial_cmp(&b1).unwrap(),
        Some(x) => x,
        None => panic!["comparison failed"],
      }
    },
    (Mix::Scalar(_),Mix::Interval(_,_)) => Ordering::Less,
    (Mix::Interval(_,_),Mix::Scalar(_)) => Ordering::Greater,
  };
  if ycmp != Ordering::Equal { return ycmp }
  let zcmp = match ((a.0).v2,(b.0).v2) {
    (Mix::Scalar(a0),Mix::Scalar(b0)) => a0.partial_cmp(&b0).unwrap(),
    (Mix::Interval(a0,a1),Mix::Interval(b0,b1)) => {
      match a0.partial_cmp(&b0) {
        Some(Ordering::Equal) => a1.partial_cmp(&b1).unwrap(),
        Some(x) => x,
        None => panic!["comparison failed"],
      }
    },
    (Mix::Scalar(_),Mix::Interval(_,_)) => Ordering::Less,
    (Mix::Interval(_,_),Mix::Scalar(_)) => Ordering::Greater,
  };
  if zcmp != Ordering::Equal { return zcmp }
  let wcmp = match ((a.0).v3,(b.0).v3) {
    (Mix::Scalar(a0),Mix::Scalar(b0)) => a0.partial_cmp(&b0).unwrap(),
    (Mix::Interval(a0,a1),Mix::Interval(b0,b1)) => {
      match a0.partial_cmp(&b0) {
        Some(Ordering::Equal) => a1.partial_cmp(&b1).unwrap(),
        Some(x) => x,
        None => panic!["comparison failed"],
      }
    },
    (Mix::Scalar(_),Mix::Interval(_,_)) => Ordering::Less,
    (Mix::Interval(_,_),Mix::Scalar(_)) => Ordering::Greater,
  };
  wcmp
}
