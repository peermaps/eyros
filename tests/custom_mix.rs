use eyros::{DB,Row,Point,Cursor,Block,order,order_len,Error as E};
use failure::{Error,bail};
use random::{Source,default as rand};
use std::mem::size_of;
use tempfile::Builder as Tmpfile;
use async_std::prelude::*;

use std::cmp::{Ordering,PartialOrd};
use desert::{FromBytes,ToBytes,CountBytes};
use std::f32;

#[derive(Copy,Clone,Debug)]
enum P {
  Point(f32,f32),
  Interval((f32,f32),(f32,f32))
}
type V = u32;

impl PartialEq for P {
  fn eq (&self, other: &Self) -> bool {
    match (self,other) {
      (P::Point(x0,y0), P::Point(x1,y1)) => x0 == x1 && y0 == y1,
      (P::Point(_,_), P::Interval(_,_)) => false,
      (P::Interval(_,_), P::Point(_,_)) => false,
      (P::Interval(x0,y0), P::Interval(x1,y1)) => x0 == x1 && y0 == y1
    }
  }
}

impl ToBytes for P {
  fn to_bytes(&self) -> Result<Vec<u8>,Error> {
    let count = self.count_bytes();
    let mut bytes = vec![0u8;count];
    let size = self.write_bytes(&mut bytes)?;
    if size != count { bail!["unexpected size while writing into buffer"] }
    Ok(bytes)
  }
  fn write_bytes(&self, dst: &mut [u8]) -> Result<usize,Error> {
    if dst.len() < 1+4+4 { bail!["dst buffer too small"] }
    match self {
      P::Point(x,y) => {
        dst[0] = 0;
        let size = (*x,*y).write_bytes(&mut dst[1..])?;
        Ok(1+size)
      },
      P::Interval(x,y) => {
        dst[0] = 1;
        let size = (*x,*y).write_bytes(&mut dst[1..])?;
        Ok(1+size)
      }
    }
  }
}

impl FromBytes for P {
  fn from_bytes(src: &[u8]) -> Result<(usize,Self),Error> {
    if src.len() < 1+4+4 {
      bail!["buffer too small while loading from bytes"]
    }
    Ok(match src[0] {
      0 => {
        let (size,(x,y)) = <(f32,f32)>::from_bytes(&src[1..])?;
        (1+size, P::Point(x,y))
      },
      1 => {
        let (size,(x,y)) = <((f32,f32),(f32,f32))>::from_bytes(&src[1..])?;
        (1+size, P::Interval(x,y))
      }
      v@_ => bail!["unexpected enum value: {}", v]
    })
  }
}

impl CountBytes for P {
  fn count_bytes(&self) -> usize {
    match self {
      P::Point(_,_) => 1+4*2,
      P::Interval(_,_) => 1+4*4,
    }
  }
  fn count_from_bytes(buf: &[u8]) -> Result<usize,Error> {
    if buf.len() < 1+4+4 { bail!["buffer too small for type in count"] }
    Ok(match buf[0] {
      0 => 1+4*2,
      1 => {
        if buf.len() < 1+4*4 {
          bail!["buffer is too small for interval type in count"]
        }
        1+4*4
      },
      v@_ => bail!["unexpected enum value: {}", v]
    })
  }
}

impl Point for P {
  type Bounds = ((f32,f32),(f32,f32));
  type Range = ((f32,f32),(f32,f32));

  fn cmp_at (&self, other: &Self, level: usize) -> Ordering where Self: Sized {
    let order = match (level % Self::dim(), self, other) {
      (0,P::Point(x0,_),P::Point(x1,_)) => x0.partial_cmp(x1),
      (0,P::Interval(iv,_),P::Point(x,_)) => {
        if x >= &iv.0 && x <= &iv.1 {
          Some(Ordering::Equal)
        } else  {
          iv.0.partial_cmp(x)
        }
      },
      (0,P::Point(x,_),P::Interval(iv,_)) => {
        if x >= &iv.0 && x <= &iv.1 {
          Some(Ordering::Equal)
        } else  {
          x.partial_cmp(&iv.0)
        }
      },
      (0,P::Interval(iv0,_),P::Interval(iv1,_)) => {
        if iv0.0 <= iv1.1 && iv1.0 <= iv0.1 {
          Some(Ordering::Equal)
        } else {
          iv0.0.partial_cmp(&iv1.0)
        }
      },
      (1,P::Point(_,y0),P::Point(_,y1)) => y0.partial_cmp(y1),
      (1,P::Interval(_,iv),P::Point(_,y)) => {
        if y >= &iv.0 && y <= &iv.1 {
          Some(Ordering::Equal)
        } else  {
          iv.0.partial_cmp(y)
        }
      },
      (1,P::Point(_,y),P::Interval(_,iv)) => {
        if y >= &iv.0 && y <= &iv.1 {
          Some(Ordering::Equal)
        } else  {
          y.partial_cmp(&iv.0)
        }
      },
      (1,P::Interval(_,iv0),P::Interval(_,iv1)) => {
        if iv0.0 <= iv1.1 && iv1.0 <= iv0.1 {
          Some(Ordering::Equal)
        } else {
          iv0.0.partial_cmp(&iv1.0)
        }
      },
      _ => panic!["match case beyond dimension"]
    };
    match order { Some(x) => x, None => Ordering::Less }
  }

  fn midpoint_upper (&self, other: &Self) -> Self where Self: Sized {
    match (self, other) {
      (P::Point(x0,y0),P::Point(x1,y1)) => {
        P::Point((x0+x1)/2.0,(y0+y1)/2.0)
      },
      (P::Point(x0,y0),P::Interval(_,(x1,y1))) => {
        P::Point((x0+x1)/2.0,(y0+y1)/2.0)
      },
      (P::Interval(_,(x0,y0)),P::Point(x1,y1)) => {
        P::Point((x0+x1)/2.0,(y0+y1)/2.0)
      },
      (P::Interval(_,(x0,y0)),P::Interval(_,(x1,y1))) => {
        P::Point((x0+x1)/2.0,(y0+y1)/2.0)
      }
    }
  }

  fn serialize_at (&self, level: usize, dst: &mut [u8])
  -> Result<usize,Error> {
    match (level % Self::dim(), self) {
      (0,P::Point(x,_)) => x.write_bytes(dst),
      (0,P::Interval((_,x),_)) => x.write_bytes(dst),
      (1,P::Point(_,y)) => y.write_bytes(dst),
      (1,P::Interval(_,(_,y))) => y.write_bytes(dst),
      _ => panic!["match case beyond dimension"]
    }
  }

  fn dim () -> usize { 2 }

  fn overlaps (&self, bbox: &Self::Bounds) -> bool {
    match self {
      P::Point(x,y) =>
        (bbox.0).0 <= *x && *x <= (bbox.1).0
        && (bbox.0).1 <= *y && *y <= (bbox.1).1,
      P::Interval((x0,x1),(y0,y1)) =>
        (bbox.0).0 <= *x1 && *x0 <= (bbox.1).0
        && (bbox.0).1 <= *y1 && *y0 <= (bbox.1).1,
    }
  }

  fn query_branch (buf: &[u8], bbox: &Self::Bounds, bf: usize, level: usize)
  -> Result<(Vec<Cursor>,Vec<Block>),Error> {
    let mut cursors = vec![];
    let mut blocks = vec![];
    let n = order_len(bf);
    let mut pivots: Vec<f32> = Vec::with_capacity(n);
    let mut offset = 0;
    for _i in 0..n {
      let (size,pivot) = f32::from_bytes(&buf[offset..])?;
      pivots.push(pivot);
      offset += size;
    }
    let d_start = offset; // data bitfield
    let i_start = d_start + (n+bf+7)/8; // intersections
    let b_start = i_start + n*size_of::<u64>(); // buckets

    let mut bcursors = vec![0];
    let mut bitfield: Vec<bool> = vec![false;bf]; // which buckets
    while !bcursors.is_empty() {
      let c = bcursors.pop().unwrap();
      let i = order(bf, c);
      let cmp = {
        let pivot = pivots[i];
        match level % Self::dim() {
          0 => ((bbox.0).0 <= pivot, pivot <= (bbox.1).0),
          1 => ((bbox.0).1 <= pivot, pivot <= (bbox.1).1),
          _ => panic!["dimension not expected"]
        }
      };
      let is_data = ((buf[d_start+i/8]>>(i%8))&1) == 1;
      let i_offset = i_start + i*8;
      // intersection:
      let offset = u64::from_be_bytes([
        buf[i_offset+0], buf[i_offset+1],
        buf[i_offset+2], buf[i_offset+3],
        buf[i_offset+4], buf[i_offset+5],
        buf[i_offset+6], buf[i_offset+7],
      ]);
      if is_data && offset > 0 {
        blocks.push(offset-1);
      } else if offset > 0 {
        cursors.push((offset-1,level+1));
      }
      // internal branches:
      if cmp.0 && c*2+1 < n { // left internal
        bcursors.push(c*2+1);
      } else if cmp.0 { // left branch
        bitfield[i/2] = true;
      }
      if cmp.1 && c*2+2 < n { // right internal
        bcursors.push(c*2+2);
      } else if cmp.1 { // right branch
        bitfield[i/2+1] = true;
      }
    }
    for (i,b) in bitfield.iter().enumerate() {
      if !b { continue }
      let j = i+n;
      let is_data = (buf[d_start+j/8]>>(j%8))&1 == 1;
      let offset = u64::from_be_bytes([
        buf[b_start+i*8+0], buf[b_start+i*8+1],
        buf[b_start+i*8+2], buf[b_start+i*8+3],
        buf[b_start+i*8+4], buf[b_start+i*8+5],
        buf[b_start+i*8+6], buf[b_start+i*8+7]
      ]);
      if offset > 0 && is_data {
        blocks.push(offset-1);
      } else if offset > 0 {
        cursors.push((offset-1,level+1));
      }
    }
    Ok((cursors,blocks))
  }

  fn pivot_bytes_at (&self, _level: usize) -> usize { 4 }

  fn count_bytes_at (_buf: &[u8], _level: usize) -> Result<usize,Error> {
    Ok(4)
  }

  fn bounds (points: &Vec<Self>) -> Option<Self::Bounds> {
    if points.is_empty() { return None }
    let inf = f32::INFINITY;
    let ninf = f32::NEG_INFINITY;
    Some(points.iter().fold(((inf,inf),(ninf,ninf)), |acc,p| {
      match p {
        P::Point(x,y) => (
          (f32::min((acc.0).0,*x),f32::min((acc.0).1,*y)),
          (f32::max((acc.1).0,*x),f32::max((acc.1).1,*y))
        ),
        P::Interval((x0,y0),(x1,y1)) => (
          (f32::min((acc.0).0,*x0),f32::min((acc.0).1,*y0)),
          (f32::max((acc.1).0,*x1),f32::max((acc.1).1,*y1))
        ),
      }
    }))
  }

  fn bounds_to_range (bbox: Self::Bounds) -> Self::Range {
    (((bbox.0).0,(bbox.1).0),((bbox.0).1,(bbox.1).1))
  }

  fn format_at (_buf: &[u8], _level: usize)
  -> Result<String,Error> {
    unimplemented![]
  }
}

#[async_std::test]
async fn mix() -> Result<(),E> {
  let dir = Tmpfile::new().prefix("eyros").tempdir()?;
  let mut db: DB<_,P,V> = DB::open_from_path(dir.path()).await?;
  let mut inserted: Vec<(P,V)> = vec![];
  let mut r = rand().seed([13,12]);
  for _n in 0..50 {
    let batch: Vec<Row<P,V>> = (0..1_000).map(|_| {
      let (point,value) = {
        if r.read::<f32>() > 0.5 {
          let xmin: f32 = r.read::<f32>()*2.0-1.0;
          let xmax: f32 = xmin + r.read::<f32>().powf(2.0)*(1.0-xmin);
          let ymin: f32 = r.read::<f32>()*2.0-1.0;
          let ymax: f32 = ymin + r.read::<f32>().powf(2.0)*(1.0-ymin);
          (P::Interval((xmin,xmax),(ymin,ymax)), r.read::<u32>())
        } else {
          let x: f32 = r.read::<f32>()*2.0-1.0;
          let y: f32 = r.read::<f32>()*2.0-1.0;
          (P::Point(x,y), r.read::<u32>())
        }
      };
      inserted.push((point,value));
      Row::Insert(point,value)
    }).collect();
    db.batch(&batch).await?;
  }
  let bbox = ((-0.5,-0.8),(0.3,-0.5));
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
  match point {
    P::Point(x,y) => {
      contains_pt((bbox.0).0, (bbox.1).0, *x)
      && contains_pt((bbox.0).1, (bbox.1).1, *y)
    },
    P::Interval(x,y) => {
      contains_iv((bbox.0).0, (bbox.1).0, *x)
      && contains_iv((bbox.0).1, (bbox.1).1, *y)
    },
  }
}

fn contains_iv<T> (min: T, max: T, iv: (T,T)) -> bool where T: PartialOrd {
  min <= iv.1 && iv.0 <= max
}
fn contains_pt<T> (min: T, max: T, pt: T) -> bool where T: PartialOrd {
  min <= pt && pt <= max
}

fn cmp (a: &(P,V), b: &(P,V)) -> Ordering {
  match (a.0,b.0) {
    (P::Point(x0,y0), P::Point(x1,y1)) => {
      match x0.partial_cmp(&x1) {
        Some(Ordering::Equal) => match y0.partial_cmp(&y1) {
          Some(x) => x,
          None => panic!["comparison failed"],
        },
        Some(x) => x,
        None => panic!["comparison failed"],
      }
    },
    (P::Interval(x0,y0), P::Interval(x1,y1)) => {
      match x0.partial_cmp(&x1) {
        Some(Ordering::Equal) => match y0.partial_cmp(&y1) {
          Some(x) => x,
          None => panic!["comparison failed"],
        },
        Some(x) => x,
        None => panic!["comparison failed"],
      }
    },
    (P::Point(_,_), P::Interval(_,_)) => { Ordering::Less },
    (P::Interval(_,_), P::Point(_,_)) => { Ordering::Greater },
  }
}
