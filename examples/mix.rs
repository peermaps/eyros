use eyros::{DB,Row,Point,TakeBytes};
use rand::random;
use failure::{Error,bail};
use random_access_disk::RandomAccessDisk;
use std::path::PathBuf;

use serde::{Serialize,Deserialize};
use std::cmp::Ordering;
use std::f32;

#[derive(Serialize,Deserialize,Copy,Clone,Debug)]
enum P {
  Point(f32,f32),
  Interval((f32,f32),(f32,f32))
}

impl TakeBytes for P {
  fn take_bytes (buf: &[u8]) -> Result<usize,Error> {
    if buf.len() < 4 { bail!["buffer slice too small"] }
    Ok(match u32::from_be_bytes([ buf[0], buf[1], buf[2], buf[3] ]) {
      0 => 4 + 4*2, // point
      1 => 4 + 4*4, // interval
      t@_ => bail!["unexpected enum type {}", t]
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

  fn cmp_buf (bincode: &bincode::Config, buf: &[u8], bbox: &Self::Bounds,
  level: usize)-> Result<(bool,bool),Error> {
    unimplemented![]
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

  fn serialize_at (&self, bincode: &bincode::Config, level: usize)
  -> Result<Vec<u8>,Error> {
    let buf: Vec<u8> = match (level % Self::dim(), self) {
      (0,P::Point(x,_)) => bincode.serialize(&x)?,
      (0,P::Interval((_,x),_)) => bincode.serialize(&x)?,
      (1,P::Point(_,y)) => bincode.serialize(&y)?,
      (1,P::Interval(_,(_,y))) => bincode.serialize(&y)?,
      _ => panic!["match case beyond dimension"]
    };
    Ok(buf)
  }

  fn dim () -> usize { 2 }

  fn overlaps (&self, bbox: &Self::Bounds) -> bool {
    match self {
      P::Point(x,y) =>
        (bbox.0).0 <= *x && *x <= (bbox.1).0
        && (bbox.0).1 <= *y && *y <= (bbox.1).1,
      P::Interval((x0,y0),(x1,y1)) => 
        (bbox.0).0 <= *x1 && *x0 <= (bbox.1).0
        && (bbox.0).1 <= *y1 && *y0 <= (bbox.1).1,
    }
  }

  fn pivot_size_at (level: usize) -> usize {
    unimplemented![]
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

  fn format_at (bincode: &bincode::Config, buf: &[u8], level: usize)
  -> Result<String,Error> {
    unimplemented![]
  }
}

type V = u32;

fn main() -> Result<(),Error> {
  let mut db: DB<_,_,P,V> = DB::open(storage)?;
  let batch: Vec<Row<P,V>> = (0..1_000).map(|_| {
    if random::<f32>() > 0.5 {
      let xmin: f32 = random::<f32>()*2.0-1.0;
      let xmax: f32 = xmin + random::<f32>().powf(64.0)*(1.0-xmin);
      let ymin: f32 = random::<f32>()*2.0-1.0;
      let ymax: f32 = ymin + random::<f32>().powf(64.0)*(1.0-ymin);
      Row::Insert(P::Interval((xmin,xmax),(ymin,ymax)), random::<u32>())
    } else {
      let x: f32 = random::<f32>()*2.0-1.0;
      let y: f32 = random::<f32>()*2.0-1.0;
      Row::Insert(P::Point(x,y), random::<u32>())
    }
  }).collect();
  db.batch(&batch)?;

  let bbox = ((-0.5,-0.8),(0.3,-0.5));
  for result in db.query(&bbox)? {
    println!("{:?}", result?);
  }
  Ok(())
}

fn storage(name:&str) -> Result<RandomAccessDisk,Error> {
  let mut p = PathBuf::from("/tmp/eyros-db/");
  p.push(name);
  Ok(RandomAccessDisk::builder(p)
    .auto_sync(false)
    .build()?)
}
