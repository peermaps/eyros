use desert::FromBytes;
use crate::{Scalar,Coord,Value,tree::{Tree2,Node2,Branch2},bytes::varint};
use failure::Error;
use async_std::sync::Arc;

impl<X,Y,V> FromBytes for Tree2<X,Y,V> where X: Scalar, Y: Scalar, V: Value {
  fn from_bytes(src: &[u8]) -> Result<(usize,Self),Error> {
    let mut offset = 0;
    let count = {
      let (s,x) = varint::decode(&src[offset..])?;
      offset += s;
      x as usize
    };
    let bounds = (
      {
        let (s,x) = X::from_bytes(&src[offset..])?;
        offset += s;
        x
      },
      {
        let (s,x) = Y::from_bytes(&src[offset..])?;
        offset += s;
        x
      },
      {
        let (s,x) = X::from_bytes(&src[offset..])?;
        offset += s;
        x
      },
      {
        let (s,x) = Y::from_bytes(&src[offset..])?;
        offset += s;
        x
      },
    );
    let (s,n) = u32::from_bytes(&src[offset..])?;
    offset += s;
    let root = match n%3 {
      0 => {
        let root = parse_branch(&src, (n/3) as usize, 0)?;
        root
      },
      1 => {
        let (s,data) = parse_data(&src[offset..], (n/3) as usize)?;
        offset += s;
        data
      },
      _ => panic!["tree pointer not implemented"]
    };
    Ok((offset,Tree2 {
      root: Arc::new(root),
      bounds,
      count
    }))
  }
}

fn parse_branch<X,Y,V>(src: &[u8], xoffset: usize, depth: usize)
-> Result<Node2<X,Y,V>,Error> where X: Scalar, Y: Scalar, V: Value {
  let mut offset = xoffset;
  let (pivot_len,pivots) = match depth%2 {
    0 => {
      let (s,x) = <Vec<X>>::from_bytes(&src[offset..])?;
      offset += s;
      (x.len(),(Some(x),None))
    },
    _ => {
      let (s,x) = <Vec<Y>>::from_bytes(&src[offset..])?;
      offset += s;
      (x.len(),(None,Some(x)))
    },
  };
  let mut intersections = vec![];
  for _ in 0..pivot_len {
    let (s,n) = u32::from_bytes(&src[offset..])?;
    offset += s;
    match n%3 {
      0 => {
        intersections.push(Arc::new(parse_branch(&src, (n/3) as usize, depth+1)?));
      },
      1 => {
        let (s,data) = parse_data(&src[offset..], (n/3) as usize)?;
        offset += s;
        intersections.push(Arc::new(data));
      },
      _ => {
        panic!["external trees not implemented"]
      }
    }
  }
  let mut nodes = vec![];
  for _ in 0..pivot_len+1 {
    let (s,n) = u32::from_bytes(&src[offset..])?;
    offset += s;
    match n%3 {
      0 => {
        nodes.push(Arc::new(parse_branch(&src, (n/3) as usize, depth+1)?));
      },
      1 => {
        let (s,data) = parse_data(&src[offset..], (n/3) as usize)?;
        offset += s;
        nodes.push(Arc::new(data));
      },
      _ => {
        panic!["external trees not implemented"]
      }
    }
  }
  Ok(Node2::Branch(Branch2 {
    pivots,
    intersections,
    nodes,
  }))
}

fn parse_data<X,Y,V>(src: &[u8], len: usize) -> Result<(usize,Node2<X,Y,V>),Error>
where X: Scalar, Y: Scalar, V: Value {
  let mut offset = 0;
  let mut data: Vec<((Coord<X>,Coord<Y>),V)> = Vec::with_capacity(len);
  let dbf = &src[offset..offset+(len+7)/8];
  offset += (len+7)/8;
  for i in 0..len {
    let bitfield = src[offset];
    offset += 1;
    let point = (
      match (bitfield>>0)&1 {
        0 => {
          let (s,x) = X::from_bytes(&src[offset..])?;
          offset += s;
          Coord::Scalar(x)
        },
        _ => {
          let (s,x) = X::from_bytes(&src[offset..])?;
          offset += s;
          let (s,y) = X::from_bytes(&src[offset..])?;
          offset += s;
          Coord::Interval(x,y)
        }
      },
      match (bitfield>>1)&1 {
        0 => {
          let (s,x) = Y::from_bytes(&src[offset..])?;
          offset += s;
          Coord::Scalar(x)
        },
        _ => {
          let (s,x) = Y::from_bytes(&src[offset..])?;
          offset += s;
          let (s,y) = Y::from_bytes(&src[offset..])?;
          offset += s;
          Coord::Interval(x,y)
        }
      },
    );
    let (s,value) = V::from_bytes(&src[offset..])?;
    offset += s;
    let is_deleted = (dbf[i/8]>>(i%8))&1 == 1;
    if !is_deleted {
      data.push((point,value));
    }
  }
  Ok((offset,Node2::Data(data)))
}
