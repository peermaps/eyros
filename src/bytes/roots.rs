use desert::{ToBytes,FromBytes,CountBytes};
use crate::{Point,Roots,bytes::varint,TreeRef,TreeId};
use failure::{Error,bail};

impl<P> ToBytes for Roots<P> where P: Point, Self: CountBytes {
  fn to_bytes(&self) -> Result<Vec<u8>,Error> {
    let mut offset = 0;
    let mut buf = vec![0u8;self.count_bytes()];
    offset += varint::encode(self.refs.len() as u64, &mut buf[offset..])?;
    for i in 0..self.refs.len() {
      buf[i/8] |= 1<<(i%8);
    }
    offset += (self.refs.len()+7)/8;
    for root in self.refs.iter() {
      match root {
        Some(r) => {
          offset += r.id.write_bytes(&mut buf[offset..])?;
          offset += r.bounds.to_bounds().unwrap().write_bytes(&mut buf[offset..])?;
        },
        None => {},
      }
    }
    Ok(buf)
  }
}

impl<P> FromBytes for Roots<P> where P: Point {
  fn from_bytes(src: &[u8]) -> Result<(usize,Self),Error> {
    let mut offset = 0;
    let (n,len64) = varint::decode(&src[offset..])?;
    offset += n;
    let len = len64 as usize;
    if src.len() < offset + (len+7)/8 {
      bail!["not enough bytes to construct bitfield for Roots"]
    }
    let bitfield = &src[offset..offset+(len+7)/8];
    offset += (len+7)/8;
    let mut refs = vec![];
    for i in 0..(len as usize) {
      if (bitfield[i/8]>>(i%8))&1==1 {
        let (n,id) = TreeId::from_bytes(&src[offset..])?;
        offset += n;
        let (n,bounds) = <P::Bounds>::from_bytes(&src[offset..])?;
        offset += n;
        refs.push(Some(TreeRef { id, bounds: P::bounds_to_point(&bounds) }));
      }
    }
    Ok((offset,Self { refs }))
  }
}

impl<P> CountBytes for Roots<P> where P: Point {
  fn count_bytes(&self) -> usize {
    let mut size = varint::length(self.refs.len() as u64) + (self.refs.len()+7)/8;
    for root in self.refs.iter() {
      size += match root {
        Some(r) => r.id.count_bytes()
          + &r.bounds.to_bounds().unwrap().count_bytes(),
        None => 0,
      }
    }
    size
  }
  fn count_from_bytes(_src: &[u8]) -> Result<usize,Error> {
    unimplemented![]
  }
}
