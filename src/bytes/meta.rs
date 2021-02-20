use desert::{ToBytes,FromBytes,CountBytes};
use crate::{Point,Meta,bytes::varint,TreeRef};
use failure::{Error,bail};

impl<P> ToBytes for Meta<P> where P: Point, Self: CountBytes {
  fn to_bytes(&self) -> Result<Vec<u8>,Error> {
    let mut offset = 0;
    let mut buf = vec![0u8;self.count_bytes()];
    offset += varint::encode(self.next_tree as u64, &mut buf[offset..])?;
    offset += varint::encode(self.roots.len() as u64, &mut buf[offset..])?;
    for (i,r) in self.roots.iter().enumerate() {
      if r.is_some() {
        buf[offset+i/8] |= 1<<(i%8);
      }
    }
    offset += (self.roots.len()+7)/8;
    for root in self.roots.iter() {
      match root {
        Some(r) => {
          offset += varint::encode(r.id as u64, &mut buf[offset..])?;
          //eprintln!["meta:to bounds={:?}", r.bounds.to_bounds().unwrap()];
          offset += r.bounds.to_bounds().unwrap().write_bytes(&mut buf[offset..])?;
        },
        None => {},
      }
    }
    Ok(buf)
  }
}

impl<P> FromBytes for Meta<P> where P: Point {
  fn from_bytes(src: &[u8]) -> Result<(usize,Self),Error> {
    let mut offset = 0;
    let (n,next_tree) = varint::decode(&src[offset..])?;
    offset += n;
    let (n,len64) = varint::decode(&src[offset..])?;
    offset += n;
    let len = len64 as usize;
    if src.len() < offset + (len+7)/8 {
      bail!["not enough bytes to construct roots bitfield for Meta"]
    }
    let bitfield = &src[offset..offset+(len+7)/8];
    offset += (len+7)/8;
    let mut roots = Vec::with_capacity(len);
    for i in 0..(len as usize) {
      if (bitfield[i/8]>>(i%8))&1==1 {
        let (n,id) = varint::decode(&src[offset..])?;
        offset += n;
        let (n,bounds) = <P::Bounds>::from_bytes(&src[offset..])?;
        //eprintln!["meta:from bounds={:?}", &bounds];
        offset += n;
        roots.push(Some(TreeRef { id, bounds: P::from_bounds(&bounds) }));
      } else {
        roots.push(None);
      }
    }
    Ok((offset,Self { roots, next_tree }))
  }
}

impl<P> CountBytes for Meta<P> where P: Point {
  fn count_bytes(&self) -> usize {
    let mut size = 0;
    size += varint::length(self.next_tree as u64);
    size += varint::length(self.roots.len() as u64);
    size += (self.roots.len()+7)/8;
    for root in self.roots.iter() {
      size += match root {
        Some(r) => varint::length(r.id as u64)
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
