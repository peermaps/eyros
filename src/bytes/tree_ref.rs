use desert::{ToBytes,FromBytes,CountBytes};
use crate::{Point,tree::{TreeRef,TreeId},Error};

impl<P> ToBytes for TreeRef<P> where P: Point+ToBytes, Self: CountBytes {
  fn to_bytes(&self) -> Result<Vec<u8>,Error> {
    let mut buf = vec![0u8;self.count_bytes()];
    let mut offset = 0;
    offset += self.id.write_bytes(&mut buf[offset..])?;
    self.bounds.write_bytes(&mut buf[offset..])?;
    Ok(buf)
  }
}

impl<P> FromBytes for TreeRef<P> where P: Point+FromBytes {
  fn from_bytes(src: &[u8]) -> Result<(usize,Self),Error> {
    let mut offset = 0;
    let (s,id) = TreeId::from_bytes(&src[offset..])?;
    offset += s;
    let (s,bounds) = P::from_bytes(&src[offset..])?;
    offset += s;
    Ok((offset, Self { id, bounds }))
  }
}

impl<P> CountBytes for TreeRef<P> where P: Point+CountBytes {
  fn count_bytes(&self) -> usize {
    self.id.count_bytes() + self.bounds.count_bytes()
  }
  fn count_from_bytes(_src: &[u8]) -> Result<usize,Error> {
    unimplemented![]
  }
}
