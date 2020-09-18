use desert::{ToBytes,FromBytes,CountBytes};
use failure::{Error,bail};
use crate::tree::{Tree,Node,Branch,Bucket,Scalar};
#[path="./varint.rs"] mod varint;

impl<X,Y> ToBytes for Tree<X,Y> where X: Scalar+ToBytes+CountBytes, Y: Scalar+ToBytes+CountBytes {
  fn to_bytes(&self) -> Result<Vec<u8>,Error> {
    let mut bytes = vec![0u8;self.count_bytes()];
    self.write_bytes(&mut bytes)?;
    Ok(bytes)
  }
  fn write_bytes(&self, dst: &mut [u8]) -> Result<usize,Error> {
    match &self.root {
      Node::BranchMem(b) => {
        dst[0] = 0;
        b.write_bytes(&mut dst[1..])
      },
      Node::BucketListMem(bs) => {
        dst[0] = 1;
        bs.write_bytes(&mut dst[1..])
      },
    }
  }
}

impl<X,Y> CountBytes for Tree<X,Y>
where X: Scalar+CountBytes, Y: Scalar+CountBytes {
  fn count_bytes(&self) -> usize {
    match &self.root {
      Node::BranchMem(b) => 1 + b.count_bytes(),
      Node::BucketListMem(bs) => 1 + bs.count_bytes(),
    }
  }
  fn count_from_bytes(buf: &[u8]) -> Result<usize,Error> {
    unimplemented![]
  }
  fn count_from_bytes_more(buf: &[u8]) -> Result<Option<usize>,Error> {
    unimplemented![]
  }
}

impl<X,Y> FromBytes for Tree<X,Y> where X: Scalar+FromBytes, Y: Scalar+FromBytes {
  fn from_bytes(src: &[u8]) -> Result<(usize,Self),Error> {
    Ok(match src[0] {
      0 => {
        let (offset,b) = Branch::from_bytes_level(&src[1..], 0)?;
        (offset, Self { root: Node::BranchMem(b) })
      },
      1 => {
        let (offset,bs) = <Vec<Bucket<X,Y>>>::from_bytes(&src[1..])?;
        (offset, Self { root: Node::BucketListMem(bs) })
      },
      _ => bail!["unexpected enum value for tree at byte 0: {}", src[0]]
    })
  }
}

impl<X,Y> ToBytes for Branch<X,Y>
where X: Scalar+ToBytes+CountBytes, Y: Scalar+ToBytes+CountBytes {
  fn to_bytes(&self) -> Result<Vec<u8>,Error> {
    let payload_len = self.payload_bytes();
    let byte_len = varint::length(payload_len as u64) + payload_len;
    let mut bytes = vec![0u8;byte_len];
    self.write_bytes_with_lens(&mut bytes, payload_len, byte_len)?;
    Ok(bytes)
  }
  fn write_bytes(&self, dst: &mut [u8]) -> Result<usize,Error> {
    let payload_len = self.payload_bytes();
    let byte_len = varint::length(payload_len as u64) + payload_len;
    self.write_bytes_with_lens(dst, payload_len, byte_len)
  }
}

impl<X,Y> CountBytes for Branch<X,Y> where X: CountBytes+Scalar, Y: CountBytes+Scalar {
  fn count_bytes(&self) -> usize {
    let len = self.payload_bytes();
    varint::length(len as u64) + len
  }
  fn count_from_bytes(buf: &[u8]) -> Result<usize,Error> {
    unimplemented![]
  }
  fn count_from_bytes_more(buf: &[u8]) -> Result<Option<usize>,Error> {
    unimplemented![]
  }
}

impl<X,Y> Branch<X,Y> where X: CountBytes+Scalar, Y: CountBytes+Scalar {
  fn payload_bytes(&self) -> usize {
    (match &self.pivots {
      (Some(pivots),_) => varint::length(pivots.len() as u64)
        + pivots.iter().fold(0, |sum,p| sum + p.count_bytes()),
      (_,Some(pivots)) => varint::length(pivots.len() as u64)
        + pivots.iter().fold(0, |sum,p| sum + p.count_bytes()),
      (_,_) => panic!["unexpected pivot state"]
    }) + (self.intersections.len() + self.nodes.len() + 7) / 8
      + self.intersections.len() * 0u64.count_bytes()
      + self.nodes.len() * 0u64.count_bytes()
  }
}

impl<X,Y> Branch<X,Y> where X: ToBytes+CountBytes+Scalar, Y: ToBytes+CountBytes+Scalar {
  fn write_bytes_with_lens(&self, dst: &mut [u8], payload_len: usize, byte_len: usize) -> Result<usize,Error> {
    if dst.len() < byte_len { bail!["buffer to small to write branch"] }
    let mut offset = 0;
    // byte length
    offset += varint::encode(payload_len as u64, &mut dst[offset..])?;
    // number of pivots
    offset += match &self.pivots {
      (Some(pivots),_) => varint::encode(pivots.len() as u64, &mut dst[offset..])?,
      (_,Some(pivots)) => varint::encode(pivots.len() as u64, &mut dst[offset..])?,
      (_,_) => bail!["unexpected pivot state"]
    };
    // pivots
    match &self.pivots {
      (Some(pivots),_) => {
        for p in pivots.iter() {
          offset += p.write_bytes(&mut dst[offset..])?;
        }
      },
      (_,Some(pivots)) => {
        for p in pivots.iter() {
          offset += p.write_bytes(&mut dst[offset..])?;
        }
      },
      (_,_) => bail!["unexpected pivot state"]
    }
    // data bitfield: 1 for bucket list, 0 for branch
    let mut data_index = 0;
    for node in self.intersections.iter() {
      dst[offset+data_index/8] |= match node {
        Node::BranchMem(_) => 0,
        Node::BucketListMem(_) => 1 << (data_index%8),
      };
      data_index += 1;
    }
    for node in self.nodes.iter() {
      dst[offset+data_index/8] |= match node {
        Node::BranchMem(_) => 0,
        Node::BucketListMem(_) => 1 << (data_index%8),
      };
      data_index += 1;
    }
    offset += (data_index+7)/8;
    // intersecting, nodes
    for x in [&self.intersections,&self.nodes].iter() {
      for node in x.iter() {
        match node {
          Node::BucketListMem(bs) => {
            offset += bs.write_bytes(&mut dst[offset..])?;
          },
          Node::BranchMem(b) => {
            offset += varint::encode(b.offset, &mut dst[offset..])?;
          },
        }
      }
    }
    Ok(offset)
  }
}

impl<X,Y> Branch<X,Y> where X: Scalar+FromBytes, Y: Scalar+FromBytes {
  pub fn from_bytes_level(src: &[u8], level: usize) -> Result<(usize,Self),Error> {
    let mut offset = 0;
    let (i,payload_len) = varint::decode(&src[offset..])?;
    offset += i;

    let pivot_len = {
      let (i,pivot_len) = varint::decode(&src[offset..])?;
      offset += i;
      pivot_len as usize
    };

    // pivots
    let mut pivots = match level % Self::dim() {
      0 => {
        let mut pivots = Vec::with_capacity(pivot_len);
        for _ in 0..pivot_len {
          let (i,p) = X::from_bytes(&src[offset..])?;
          pivots.push(p);
          offset += i;
        }
        (Some(pivots),None)
      },
      1 => {
        let mut pivots = Vec::with_capacity(pivot_len);
        for _ in 0..pivot_len {
          let (i,p) = Y::from_bytes(&src[offset..])?;
          pivots.push(p);
          offset += i;
        }
        (None,Some(pivots))
      },
      _ => bail!["unexpected level modulo dimension"]
    };

    // data bitfield: 1 for bucket list, 0 for branch
    let bitfield = &src[offset..offset+(pivot_len+7)/8];
    offset += (pivot_len+7)/8;

    // intersecting
    let mut intersections: Vec<Node<X,Y>> = (0..pivot_len)
      .try_fold(Vec::with_capacity(pivot_len), |mut acc,i| -> Result<Vec<Node<X,Y>>,Error> {
        acc.push(match (bitfield[i/8]>>(i%8)) == 0 {
          true => {
            let (j,b) = Self::from_bytes_level(&src[offset..], level+1)?;
            offset += j;
            Node::BranchMem(b)
          },
          false => {
            let (j,bs) = <Vec<Bucket<X,Y>>>::from_bytes(&src[offset..])?;
            offset += j;
            Node::BucketListMem(bs)
          }
        });
        Ok(acc)
      })?;

    // nodes
    let mut nodes: Vec<Node<X,Y>> = (0..pivot_len+1)
      .try_fold(Vec::with_capacity(pivot_len+1), |mut acc,i| -> Result<Vec<Node<X,Y>>,Error> {
        let k = i + pivot_len;
        acc.push(match (bitfield[k/8]>>(k%8)) == 0 {
          true => {
            let (j,b) = Self::from_bytes_level(&src[offset..], level+1)?;
            offset += j;
            Node::BranchMem(b)
          },
          false => {
            let (j,bs) = <Vec<Bucket<X,Y>>>::from_bytes(&src[offset..])?;
            offset += j;
            Node::BucketListMem(bs)
          }
        });
        Ok(acc)
      })?;

    Ok((offset, Self {
      offset: 0,
      pivots,
      intersections,
      nodes,
    }))
  }
}

impl<X,Y> ToBytes for Bucket<X,Y>
where X: Scalar+ToBytes+CountBytes, Y: Scalar+ToBytes+CountBytes {
  fn to_bytes(&self) -> Result<Vec<u8>,Error> {
    let mut buf = vec![0u8;self.count_bytes()];
    let mut offset = 0;
    offset += varint::encode(self.offset, &mut buf[offset..])?;
    offset += self.bounds.write_bytes(&mut buf[offset..])?;
    Ok(buf)
  }
  fn write_bytes(&self, dst: &mut [u8]) -> Result<usize,Error> {
    if dst.len() < self.count_bytes() { bail!["buffer too small to write bucket"] }
    let mut offset = 0;
    offset += varint::encode(self.offset, &mut dst[offset..])?;
    offset += self.bounds.write_bytes(&mut dst[offset..])?;
    Ok(offset)
  }
}

impl<X,Y> CountBytes for Bucket<X,Y>
where X: Scalar+CountBytes, Y: Scalar+CountBytes {
  fn count_bytes(&self) -> usize {
    varint::length(self.offset) + self.bounds.count_bytes()
  }
  fn count_from_bytes(buf: &[u8]) -> Result<usize,Error> {
    unimplemented![]
  }
  fn count_from_bytes_more(buf: &[u8]) -> Result<Option<usize>,Error> {
    unimplemented![]
  }
}

impl<X,Y> FromBytes for Bucket<X,Y> where X: Scalar+FromBytes, Y: Scalar+FromBytes {
  fn from_bytes(src: &[u8]) -> Result<(usize,Self),Error> {
    let mut offset = 0;
    let (i,b_offset) = varint::decode(&src[offset..])?;
    offset += i;
    let (i,bounds) = <(X,Y,X,Y)>::from_bytes(&src[offset..])?;
    offset += i;
    Ok((offset, Self { offset: 0, bounds }))
  }
}
