use desert::{ToBytes,FromBytes,CountBytes};
use failure::{Error,bail};
use std::ops::{Add,Div};
//#[path="../order.rs"] mod order;
//use order::order;
#[path="../ensure.rs"] #[macro_use] mod ensure;
#[path="./varint.rs"] mod varint;

pub trait Scalar: Copy+PartialOrd+From<u8>+core::fmt::Debug
  +Add<Output=Self>+Div<Output=Self> {}
impl Scalar for f32 {}

#[derive(Debug,Clone,Copy)]
pub struct Bucket<X,Y> where X: Scalar, Y: Scalar {
  pub bounds: (X,Y,X,Y),
  pub offset: u64
}

#[derive(Debug)]
pub enum Node<X,Y> where X: Scalar, Y: Scalar {
  BranchMem(Branch<X,Y>),
  BucketListMem(Vec<Bucket<X,Y>>),
  //BranchOffset(u64),
  //BranchFile(u32),
}

#[derive(Debug)]
pub struct Branch<X,Y> where X: Scalar, Y: Scalar {
  pub offset: u64,
  pub pivots: (Option<Vec<X>>,Option<Vec<Y>>),
  pub intersections: Vec<Node<X,Y>>,
  pub nodes: Vec<Node<X,Y>>,
}

impl<X,Y> Branch<X,Y> where X: Scalar, Y: Scalar {
  fn dim() -> usize { 2 }
  pub fn build(branch_factor: usize, buckets: &[Bucket<X,Y>]) -> Node<X,Y> {
    let sorted = (
      {
        let mut xs: Vec<usize> = (0..buckets.len()).collect();
        xs.sort_unstable_by(|a,b| {
          let xa = buckets[*a].bounds;
          let xb = buckets[*b].bounds;
          xa.0.partial_cmp(&xb.0).unwrap()
        });
        xs
      },
      {
        let mut xs: Vec<usize> = (0..buckets.len()).collect();
        xs.sort_unstable_by(|a,b| {
          let xa = buckets[*a].bounds;
          let xb = buckets[*b].bounds;
          xa.1.partial_cmp(&xb.1).unwrap()
        });
        xs
      },
    );
    Self::from_sorted(
      branch_factor, 0, buckets,
      (sorted.0.as_slice(), sorted.1.as_slice())
    )
  }
  pub fn from_sorted(branch_factor: usize, level: usize, buckets: &[Bucket<X,Y>],
  sorted: (&[usize],&[usize])) -> Node<X,Y> {
    if sorted.0.len() == 0 {
      return Node::BucketListMem(vec![]);
    } else if sorted.0.len() < branch_factor {
      return Node::BucketListMem(buckets.to_vec());
    }
    let n = (branch_factor-1).min(sorted.0.len()-1); // number of pivots
    let is_min = (level / Self::dim()) % 2 != 0;
    let mut pivots = (None,None);
    match level % Self::dim() {
      0 => {
        let mut ps = match sorted.0.len() {
          0 => panic!["not enough data to create a branch"],
          1 => {
            let b = &buckets[sorted.0[0]].bounds;
            vec![find_separation(b.0,b.2,b.0,b.2,is_min)]
          },
          2 => {
            let a = &buckets[sorted.0[0]].bounds;
            let b = &buckets[sorted.0[1]].bounds;
            vec![find_separation(a.0,a.2,b.0,b.2,is_min)]
          },
          _ => {
            (0..n).map(|k| {
              let m = k * sorted.0.len() / (n+1);
              let a = &buckets[sorted.0[m+0]].bounds;
              let b = &buckets[sorted.0[m+1]].bounds;
              find_separation(a.0,a.2,b.0,b.2,is_min)
            }).collect()
          }
        };
        ps.sort_unstable_by(|a,b| {
          a.partial_cmp(b).unwrap()
        });
        pivots.0 = Some(ps);
      },
      1 => {
        let mut ps = match sorted.1.len() {
          0 => panic!["not enough data to create a branch"],
          1 => {
            let b = &buckets[sorted.1[0]].bounds;
            vec![find_separation(b.1,b.3,b.1,b.3,is_min)]
          },
          2 => {
            let a = &buckets[sorted.1[0]].bounds;
            let b = &buckets[sorted.1[1]].bounds;
            vec![find_separation(a.1,b.3,b.1,b.3,is_min)]
          },
          _ => {
            (0..n).map(|k| {
              let m = k * sorted.1.len() / (n+1);
              let a = &buckets[sorted.1[m+0]].bounds;
              let b = &buckets[sorted.1[m+1]].bounds;
              find_separation(a.1,a.3,b.1,b.3,is_min)
            }).collect()
          }
        };
        ps.sort_unstable_by(|a,b| {
          a.partial_cmp(b).unwrap()
        });
        pivots.1 = Some(ps);
      },
      _ => panic!["unexpected level modulo dimension"]
    };
    //pad_pivots(n, &mut pivots);
    //eprintln!["n={}, pivots={:?}", n, pivots];

    let mut matched = vec![false;buckets.len()];
    let intersections: Vec<Node<X,Y>> = match level % Self::dim() {
      0 => pivots.0.as_ref().unwrap().iter().map(|pivot| {
        let indexes: Vec<usize> = sorted.0.iter()
          .map(|j| *j)
          .filter(|j| {
            let b = &buckets[*j];
            !matched[*j] && intersect(b.bounds.0, b.bounds.2, *pivot)
          })
          .collect();
        if indexes.len() == sorted.0.len() {
          //eprintln!["{} == {}", indexes.len(), sorted.0.len()];
          return Node::BucketListMem(indexes.iter().map(|i| buckets[*i]).collect());
        }
        let b = Branch::from_sorted(
          branch_factor,
          level+1,
          buckets,
          (
            sorted.1.iter()
              .map(|j| *j)
              .filter(|j| {
                let b = &buckets[*j];
                !matched[*j] && intersect(b.bounds.0, b.bounds.2, *pivot)
              })
              .collect::<Vec<usize>>().as_slice(),
            &indexes
          )
        );
        indexes.iter().for_each(|i| {
          matched[*i] = true;
        });
        b
      }).collect(),
      1 => pivots.1.as_ref().unwrap().iter().map(|pivot| {
        let indexes: Vec<usize> = sorted.1.iter()
          .map(|j| *j)
          .filter(|j| {
            let b = &buckets[*j];
            !matched[*j] && intersect(b.bounds.1, b.bounds.3, *pivot)
          })
          .collect();
        if indexes.len() == sorted.1.len() {
          //eprintln!["{} == {}", indexes.len(), sorted.0.len()];
          return Node::BucketListMem(indexes.iter().map(|i| buckets[*i]).collect());
        }
        let b = Branch::from_sorted(
          branch_factor,
          level+1,
          buckets,
          (
            sorted.1.iter()
              .map(|j| *j)
              .filter(|j| {
                let b = &buckets[*j];
                !matched[*j] && intersect(b.bounds.1, b.bounds.3, *pivot)
              })
              .collect::<Vec<usize>>().as_slice(),
            &indexes
          )
        );
        indexes.iter().for_each(|i| {
          matched[*i] = true;
        });
        b
      }).collect(),
      _ => panic!["unexpected level modulo dimension"]
    };

    let pivot_lens = (
      match &pivots.0 {
        Some(p) => p.len(),
        None => 0
      },
      match &pivots.1 {
        Some(p) => p.len(),
        None => 0
      },
    );
    let nodes: Vec<Node<X,Y>> = match level % Self::dim() {
      0 => pivots.0.as_ref().unwrap().iter().enumerate()
        .map(|(i,pivot)| {
          if i == pivot_lens.0-1 {
            let next_sorted: (Vec<usize>,Vec<usize>) = (
              sorted.0.iter().map(|j| *j).filter(|j| !matched[*j]).collect(),
              sorted.1.iter().map(|j| *j).filter(|j| !matched[*j]).collect()
            );
            Branch::from_sorted(
              branch_factor,
              level+1,
              buckets,
              (next_sorted.0.as_slice(), next_sorted.1.as_slice())
            )
          } else {
            let next_sorted: (Vec<usize>,Vec<usize>) = (
              sorted.0.iter().map(|j| *j).filter(|j| {
                !matched[*j] && buckets[*j].bounds.2 < *pivot
              }).collect(),
              sorted.1.iter().map(|j| *j).filter(|j| {
                !matched[*j] && buckets[*j].bounds.2 < *pivot
              }).collect()
            );
            for j in next_sorted.0.iter() {
              matched[*j] = true;
            }
            Branch::from_sorted(
              branch_factor,
              level+1,
              buckets,
              (next_sorted.0.as_slice(), next_sorted.1.as_slice())
            )
          }
        }).collect(),
      1 => pivots.1.as_ref().unwrap().iter().enumerate()
        .map(|(i,pivot)| {
          if i == pivot_lens.1-1 {
            let next_sorted: (Vec<usize>,Vec<usize>) = (
              sorted.0.iter().map(|j| *j).filter(|j| !matched[*j]).collect(),
              sorted.1.iter().map(|j| *j).filter(|j| !matched[*j]).collect()
            );
            Branch::from_sorted(
              branch_factor,
              level+1,
              buckets,
              (next_sorted.0.as_slice(), next_sorted.1.as_slice())
            )
          } else {
            let next_sorted: (Vec<usize>,Vec<usize>) = (
              sorted.0.iter().map(|j| *j).filter(|j| {
                !matched[*j] && buckets[*j].bounds.3 < *pivot
              }).collect(),
              sorted.1.iter().map(|j| *j).filter(|j| {
                !matched[*j] && buckets[*j].bounds.3 < *pivot
              }).collect()
            );
            for j in next_sorted.1.iter() {
              matched[*j] = true;
            }
            Branch::from_sorted(
              branch_factor,
              level+1,
              buckets,
              (next_sorted.0.as_slice(), next_sorted.1.as_slice())
            )
          }
        }).collect(),
      _ => panic!["unexpected level modulo dimension"]
    };

    let node_count = nodes.iter().fold(0usize, |count,node| {
      count + match node {
        Node::BucketListMem(bs) => if bs.is_empty() { 0 } else { 1 },
        Node::BranchMem(_) => 1,
      }
    });
    if node_count <= 1 {
      return Node::BucketListMem(buckets.to_vec());
    }

    /*
    eprintln!["({}, i={}, n={}) pivots:{}",
      sorted.0.len(), intersections.len(), nodes.len(),
      match level % Self::dim() { 0 => pivot_lens.0, 1 => pivot_lens.1, _ => panic!["!"] }
    ];
    */

    Node::BranchMem(Self {
      offset: 0,
      pivots,
      intersections,
      nodes,
    })
  }
}

#[derive(Debug)]
pub struct Tree<X,Y> where X: Scalar, Y: Scalar {
  root: Node<X,Y>
}

impl<X,Y> Tree<X,Y> where X: Scalar, Y: Scalar {
  pub fn build(branch_factor: usize, buckets: &[Bucket<X,Y>]) -> Self {
    Self {
      root: Branch::build(branch_factor, buckets)
    }
  }
  pub fn list(&mut self) -> Vec<Bucket<X,Y>> {
    let mut cursors = vec![&self.root];
    let mut buckets = vec![];
    while let Some(c) = cursors.pop() {
      match c {
        Node::BranchMem(branch) => {
          for b in branch.intersections.iter() {
            cursors.push(b);
          }
          for b in branch.nodes.iter() {
            cursors.push(b);
          }
        },
        Node::BucketListMem(bucket_list) => {
          buckets.extend_from_slice(bucket_list.as_slice());
        }
      }
    }
    buckets
  }
  pub fn merge(branch_factor: usize, trees: &mut [&mut Self]) -> Self {
    let mut buckets = vec![];
    for tree in trees.iter_mut() {
      buckets.extend(tree.list());
    }
    // todo: split large intersecting buckets
    Self::build(branch_factor, buckets.as_slice())
  }
}

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

fn find_separation<X>(amin: X, amax: X, bmin: X, bmax: X, is_min: bool) -> X where X: Scalar {
  if is_min && intersect_iv(amin, amax, bmin, bmax) {
    (amin + bmin) / 2.into()
  } else if !is_min && intersect_iv(amin, amax, bmin, bmax) {
    (amax + bmax) / 2.into()
  } else {
    (amax + bmin)/2.into()
  }
}

fn intersect_iv<X>(a0: X, a1: X, b0: X, b1: X) -> bool where X: PartialOrd {
  a0 <= b1 && a1 >= b0
}

fn intersect<X>(min: X, max: X, x: X) -> bool where X: PartialOrd {
  min <= x && x <= max
}
