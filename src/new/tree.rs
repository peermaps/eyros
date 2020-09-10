//use desert::{ToBytes,FromBytes};
use std::ops::{Add,Div};
//#[path="../order.rs"] mod order;
//use order::order;
#[path="../ensure.rs"] #[macro_use] mod ensure;

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
  Empty(),
  //BranchOffset(u64),
  //BranchFile(u32),
}

#[derive(Debug)]
pub struct Branch<X,Y> where X: Scalar, Y: Scalar {
  offset: u64,
  pivots: Vec<X>,
  intersections: Vec<Node<Y,X>>,
  nodes: Vec<Node<Y,X>>,
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
          //let ma = (xa.0 + xa.2) / 2.into();
          //let mb = (xb.0 + xb.2) / 2.into();
          //ma.partial_cmp(&mb).unwrap()
          xa.0.partial_cmp(&xb.0).unwrap()
        });
        xs
      },
      {
        let mut xs: Vec<usize> = (0..buckets.len()).collect();
        xs.sort_unstable_by(|a,b| {
          let xa = buckets[*a].bounds;
          let xb = buckets[*b].bounds;
          //let ma = (xa.0 + xa.2) / 2.into();
          //let mb = (xb.0 + xb.2) / 2.into();
          //ma.partial_cmp(&mb).unwrap()
          xa.1.partial_cmp(&xb.1).unwrap()
        });
        xs
      },
    );
    let buckets_yx = buckets.iter().map(|x| {
      let b = x.bounds;
      Bucket { bounds: (b.1,b.0,b.3,b.2), offset: x.offset }
    }).collect::<Vec<Bucket<Y,X>>>();
    Self::from_sorted(branch_factor, 0, (buckets, buckets_yx.as_slice()),
      (sorted.0.as_slice(), sorted.1.as_slice()))
  }
  pub fn from_sorted(branch_factor: usize, level: usize, buckets: (&[Bucket<X,Y>],&[Bucket<Y,X>]),
  sorted: (&[usize],&[usize])) -> Node<X,Y> {
    if sorted.0.len() == 0 {
      return Node::Empty();
    } else if sorted.0.len() < branch_factor {
      return Node::BucketListMem(buckets.0.to_vec());
    }
    let n = (branch_factor-1).min(sorted.0.len()-1); // number of pivots
    let is_min = (level / Self::dim()) % 2 != 0;
    let mut pivots = match sorted.0.len() {
      0 => panic!["not enough data to create a branch"],
      1 => {
        let b = &buckets.0[sorted.0[0]].bounds;
        vec![find_separation(b,b,is_min)]
      },
      2 => {
        let a = &buckets.0[sorted.0[0]].bounds;
        let b = &buckets.0[sorted.0[1]].bounds;
        vec![find_separation(a,b,is_min)]
      },
      _ => {
        (0..n).map(|k| {
          let m = k * sorted.0.len() / (n+1);
          let a = &buckets.0[sorted.0[m+0]].bounds;
          let b = &buckets.0[sorted.0[m+1]].bounds;
          find_separation(a,b,is_min)
        }).collect()
      }
    };
    pivots.sort_unstable_by(|a,b| {
      a.partial_cmp(b).unwrap()
    });
    //pad_pivots(n, &mut pivots);
    //eprintln!["n={}, pivots={:?}", n, pivots];

    let mut matched = vec![false;buckets.0.len()];
    let intersections: Vec<Node<Y,X>> = pivots.iter()
      .map(|pivot| {
        let indexes: Vec<usize> = sorted.0.iter()
          .map(|j| *j)
          .filter(|j| {
            let b = &buckets.0[*j];
            !matched[*j] && intersect(b.bounds.0, b.bounds.2, *pivot)
          })
          .collect();
        if indexes.len() == sorted.0.len() {
          eprintln!["{} == {}", indexes.len(), sorted.0.len()];
          return Node::BucketListMem(indexes.iter().map(|i| buckets.1[*i]).collect());
        }
        let b = <Branch<Y,X>>::from_sorted(
          branch_factor,
          level+1,
          (buckets.1,buckets.0),
          (
            sorted.1.iter()
              .map(|j| *j)
              .filter(|j| {
                let b = &buckets.0[*j];
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
      })
      .filter(|b| {
        match b {
          Node::Empty() => false,
          _ => true
        }
      })
      .collect();

    let nodes: Vec<Node<Y,X>> = pivots.iter().enumerate()
      .map(|(i,pivot)| {
        if i == pivots.len()-1 {
          let next_sorted: (Vec<usize>,Vec<usize>) = (
            sorted.1.iter().map(|j| *j).filter(|j| !matched[*j]).collect(),
            sorted.0.iter().map(|j| *j).filter(|j| !matched[*j]).collect()
          );
          <Branch<Y,X>>::from_sorted(
            branch_factor,
            level+1,
            (buckets.1,buckets.0),
            (next_sorted.0.as_slice(), next_sorted.1.as_slice())
          )
        } else {
          let next_sorted: (Vec<usize>,Vec<usize>) = (
            sorted.1.iter().map(|j| *j).filter(|j| {
              !matched[*j] && buckets.0[*j].bounds.2 < *pivot
            }).collect(),
            sorted.0.iter().map(|j| *j).filter(|j| {
              !matched[*j] && buckets.0[*j].bounds.2 < *pivot
            }).collect()
          );
          for j in next_sorted.0.iter() {
            matched[*j] = true;
          }
          <Branch<Y,X>>::from_sorted(
            branch_factor,
            level+1,
            (buckets.1,buckets.0),
            (next_sorted.0.as_slice(), next_sorted.1.as_slice())
          )
        }
      })
      .filter(|b| {
        match b {
          Node::Empty() => false,
          _ => true
        }
      })
      .collect();

    if nodes.len() <= 1 {
      return Node::BucketListMem(buckets.0.to_vec());
    }

    eprintln!["({}, i={}, n={}) pivots:{}",
      sorted.0.len(), intersections.len(), nodes.len(), pivots.len()
    ];

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
  pub fn build(branch_factor: usize, buckets: &[Bucket<X,Y>]) -> Tree<X,Y> {
    Self {
      root: Branch::build(branch_factor, buckets)
    }
  }
  pub fn list(&mut self) -> Vec<u64> {
    vec![]
  }
  //pub fn merge() {
  //}
}

fn find_separation<X,Y>(a: &(X,Y,X,Y), b: &(X,Y,X,Y), is_min: bool) -> X where X: Scalar {
  if is_min && intersect_iv(a.0, a.2, b.0, b.2) {
    (a.0 + b.0) / 2.into()
  } else if !is_min && intersect_iv(a.0, a.2, b.0, b.2) {
    (a.2 + b.2) / 2.into()
  } else {
    (a.2+b.0)/2.into()
  }
}

fn intersect_iv<X>(a0: X, a1: X, b0: X, b1: X) -> bool where X: PartialOrd {
  a0 <= b1 && a1 >= b0
}

fn intersect<X>(min: X, max: X, x: X) -> bool where X: PartialOrd {
  min <= x && x <= max
}

fn pad_pivots(n: usize, pivots: &mut Vec<impl Scalar>) {
  if pivots.len() == 1 {
    let p = pivots[0];
    for _ in 1..n {
      pivots.push(p);
    }
  } else {
    while pivots.len() < n {
      let slots = (n-pivots.len()).min(pivots.len()-1);
      for i in 0..slots {
        let k = slots-i-1;
        let j = k*pivots.len()/slots;
        let x = (pivots[j]+pivots[j+1])/2.into();
        pivots.insert(j+1, x);
      }
    }
  }
}
