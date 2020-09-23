use failure::{Error,bail};
use std::ops::{Add,Div};
use desert::{ToBytes,FromBytes,CountBytes};

#[derive(Debug)]
pub enum Node2<X,Y,V> where X: Scalar, Y: Scalar, V: Value {
  BranchMem(Branch<X,Y>),
  DataMem(Vec<(X,Y,V)>),
  //BranchRef(u64),
  //BucketListRef(u64),
}

#[derive(Debug)]
pub struct Branch2<X,Y,V> where X: Scalar, Y: Scalar, V: Value {
  pub offset: u64,
  pub pivots: (Option<Vec<X>>,Option<Vec<Y>>),
  pub intersections: Vec<Node<X,Y,V>>,
  pub nodes: Vec<Node<X,Y,V>>,
}

impl<X,Y,V> Branch2<X,Y,V> where X: Scalar, Y: Scalar, V: Value {
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
