use desert::{ToBytes,FromBytes,CountBytes};
use crate::{Scalar,Point,Value,Coord,Location,query::QStream,Error,Storage};
use async_std::{sync::{Arc,Mutex}};
use crate::unfold::unfold;
use random_access_storage::RandomAccess;
use std::collections::HashMap;

pub type TreeRef = u64;

macro_rules! impl_tree {
  ($Tree:ident,$Branch:ident,$Node:ident,$Build:ident,$MState:ident,$get_bounds:ident,
  ($($T:tt),+),($($i:tt),+),($($j:tt),+),($($k:tt),+),($($cf:tt),+),
  ($($u:ty),+),($($n:tt),+),$dim:expr) => {
    #[derive(Debug)]
    pub enum $Node<$($T),+,V> where $($T: Scalar),+, V: Value {
      Branch($Branch<$($T),+,V>),
      Data(Vec<(($(Coord<$T>),+),V)>),
      Ref(TreeRef)
    }
    #[derive(Debug)]
    pub struct $Branch<$($T),+,V> where $($T: Scalar),+, V: Value {
      pub pivots: ($(Option<Vec<$T>>),+),
      pub intersections: Vec<Arc<$Node<$($T),+,V>>>,
      pub nodes: Vec<Arc<$Node<$($T),+,V>>>,
    }

    pub struct $Build<'a,$($T),+,V> where $($T: Scalar),+, V: Value {
      pub branch_factor: usize,
      pub max_depth: usize,
      pub level: usize,
      pub inserts: &'a Vec<(&'a ($(Coord<$T>),+),&'a V)>,
      pub range: (usize,usize), // (start,end) indexes into sorted
    }

    pub struct $MState<$($T),+,V> where $($T: Scalar),+, V: Value {
      pub matched: Vec<bool>,
      pub next_tree: TreeRef,
      pub ext_trees: HashMap<TreeRef,$Tree<$($T),+,V>>,
      pub sorted: Vec<usize>,
    }

    impl<'a,$($T),+,V> $Build<'a,$($T),+,V> where $($T: Scalar),+, V: Value {
      fn next<X>(&self, x: &X, mstate: &mut $MState<$($T),+,V>, index: &mut usize,
      f: Box<dyn Fn (&X,&Vec<(&'a ($(Coord<$T>),+),&'a V)>, &usize) -> bool>) -> Self {
        let matched = &mstate.matched;
        // partition:
        let n = mstate.sorted[self.range.0+*index..self.range.1]
          .iter_mut().partition_in_place(|j| {
            !matched[*j] && f(x, self.inserts, j)
          });
        let range = (self.range.0+*index,self.range.0+*index+n);
        // sort for next dimension:
        match (self.level+1)%$dim {
          $($i => mstate.sorted[range.0..range.1].sort_unstable_by(|a,b| {
            coord_cmp(&(self.inserts[*a].0).$i,&(self.inserts[*b].0).$i).unwrap()
          })),+,
          _ => panic!["unexpected level modulo dimension"]
        };
        *index += n;
        Self {
          branch_factor: self.branch_factor,
          max_depth: self.max_depth,
          level: self.level + 1,
          inserts: self.inserts,
          range
        }
      }
      fn ext(&self) -> Self {
        Self {
          branch_factor: self.branch_factor,
          max_depth: self.max_depth,
          level: 0,
          inserts: self.inserts,
          range: self.range.clone(),
        }
      }
      fn build(&mut self, mstate: &mut $MState<$($T),+,V>) -> $Node<$($T),+,V> {
        let rlen = self.range.1 - self.range.0;
        if rlen == 0 {
          return $Node::Data(vec![]);
        } else if rlen < self.branch_factor {
          let matched = &mut mstate.matched;
          return $Node::Data(mstate.sorted[self.range.0..self.range.1].iter().map(|i| {
            matched[*i] = true;
            let pv = &self.inserts[*i];
            (($((pv.0).$i.clone()),+),pv.1.clone())
          }).collect());
        }
        if self.level >= self.max_depth {
          let r = mstate.next_tree;
          let t = $Tree {
            root: Arc::new(self.ext().build(mstate)),
            count: rlen,
            bounds: $get_bounds(
              mstate.sorted[self.range.0..self.range.1].iter().map(|i| *i),
              self.inserts
            ),
          };
          //eprintln!["EXT {} bytes", t.count_bytes()];
          mstate.ext_trees.insert(r, t);
          mstate.next_tree += 1;
          return $Node::Ref(r);
        }

        let n = (self.branch_factor-1).min(rlen-1); // number of pivots
        let is_min = (self.level / $dim) % 2 != 0;
        let mut pivots = ($($n),+);
        match self.level % $dim {
          $($i => {
            let mut ps = match rlen {
              0 => panic!["not enough data to create a branch"],
              1 => match &(self.inserts[mstate.sorted[self.range.0]].0).$i {
                Coord::Scalar(x) => {
                  vec![find_separation(x,x,x,x,is_min)]
                },
                Coord::Interval(min,max) => {
                  vec![find_separation(min,max,min,max,is_min)]
                }
              },
              2 => {
                let a = match &(self.inserts[mstate.sorted[self.range.0]].0).$i {
                  Coord::Scalar(x) => (x,x),
                  Coord::Interval(min,max) => (min,max),
                };
                let b = match &(self.inserts[mstate.sorted[self.range.0+1]].0).$i {
                  Coord::Scalar(x) => (x,x),
                  Coord::Interval(min,max) => (min,max),
                };
                vec![find_separation(a.0,a.1,b.0,b.1,is_min)]
              },
              _ => {
                (0..n).map(|k| {
                  let m = k * rlen / (n+1);
                  let a = match &(self.inserts[mstate.sorted[self.range.0+m+0]].0).$i {
                    Coord::Scalar(x) => (x,x),
                    Coord::Interval(min,max) => (min,max),
                  };
                  let b = match &(self.inserts[mstate.sorted[self.range.0+m+1]].0).$i {
                    Coord::Scalar(x) => (x,x),
                    Coord::Interval(min,max) => (min,max),
                  };
                  find_separation(a.0,a.1,b.0,b.1,is_min)
                }).collect()
              }
            };
            ps.sort_unstable_by(|a,b| {
              a.partial_cmp(b).unwrap()
            });
            pivots.$i = Some(ps);
          }),+,
          _ => panic!["unexpected level modulo dimension"]
        };

        let mut index = 0;
        let intersections: Vec<Arc<$Node<$($T),+,V>>> = match self.level % $dim {
          $($i => {
            pivots.$i.as_ref().unwrap().iter().map(|pivot| {
              let mut next = self.next(
                &pivot,
                mstate,
                &mut index,
                Box::new(|pivot, inserts, j: &usize| {
                  intersect_pivot(&(inserts[*j].0).$i, pivot)
                })
              );
              if next.range.1 - next.range.0 == rlen {
                let matched = &mut mstate.matched;
                Arc::new($Node::Data(mstate.sorted[next.range.0..next.range.1].iter().map(|i| {
                  let pv = &self.inserts[*i];
                  matched[*i] = true;
                  (pv.0.clone(),pv.1.clone())
                }).collect()))
              } else {
                Arc::new(next.build(mstate))
              }
            }).collect()
          }),+,
          _ => panic!["unexpected level modulo dimension"]
        };

        let nodes = match self.level % $dim {
          $($i => {
            let pv = pivots.$i.as_ref().unwrap();
            let mut nodes = Vec::with_capacity(pv.len()+1);
            nodes.push({
              let pivot = pv.first().unwrap();
              let mut next = self.next(
                pivot,
                mstate,
                &mut index,
                Box::new(|pivot, inserts, j: &usize| {
                  coord_cmp_pivot(&(inserts[*j].0).$i, &pivot)
                    == Some(std::cmp::Ordering::Less)
                })
              );
              Arc::new(next.build(mstate))
            });
            let ranges = pv.iter().zip(pv.iter().skip(1));
            for range in ranges {
              let mut next = self.next(
                &range,
                mstate,
                &mut index,
                Box::new(|range, inserts, j: &usize| {
                  intersect_coord(&(inserts[*j].0).$i, range.0, range.1)
                })
              );
              nodes.push(Arc::new(next.build(mstate)));
            }
            if pv.len() > 1 {
              nodes.push({
                let pivot = pv.first().unwrap();
                let mut next = self.next(
                  pivot,
                  mstate,
                  &mut index,
                  Box::new(|pivot, inserts, j: &usize| {
                    coord_cmp_pivot(&(inserts[*j].0).$i, &pivot)
                      == Some(std::cmp::Ordering::Greater)
                  })
                );
                Arc::new(next.build(mstate))
              });
            }
            nodes
          }),+,
          _ => panic!["unexpected level modulo dimension"]
        };

        let node_count = nodes.iter().fold(0usize, |count,node| {
          count + match node.as_ref() {
            $Node::Data(bs) => if bs.is_empty() { 0 } else { 1 },
            $Node::Branch(_) => 1,
            $Node::Ref(_) => 1,
          }
        });
        if node_count <= 1 {
          let matched = &mut mstate.matched;
          return $Node::Data(mstate.sorted[self.range.0..self.range.1].iter().map(|i| {
            let (p,v) = &self.inserts[*i];
            matched[*i] = true;
            ((*p).clone(),(*v).clone())
          }).collect());
        }

        $Node::Branch($Branch {
          pivots,
          intersections,
          nodes,
        })
      }
    }

    impl<$($T),+,V> $Branch<$($T),+,V> where $($T: Scalar),+, V: Value {
      pub fn build(branch_factor: usize, max_depth: usize,
      inserts: &Vec<(&($(Coord<$T>),+),&V)>, next_tree: TreeRef) -> $Node<$($T),+,V> {
        let mut mstate = $MState {
          matched: vec![false;inserts.len()],
          next_tree,
          ext_trees: HashMap::new(),
          sorted: {
            let mut xs: Vec<usize> = (0..inserts.len()).collect();
            xs.sort_unstable_by(|a,b| {
              coord_cmp(&(inserts[*a].0).0,&(inserts[*b].0).0).unwrap()
            });
            xs
          }
        };
        $Build {
          range: (0, inserts.len()),
          branch_factor,
          max_depth,
          level: 0,
          inserts,
        }.build(&mut mstate)
      }
    }

    #[derive(Debug)]
    pub struct $Tree<$($T),+,V> where $($T: Scalar),+, V: Value {
      pub root: Arc<$Node<$($T),+,V>>,
      pub bounds: ($($T),+,$($T),+),
      pub count: usize,
    }

    #[async_trait::async_trait]
    impl<$($T),+,V> Tree<($(Coord<$T>),+),V> for $Tree<$($T),+,V> where $($T: Scalar),+, V: Value {
      fn build(branch_factor: usize, max_depth: usize,
      rows: &Vec<(&($(Coord<$T>),+),&V)>, next_tree: TreeRef) -> Self {
        Self {
          root: Arc::new($Branch::build(
            branch_factor,
            max_depth,
            rows,
            next_tree
          )),
          count: rows.len(),
          bounds: $get_bounds(0..rows.len(), rows),
        }
      }
      fn list(&mut self) -> (Vec<(($(Coord<$T>),+),V)>,Vec<TreeRef>) {
        let mut cursors = vec![Arc::clone(&self.root)];
        let mut rows = vec![];
        let mut refs = vec![];
        while let Some(c) = cursors.pop() {
          match c.as_ref() {
            $Node::Branch(branch) => {
              for b in branch.intersections.iter() {
                cursors.push(Arc::clone(b));
              }
              for b in branch.nodes.iter() {
                cursors.push(Arc::clone(b));
              }
            },
            $Node::Data(data) => {
              rows.extend(data.iter().map(|pv| {
                (pv.0.clone(),pv.1.clone())
              }).collect::<Vec<_>>());
            },
            $Node::Ref(r) => {
              refs.push(*r);
            },
          }
        }
        (rows,refs)
      }
      fn query<S>(&mut self, storage: Arc<Mutex<Box<dyn Storage<S>+Unpin+Send+Sync>>>,
      bbox: &(($($T),+),($($T),+))) -> Arc<Mutex<QStream<($(Coord<$T>),+),V>>>
      where S: RandomAccess<Error=Error>+Unpin+Send+Sync+'static {
        let istate = (
          bbox.clone(),
          vec![], // queue
          vec![(0usize,Arc::clone(&self.root))], // cursors
          vec![], // refs
          Arc::clone(&storage), // storage
        );
        Arc::new(Mutex::new(Box::new(unfold(istate, async move |mut state| {
          let bbox = &state.0;
          let queue = &mut state.1;
          let cursors = &mut state.2;
          let refs = &mut state.3;
          let storage = &mut state.4;
          loop {
            if let Some(q) = queue.pop() {
              return Some((Ok(q),state));
            }
            if cursors.is_empty() && !refs.is_empty() {
              // TODO: use a tree LRU
              match Self::load(Arc::clone(storage), refs.pop().unwrap()).await {
                Err(e) => return Some((Err(e.into()),state)),
                Ok(tree) => cursors.push((0usize,tree.root)),
              };
              continue;
            } else if cursors.is_empty() {
              return None;
            }
            let (level,c) = cursors.pop().unwrap();
            match c.as_ref() {
              $Node::Branch(branch) => {
                match level % $dim {
                  $($i => {
                    let pivots = branch.pivots.$i.as_ref().unwrap();
                    for (pivot,b) in pivots.iter().zip(branch.intersections.iter()) {
                      if &(bbox.0).$i <= pivot && pivot <= &(bbox.1).$i {
                        cursors.push((level+1,Arc::clone(b)));
                      }
                    }
                    let xs = &branch.nodes;
                    let ranges = pivots.iter().zip(pivots.iter().skip(1));
                    if &(bbox.0).$i <= pivots.first().unwrap() {
                      cursors.push((level+1,Arc::clone(xs.first().unwrap())));
                    }
                    for ((start,end),b) in ranges.zip(xs.iter().skip(1)) {
                      if intersect_iv(start, end, &(bbox.0).$i, &(bbox.1).$i) {
                        cursors.push((level+1,Arc::clone(b)));
                      }
                    }
                    if &(bbox.1).$i >= pivots.last().unwrap() {
                      cursors.push((level+1,Arc::clone(xs.last().unwrap())));
                    }
                  }),+
                  _ => panic!["unexpected level modulo dimension"]
                }
              },
              $Node::Data(data) => {
                queue.extend(data.iter()
                  .filter(|pv| {
                    intersect_coord(&(pv.0).0, &(bbox.0).0, &(bbox.1).0)
                    && intersect_coord(&(pv.0).1, &(bbox.0).1, &(bbox.1).1)
                  })
                  .map(|pv| {
                    let loc: Location = (0,0); // TODO
                    (pv.0.clone(),pv.1.clone(),loc)
                  })
                  .collect::<Vec<_>>()
                );
              },
              $Node::Ref(r) => {
                refs.push(*r);
              }
            }
          }
        }))))
      }
    }

    impl<$($T),+,V> $Tree<$($T),+,V> where $($T: Scalar),+, V: Value {
      async fn load<S>(storage: Arc<Mutex<Box<dyn Storage<S>+Unpin+Send+Sync>>>,
      r: TreeRef) -> Result<Self,Error> where S: RandomAccess<Error=Error>+Unpin+Send+Sync {
        let mut s = storage.lock().await.open(&format!["tree/{}",r.to_string()]).await?;
        let bytes = s.read(0, s.len().await?).await?;
        Ok(Self::from_bytes(&bytes)?.1)
      }
    }

    fn $get_bounds<$($T),+,V,I>(mut indexes: I,
    rows: &Vec<(&($(Coord<$T>),+),&V)>) -> ($($T),+,$($T),+)
    where $($T: Scalar),+, V: Value, I: Iterator<Item=usize> {
      let ibounds = ($(
        match (rows[indexes.next().unwrap()].0).$k.clone() {
          Coord::Scalar(x) => x,
          Coord::Interval(x,_) => x,
        }
      ),+);
      indexes.fold(ibounds, |bounds,i| {
        ($($cf(&(rows[i].0).$k, &bounds.$j)),+)
      })
    }
  }
}

#[cfg(feature="2d")] impl_tree![Tree2,Branch2,Node2,Build2,MState2,get_bounds2,
  (P0,P1),(0,1),(0,1,2,3),(0,1,0,1),(coord_min,coord_min,coord_max,coord_max),
  (usize,usize),(None,None),2
];
#[cfg(feature="3d")] impl_tree![Tree3,Branch3,Node3,Build3,MState3,get_bounds3,
  (P0,P1,P2),(0,1,2),(0,1,2,3,4,5),(0,1,2,0,1,2),
  (coord_min,coord_min,coord_min,coord_max,coord_max,coord_max),
  (usize,usize,usize),(None,None,None),3
];
#[cfg(feature="4d")] impl_tree![Tree4,Branch4,Node4,Build4,Mstate4,get_bounds4,
  (P0,P1,P2,P3),(0,1,2,3),(0,1,2,3,4,5,6,7),(0,1,2,3,0,1,2,3),
  (coord_min,coord_min,coord_min,coord_min,coord_max,coord_max,coord_max,coord_max),
  (usize,usize,usize,usize),(None,None,None,None),4
];
#[cfg(feature="5d")] impl_tree![Tree5,Branch5,Node5,Build5,MState5,get_bounds5,
  (P0,P1,P2,P3,P4),(0,1,2,3,4),(0,1,2,3,4,5,6,7,8,9),(0,1,2,3,4,0,1,2,3,4),
  (coord_min,coord_min,coord_min,coord_min,coord_min,
    coord_max,coord_max,coord_max,coord_max,coord_max),
  (usize,usize,usize,usize,usize),(None,None,None,None,None),5
];
#[cfg(feature="6d")] impl_tree![Tree6,Branch6,Node6,Build6,MState6,get_bounds6,
  (P0,P1,P2,P3,P4,P5),(0,1,2,3,4,5),(0,1,2,3,4,5,6,7,8,9,10,11),(0,1,2,3,4,5,0,1,2,3,4,5),
  (coord_min,coord_min,coord_min,coord_min,coord_min,coord_min,
    coord_max,coord_max,coord_max,coord_max,coord_max,coord_max),
  (usize,usize,usize,usize,usize,usize),(None,None,None,None,None,None),6
];
#[cfg(feature="7d")] impl_tree![Tree7,Branch7,Node7,Build7,MState7,get_bounds7,
  (P0,P1,P2,P3,P4,P5,P6),(0,1,2,3,4,5,6),
  (0,1,2,3,4,5,6,7,8,9,10,11,12,13),(0,1,2,3,4,5,6,0,1,2,3,4,5,6),
  (coord_min,coord_min,coord_min,coord_min,coord_min,coord_min,coord_min,
    coord_max,coord_max,coord_max,coord_max,coord_max,coord_max,coord_max),
  (usize,usize,usize,usize,usize,usize,usize),(None,None,None,None,None,None,None),7
];
#[cfg(feature="8d")] impl_tree![Tree8,Branch8,Node8,Build8,MState8,get_bounds8,
  (P0,P1,P2,P3,P4,P5,P6,P7),(0,1,2,3,4,5,6,7),
  (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15),(0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7),
  (coord_min,coord_min,coord_min,coord_min,coord_min,coord_min,coord_min,coord_min,
    coord_max,coord_max,coord_max,coord_max,coord_max,coord_max,coord_max,coord_max),
  (usize,usize,usize,usize,usize,usize,usize,usize),(None,None,None,None,None,None,None,None),8
];

#[async_trait::async_trait]
pub trait Tree<P,V>: Send+Sync+ToBytes+FromBytes+CountBytes+std::fmt::Debug where P: Point, V: Value {
  fn build(branch_factor: usize, max_depth: usize, rows: &Vec<(&P,&V)>, next_tree: TreeRef)
    -> Self where Self: Sized;
  fn list(&mut self) -> (Vec<(P,V)>,Vec<TreeRef>);
  fn query<S>(&mut self, storage: Arc<Mutex<Box<dyn Storage<S>+Unpin+Send+Sync>>>,
    bbox: &P::Bounds) -> Arc<Mutex<QStream<P,V>>>
    where S: RandomAccess<Error=Error>+Unpin+Send+Sync+'static;
}

pub async fn merge<T,P,V>(branch_factor: usize, max_depth: usize, inserts: &[(&P,&V)],
trees: &[Arc<Mutex<T>>], next_tree: TreeRef) -> T
where P: Point, V: Value, T: Tree<P,V> {
  let mut lists = vec![];
  let mut refs = vec![];
  for tree in trees.iter() {
    let (list,xrefs) = tree.lock().await.list();
    lists.push(list);
    refs.extend(xrefs);
  }
  let mut rows = vec![];
  rows.extend_from_slice(inserts);
  for list in lists.iter_mut() {
    rows.extend(list.iter().map(|pv| {
      (&pv.0,&pv.1)
    }).collect::<Vec<_>>());
  }
  // TODO: merge overlapping refs
  // TODO: split large intersecting buckets
  // TODO: include refs into build()
  T::build(branch_factor, max_depth, &rows, next_tree)
}

fn find_separation<X>(amin: &X, amax: &X, bmin: &X, bmax: &X, is_min: bool) -> X where X: Scalar {
  if is_min && intersect_iv(amin, amax, bmin, bmax) {
    ((*amin).clone() + (*bmin).clone()) / 2.into()
  } else if !is_min && intersect_iv(amin, amax, bmin, bmax) {
    ((*amax).clone() + (*bmax).clone()) / 2.into()
  } else {
    ((*amax).clone() + (*bmin).clone()) / 2.into()
  }
}

fn intersect_iv<X>(a0: &X, a1: &X, b0: &X, b1: &X) -> bool where X: PartialOrd {
  a1 >= b0 && a0 <= b1
}

fn intersect_pivot<X>(c: &Coord<X>, p: &X) -> bool where X: Scalar {
  match c {
    Coord::Scalar(x) => *x == *p,
    Coord::Interval(min,max) => *min <= *p && *p <= *max,
  }
}

fn intersect_coord<X>(c: &Coord<X>, low: &X, high: &X) -> bool where X: Scalar {
  match c {
    Coord::Scalar(x) => low <= x && x <= high,
    Coord::Interval(x,y) => intersect_iv(x,y,low,high),
  }
}

fn coord_cmp<X>(x: &Coord<X>, y: &Coord<X>) -> Option<std::cmp::Ordering> where X: Scalar {
  match (x,y) {
    (Coord::Scalar(a),Coord::Scalar(b)) => a.partial_cmp(b),
    (Coord::Scalar(a),Coord::Interval(b,_)) => a.partial_cmp(b),
    (Coord::Interval(a,_),Coord::Scalar(b)) => a.partial_cmp(b),
    (Coord::Interval(a,_),Coord::Interval(b,_)) => a.partial_cmp(b),
  }
}

fn coord_min<X>(x: &Coord<X>, r: &X) -> X where X: Scalar {
  let l = match x {
    Coord::Scalar(a) => a,
    Coord::Interval(a,_) => a,
  };
  match l.partial_cmp(r) {
    None => l.clone(),
    Some(std::cmp::Ordering::Less) => l.clone(),
    Some(std::cmp::Ordering::Equal) => l.clone(),
    Some(std::cmp::Ordering::Greater) => r.clone(),
  }
}

fn coord_max<X>(x: &Coord<X>, r: &X) -> X where X: Scalar {
  let l = match x {
    Coord::Scalar(a) => a,
    Coord::Interval(a,_) => a,
  };
  match l.partial_cmp(r) {
    None => l.clone(),
    Some(std::cmp::Ordering::Less) => r.clone(),
    Some(std::cmp::Ordering::Equal) => r.clone(),
    Some(std::cmp::Ordering::Greater) => l.clone(),
  }
}

fn coord_cmp_pivot<X>(x: &Coord<X>, p: &X) -> Option<std::cmp::Ordering> where X: Scalar {
  match x {
    Coord::Scalar(a) => a.partial_cmp(p),
    Coord::Interval(a,_) => a.partial_cmp(p),
  }
}
