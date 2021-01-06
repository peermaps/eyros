use desert::{ToBytes,FromBytes,CountBytes};
use crate::{Scalar,Point,Value,Coord,Location,query::QStream,Error,Storage,Overlap};
use async_std::{sync::{Arc,Mutex}};
use crate::unfold::unfold;
use random_access_storage::RandomAccess;
use std::collections::HashMap;

pub type TreeRef = u64;
pub struct Build {
  pub level: usize,
  pub range: (usize,usize), // (start,end) indexes into sorted
}
impl Build {
  fn ext(&self) -> Self {
    Self {
      range: self.range.clone(),
      level: 0,
    }
  }
}

macro_rules! impl_tree {
  ($Tree:ident,$Branch:ident,$Node:ident,$MState:ident,$get_bounds:ident,
  ($($T:tt),+),($($i:tt),+),($($u:ty),+),($($n:tt),+),$dim:expr) => {
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

    pub struct $MState<'a,$($T),+,V> where $($T: Scalar),+, V: Value {
      pub branch_factor: usize,
      pub max_depth: usize,
      pub inserts: &'a [(&'a ($(Coord<$T>),+),&'a V)],
      pub refs: &'a [TreeRef],
      pub matched: Vec<bool>,
      pub next_tree: TreeRef,
      pub ext_trees: HashMap<TreeRef,Arc<Mutex<$Tree<$($T),+,V>>>>,
      pub sorted: Vec<usize>,
    }

    impl<'a,$($T),+,V> $MState<'a,$($T),+,V> where $($T: Scalar),+, V: Value {
      fn next<X>(&mut self, x: &X, build: &Build, index: &mut usize,
      f: Box<dyn Fn (&X,&[(&'a ($(Coord<$T>),+),&'a V)], &usize) -> bool>) -> Build {
        let matched = &self.matched;
        let inserts = &self.inserts;
        // partition:
        let n = self.sorted[build.range.0+*index..build.range.1]
          .iter_mut().partition_in_place(|j| {
            !matched[*j] && f(x, inserts, j)
          });
        let range = (build.range.0+*index,build.range.0+*index+n);
        // sort for next dimension:
        match (build.level+1)%$dim {
          $($i => self.sorted[range.0..range.1].sort_unstable_by(|a,b| {
            coord_cmp(&(inserts[*a].0).$i,&(inserts[*b].0).$i).unwrap()
          })),+,
          _ => panic!["unexpected level modulo dimension"]
        };
        *index += n;
        Build {
          level: build.level + 1,
          range
        }
      }
      fn build(&mut self, build: &Build) -> $Node<$($T),+,V> {
        let rlen = build.range.1 - build.range.0;
        if rlen == 0 {
          return $Node::Data(vec![]);
        } else if rlen < self.branch_factor {
          let matched = &mut self.matched;
          let inserts = &self.inserts;
          return $Node::Data(self.sorted[build.range.0..build.range.1].iter().map(|i| {
            matched[*i] = true;
            let pv = &inserts[*i];
            (($((pv.0).$i.clone()),+),pv.1.clone())
          }).collect());
        }
        if build.level >= self.max_depth {
          let r = self.next_tree;
          let t = $Tree {
            root: Arc::new(self.build(&build.ext())),
            count: rlen,
            bounds: $get_bounds(
              self.sorted[build.range.0..build.range.1].iter().map(|i| *i),
              self.inserts
            ),
          };
          //eprintln!["EXT {} bytes", t.count_bytes()];
          self.ext_trees.insert(r, Arc::new(Mutex::new(t)));
          self.next_tree += 1;
          return $Node::Ref(r);
        }

        let n = (self.branch_factor-1).min(rlen-1); // number of pivots
        let is_min = (build.level / $dim) % 2 != 0;
        let mut pivots = ($($n),+);
        match build.level % $dim {
          $($i => {
            pivots.$i = Some(match rlen {
              0 => panic!["not enough data to create a branch"],
              1 => match &(self.inserts[self.sorted[build.range.0]].0).$i {
                Coord::Scalar(x) => {
                  vec![find_separation(x,x,x,x,is_min)]
                },
                Coord::Interval(min,max) => {
                  vec![find_separation(min,max,min,max,is_min)]
                }
              },
              2 => {
                let a = match &(self.inserts[self.sorted[build.range.0]].0).$i {
                  Coord::Scalar(x) => (x,x),
                  Coord::Interval(min,max) => (min,max),
                };
                let b = match &(self.inserts[self.sorted[build.range.0+1]].0).$i {
                  Coord::Scalar(x) => (x,x),
                  Coord::Interval(min,max) => (min,max),
                };
                vec![find_separation(a.0,a.1,b.0,b.1,is_min)]
              },
              _ => {
                (0..n).map(|k| {
                  let m = k * rlen / (n+1);
                  let a = match &(self.inserts[self.sorted[build.range.0+m+0]].0).$i {
                    Coord::Scalar(x) => (x,x),
                    Coord::Interval(min,max) => (min,max),
                  };
                  let b = match &(self.inserts[self.sorted[build.range.0+m+1]].0).$i {
                    Coord::Scalar(x) => (x,x),
                    Coord::Interval(min,max) => (min,max),
                  };
                  find_separation(a.0,a.1,b.0,b.1,is_min)
                }).collect()
              }
            });
          }),+,
          _ => panic!["unexpected level modulo dimension"]
        };

        let mut index = 0;
        let intersections: Vec<Arc<$Node<$($T),+,V>>> = match build.level % $dim {
          $($i => {
            pivots.$i.as_ref().unwrap().iter().map(|pivot| {
              let next = self.next(
                &pivot,
                build,
                &mut index,
                Box::new(|pivot, inserts, j: &usize| {
                  intersect_pivot(&(inserts[*j].0).$i, pivot)
                })
              );
              if next.range.1 - next.range.0 == rlen {
                let matched = &mut self.matched;
                let inserts = &self.inserts;
                Arc::new($Node::Data(self.sorted[next.range.0..next.range.1].iter().map(|i| {
                  let pv = &inserts[*i];
                  matched[*i] = true;
                  (pv.0.clone(),pv.1.clone())
                }).collect()))
              } else {
                Arc::new(self.build(&next))
              }
            }).collect()
          }),+,
          _ => panic!["unexpected level modulo dimension"]
        };

        let nodes = match build.level % $dim {
          $($i => {
            let pv = pivots.$i.as_ref().unwrap();
            let mut nodes = Vec::with_capacity(pv.len()+1);
            nodes.push({
              let pivot = pv.first().unwrap();
              let next = self.next(
                pivot,
                build,
                &mut index,
                Box::new(|pivot, inserts, j: &usize| {
                  coord_cmp_pivot(&(inserts[*j].0).$i, &pivot)
                    == Some(std::cmp::Ordering::Less)
                })
              );
              Arc::new(self.build(&next))
            });
            let ranges = pv.iter().zip(pv.iter().skip(1));
            for range in ranges {
              let next = self.next(
                &range,
                build,
                &mut index,
                Box::new(|range, inserts, j: &usize| {
                  intersect_coord(&(inserts[*j].0).$i, range.0, range.1)
                })
              );
              nodes.push(Arc::new(self.build(&next)));
            }
            if pv.len() > 1 {
              nodes.push({
                let pivot = pv.first().unwrap();
                let next = self.next(
                  pivot,
                  build,
                  &mut index,
                  Box::new(|pivot, inserts, j: &usize| {
                    coord_cmp_pivot(&(inserts[*j].0).$i, &pivot)
                      == Some(std::cmp::Ordering::Greater)
                  })
                );
                Arc::new(self.build(&next))
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
          let matched = &mut self.matched;
          let inserts = &self.inserts;
          return $Node::Data(self.sorted[build.range.0..build.range.1].iter().map(|i| {
            let (p,v) = &inserts[*i];
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
      pub fn build(branch_factor: usize, max_depth: usize, inserts: &[(&($(Coord<$T>),+),&V)],
      refs: &[TreeRef], next_tree: &mut TreeRef)
      -> ($Node<$($T),+,V>,HashMap<TreeRef,Arc<Mutex<$Tree<$($T),+,V>>>>) {
        let mut mstate = $MState {
          branch_factor,
          max_depth,
          inserts,
          refs,
          matched: vec![false;inserts.len()],
          next_tree: *next_tree,
          ext_trees: HashMap::new(),
          sorted: {
            let mut xs: Vec<usize> = (0..inserts.len()).collect();
            xs.sort_unstable_by(|a,b| {
              coord_cmp(&(inserts[*a].0).0,&(inserts[*b].0).0).unwrap()
            });
            xs
          }
        };
        let root = mstate.build(&Build {
          range: (0, inserts.len()),
          level: 0,
        });
        *next_tree = mstate.next_tree;
        (root, mstate.ext_trees)
      }
    }

    #[derive(Debug)]
    pub struct $Tree<$($T),+,V> where $($T: Scalar),+, V: Value {
      pub root: Arc<$Node<$($T),+,V>>,
      pub bounds: (($($T),+),($($T),+)),
      pub count: usize,
    }

    #[async_trait::async_trait]
    impl<$($T),+,V> Tree<($(Coord<$T>),+),V> for $Tree<$($T),+,V> where $($T: Scalar),+, V: Value {
      fn build(branch_factor: usize, max_depth: usize, rows: &[(&($(Coord<$T>),+),&V)],
      refs: &[TreeRef], next_tree: &mut TreeRef) -> (Self,HashMap<TreeRef,Arc<Mutex<Self>>>) {
        let (root,ext_trees) = $Branch::build(
          branch_factor,
          max_depth,
          rows,
          refs,
          next_tree
        );
        (Self {
          root: Arc::new(root),
          count: rows.len(),
          bounds: $get_bounds(0..rows.len(), rows),
        }, ext_trees)
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
      fn get_bounds(&self) -> <($(Coord<$T>),+) as Point>::Bounds {
        self.bounds.clone()
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

    fn $get_bounds<$($T),+,V,I>(mut indexes: I, rows: &[(&($(Coord<$T>),+),&V)]) -> (($($T),+),($($T),+))
    where $($T: Scalar),+, V: Value, I: Iterator<Item=usize> {
      let ibounds = (
        ($(
          match (rows[indexes.next().unwrap()].0).$i.clone() {
            Coord::Scalar(x) => x,
            Coord::Interval(x,_) => x,
          }
        ),+),
        ($(
          match (rows[indexes.next().unwrap()].0).$i.clone() {
            Coord::Scalar(x) => x,
            Coord::Interval(x,_) => x,
          }
        ),+)
      );
      indexes.fold(ibounds, |bounds,i| {
        (
          ($(coord_min(&(rows[i].0).$i,&(bounds.0).$i)),+),
          ($(coord_max(&(rows[i].0).$i,&(bounds.1).$i)),+)
        )
      })
    }

    impl<$($T),+> Overlap for (($($T),+),($($T),+)) where $($T: Scalar),+ {
      fn overlap(&self, other: &Self) -> bool {
        true $(&& intersect_iv(&(self.0).$i, &(self.1).$i, &(other.0).$i, &(other.1).$i))+
      }
    }
  }
}

#[cfg(feature="2d")] impl_tree![Tree2,Branch2,Node2,MState2,get_bounds2,
  (P0,P1),(0,1),(usize,usize),(None,None),2
];
#[cfg(feature="3d")] impl_tree![Tree3,Branch3,Node3,MState3,get_bounds3,
  (P0,P1,P2),(0,1,2),(usize,usize,usize),(None,None,None),3
];
#[cfg(feature="4d")] impl_tree![Tree4,Branch4,Node4,Mstate4,get_bounds4,
  (P0,P1,P2,P3),(0,1,2,3),(usize,usize,usize,usize),(None,None,None,None),4
];
#[cfg(feature="5d")] impl_tree![Tree5,Branch5,Node5,MState5,get_bounds5,
  (P0,P1,P2,P3,P4),(0,1,2,3,4),
  (usize,usize,usize,usize,usize),(None,None,None,None,None),5
];
#[cfg(feature="6d")] impl_tree![Tree6,Branch6,Node6,MState6,get_bounds6,
  (P0,P1,P2,P3,P4,P5),(0,1,2,3,4,5),
  (usize,usize,usize,usize,usize,usize),(None,None,None,None,None,None),6
];
#[cfg(feature="7d")] impl_tree![Tree7,Branch7,Node7,MState7,get_bounds7,
  (P0,P1,P2,P3,P4,P5,P6),(0,1,2,3,4,5,6),
  (usize,usize,usize,usize,usize,usize,usize),(None,None,None,None,None,None,None),7
];
#[cfg(feature="8d")] impl_tree![Tree8,Branch8,Node8,MState8,get_bounds8,
  (P0,P1,P2,P3,P4,P5,P6,P7),(0,1,2,3,4,5,6,7),
  (usize,usize,usize,usize,usize,usize,usize,usize),(None,None,None,None,None,None,None,None),8
];

#[async_trait::async_trait]
pub trait Tree<P,V>: Send+Sync+ToBytes+FromBytes+CountBytes+std::fmt::Debug where P: Point, V: Value {
  fn build(branch_factor: usize, max_depth: usize, rows: &[(&P,&V)],
    refs: &[TreeRef], next_tree: &mut TreeRef)
    -> (Self,HashMap<TreeRef,Arc<Mutex<Self>>>) where Self: Sized;
  fn list(&mut self) -> (Vec<(P,V)>,Vec<TreeRef>);
  fn query<S>(&mut self, storage: Arc<Mutex<Box<dyn Storage<S>+Unpin+Send+Sync>>>,
    bbox: &P::Bounds) -> Arc<Mutex<QStream<P,V>>>
    where S: RandomAccess<Error=Error>+Unpin+Send+Sync+'static;
  fn get_bounds(&self) -> P::Bounds;
}

// return value: (tree, remove_trees, create_trees)
pub async fn merge<T,P,V>(branch_factor: usize, max_depth: usize, inserts: &[(&P,&V)],
roots: &[TreeRef], trees: &HashMap<TreeRef,Arc<Mutex<T>>>, next_tree: &mut TreeRef)
-> (T,Vec<TreeRef>,HashMap<TreeRef,Arc<Mutex<T>>>)
where P: Point, V: Value, T: Tree<P,V> {
  let mut lists = vec![];
  let mut l_refs = vec![];
  let mut refs = vec![];
  let mut rm_trees = vec![];

  // TODO: quotas for deconstructed external tree per merge
  {
    let mut bounds = Vec::with_capacity(roots.len());
    for r in roots.iter() {
      bounds.push(trees[r].lock().await.get_bounds());
    }
    let intersecting = calc_overlap::<P::Bounds>(&bounds);
    // these nearly always intersect each other
    //eprintln!["TOP {:?}", intersecting];
    for (r,overlap) in roots.iter().zip(intersecting) {
      if overlap {
        let (list,xrefs) = trees[r].lock().await.list();
        lists.push(list);
        l_refs.extend(xrefs);
        rm_trees.push(*r);
      } else {
        refs.push(*r);
      }
    }
  }
  {
    // TODO : limits on number of trees to expand?
    let mut bounds = Vec::with_capacity(l_refs.len());
    for r in l_refs.iter() {
      bounds.push(trees[r].lock().await.get_bounds());
    }
    let intersecting = calc_overlap::<P::Bounds>(&bounds);
    //eprintln!["SUB {:?}", intersecting];
    for (i,(r,overlap)) in l_refs.iter().zip(intersecting).enumerate() {
      if overlap {
        let (list,xrefs) = trees[r].lock().await.list();
        lists.push(list);
        refs.extend(xrefs);
        rm_trees.push(*r);
      } else {
        refs.push(*r);
      }
    }
  }
  refs.extend(l_refs);

  let mut rows = vec![];
  rows.extend_from_slice(inserts);
  for list in lists.iter_mut() {
    rows.extend(list.iter().map(|pv| {
      (&pv.0,&pv.1)
    }).collect::<Vec<_>>());
  }
  let (t, create_trees) = T::build(branch_factor, max_depth, &rows, &refs, next_tree);
  (t, rm_trees, create_trees)
}

fn calc_overlap<X>(bounds: &[X]) -> Vec<bool> where X: Overlap {
  let mut res = vec![false;bounds.len()];
  for i in 0..bounds.len() {
    for j in i+1..bounds.len() {
      if bounds[i].overlap(&bounds[j]) {
        res[i] = true;
        res[j] = true;
      }
    }
  }
  res
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
