use desert::{ToBytes,FromBytes,CountBytes};
use crate::{Scalar,Point,Value,Coord,Location,Error,Storage,Overlap,RA,
  query::QStream, tree_file::TreeFile, SetupFields};
use async_std::{sync::{Arc,Mutex}};
use crate::unfold::unfold;
use random_access_storage::RandomAccess;
use std::collections::HashMap;

pub type TreeId = u64;
#[derive(Debug,Clone)]
pub struct TreeRef<P> {
  pub id: TreeId,
  pub bounds: P,
}

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

#[derive(Clone)]
pub enum InsertValue<'a,P,V> where P: Point, V: Value {
  Value(&'a V),
  Ref(TreeRef<P>),
}

macro_rules! impl_tree {
  ($Tree:ident,$Branch:ident,$Node:ident,$MState:ident,$get_bounds:ident,$build_data:ident,
  ($($T:tt),+),($($i:tt),+),($($u:ty),+),($($n:tt),+),$dim:expr) => {
    #[derive(Debug)]
    pub enum $Node<$($T),+,V> where $($T: Scalar),+, V: Value {
      Branch($Branch<$($T),+,V>),
      Data(Vec<(($(Coord<$T>),+),V)>,Vec<TreeRef<($(Coord<$T>),+)>>),
    }
    fn $build_data<'a,$($T),+,V>(rows: &[
      (($(Coord<$T>),+),InsertValue<'a,($(Coord<$T>),+),V>)
    ]) -> $Node<$($T),+,V> where $($T: Scalar),+, V: Value {
      let (points, refs) = rows.iter().fold((vec![],vec![]),|(mut points, mut refs), pv| {
        match &pv.1 {
          InsertValue::Value(v) => points.push((pv.0.clone(),(*v).clone())),
          InsertValue::Ref(r) => refs.push(r.clone()),
        }
        (points,refs)
      });
      $Node::Data(points, refs)
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
      pub inserts: &'a [(($(Coord<$T>),+),InsertValue<'a,($(Coord<$T>),+),V>)],
      pub matched: Vec<bool>,
      pub next_tree: TreeId,
      pub ext_trees: HashMap<TreeId,Arc<Mutex<$Tree<$($T),+,V>>>>,
      pub sorted: Vec<usize>,
    }

    impl<'a,$($T),+,V> $MState<'a,$($T),+,V> where $($T: Scalar),+, V: Value {
      fn next<X>(&mut self, x: &X, build: &Build, index: &mut usize,
      f: Box<dyn Fn (&X,&[(($(Coord<$T>),+),InsertValue<'a,($(Coord<$T>),+),V>)], &usize) -> bool>) -> Build {
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
        // TODO: insert self.refs into the construction
        let rlen = build.range.1 - build.range.0;
        if rlen == 0 {
          return $Node::Data(vec![],vec![]);
        } else if rlen < self.branch_factor {
          let matched = &mut self.matched;
          let inserts = &self.inserts;
          return $build_data(&self.sorted[build.range.0..build.range.1].iter().map(|i| {
            matched[*i] = true;
            inserts[*i].clone()
          }).collect::<Vec<(($(Coord<$T>),+),InsertValue<'_,($(Coord<$T>),+),V>)>>());
        }
        if build.level >= self.max_depth {
          let r = self.next_tree;
          let t = $Tree {
            root: Arc::new(self.build(&build.ext())),
          };
          //eprintln!["EXT {} bytes", t.count_bytes()];
          let tr = TreeRef {
            id: r,
            bounds: $get_bounds(
              self.sorted[build.range.0..build.range.1].iter().map(|i| *i),
              self.inserts
            ),
          };
          self.ext_trees.insert(r, Arc::new(Mutex::new(t)));
          self.next_tree += 1;
          return $Node::Data(vec![],vec![tr]);
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
                Arc::new($build_data(&self.sorted[next.range.0..next.range.1].iter().map(|i| {
                  matched[*i] = true;
                  inserts[*i].clone()
                }).collect::<Vec<(_,InsertValue<'_,_,V>)>>()))
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
            $Node::Data(bs,rs) => if bs.is_empty() && rs.is_empty() { 0 } else { 1 },
            $Node::Branch(_) => 1,
          }
        });
        if node_count <= 1 {
          let matched = &mut self.matched;
          let inserts = &self.inserts;
          return $build_data(&self.sorted[build.range.0..build.range.1].iter().map(|i| {
            matched[*i] = true;
            inserts[*i].clone()
          }).collect::<Vec<(_,InsertValue<'_,($(Coord<$T>),+),V>)>>());
        }

        $Node::Branch($Branch {
          pivots,
          intersections,
          nodes,
        })
      }
    }

    impl<$($T),+,V> $Branch<$($T),+,V> where $($T: Scalar),+, V: Value {
      pub fn build<'a>(branch_factor: usize, max_depth: usize,
      inserts: &[(($(Coord<$T>),+),InsertValue<'a,($(Coord<$T>),+),V>)],
      next_tree: &mut TreeId)
      -> (TreeRef<($(Coord<$T>),+)>,$Node<$($T),+,V>,HashMap<TreeId,Arc<Mutex<$Tree<$($T),+,V>>>>) {
        let mut mstate = $MState {
          branch_factor,
          max_depth,
          inserts,
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
        let bounds = $get_bounds(
          mstate.sorted.iter().map(|i| *i),
          mstate.inserts
        );
        let root = mstate.build(&Build {
          range: (0, inserts.len()),
          level: 0,
        });
        *next_tree = mstate.next_tree;
        let tr = TreeRef {
          id: *next_tree,
          bounds,
        };
        *next_tree += 1;
        (tr, root, mstate.ext_trees)
      }
    }

    #[derive(Debug)]
    pub struct $Tree<$($T),+,V> where $($T: Scalar),+, V: Value {
      pub root: Arc<$Node<$($T),+,V>>
    }

    #[async_trait::async_trait]
    impl<$($T),+,V> Tree<($(Coord<$T>),+),V> for $Tree<$($T),+,V> where $($T: Scalar),+, V: Value {
      fn build<'a>(branch_factor: usize, max_depth: usize,
      rows: &[(($(Coord<$T>),+),InsertValue<'a,($(Coord<$T>),+),V>)], next_tree: &mut TreeId)
      -> (TreeRef<($(Coord<$T>),+)>,Self,HashMap<TreeId,Arc<Mutex<Self>>>) {
        let (tr,root,ext_trees) = $Branch::build(
          branch_factor,
          max_depth,
          rows,
          next_tree
        );
        (tr, Self { root: Arc::new(root) }, ext_trees)
      }
      fn list(&mut self) -> (Vec<(($(Coord<$T>),+),V)>,Vec<TreeRef<($(Coord<$T>),+)>>) {
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
            $Node::Data(data,rs) => {
              rows.extend(data.iter().map(|pv| {
                (pv.0.clone(),pv.1.clone())
              }).collect::<Vec<_>>());
              refs.extend_from_slice(&rs);
            },
          }
        }
        (rows,refs)
      }
      fn query<S>(&mut self, storage: Arc<Mutex<Box<dyn Storage<S>>>>,
      bbox: &(($($T),+),($($T),+))) -> Arc<Mutex<QStream<($(Coord<$T>),+),V>>>
      where S: RA {
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
              $Node::Data(data,rs) => {
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
                // TODO: check bbox against ref bounds
                refs.extend(rs.iter().map(|r| { r.id }).collect::<Vec<TreeId>>());
              },
            }
          }
        }))))
      }
    }

    impl<$($T),+,V> $Tree<$($T),+,V> where $($T: Scalar),+, V: Value {
      async fn load<S>(storage: Arc<Mutex<Box<dyn Storage<S>>>>,
      r: TreeId) -> Result<Self,Error> where S: RA {
        let mut s = storage.lock().await.open(&format!["tree/{}",r.to_string()]).await?;
        let bytes = s.read(0, s.len().await?).await?;
        Ok(Self::from_bytes(&bytes)?.1)
      }
    }

    fn $get_bounds<$($T),+,V,I>(mut indexes: I,
    rows: &[(($(Coord<$T>),+),InsertValue<'_,($(Coord<$T>),+),V>)]) -> ($(Coord<$T>),+)
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
      let bounds = indexes.fold(ibounds, |bounds,i| {
        (
          ($(coord_min(&(rows[i].0).$i,&(bounds.0).$i)),+),
          ($(coord_max(&(rows[i].0).$i,&(bounds.1).$i)),+)
        )
      });
      ($(Coord::Interval((bounds.0).$i,(bounds.1).$i)),+)
    }

    impl<$($T),+> Overlap for (($($T),+),($($T),+)) where $($T: Scalar),+ {
      fn overlap(&self, other: &Self) -> bool {
        true $(&& intersect_iv(&(self.0).$i, &(self.1).$i, &(other.0).$i, &(other.1).$i))+
      }
    }

    impl<$($T),+> Overlap for ($(Coord<$T>),+) where $($T: Scalar),+ {
      fn overlap(&self, other: &Self) -> bool {
        true $(&& intersect_coord_coord(&self.$i, &other.$i))+
      }
    }
  }
}

#[cfg(feature="2d")] impl_tree![Tree2,Branch2,Node2,MState2,get_bounds2,build_data2,
  (P0,P1),(0,1),(usize,usize),(None,None),2
];
#[cfg(feature="3d")] impl_tree![Tree3,Branch3,Node3,MState3,get_bounds3,build_data3,
  (P0,P1,P2),(0,1,2),(usize,usize,usize),(None,None,None),3
];
#[cfg(feature="4d")] impl_tree![Tree4,Branch4,Node4,Mstate4,get_bounds4,build_data4,
  (P0,P1,P2,P3),(0,1,2,3),(usize,usize,usize,usize),(None,None,None,None),4
];
#[cfg(feature="5d")] impl_tree![Tree5,Branch5,Node5,MState5,get_bounds5,build_data5,
  (P0,P1,P2,P3,P4),(0,1,2,3,4),
  (usize,usize,usize,usize,usize),(None,None,None,None,None),5
];
#[cfg(feature="6d")] impl_tree![Tree6,Branch6,Node6,MState6,get_bounds6,build_data6,
  (P0,P1,P2,P3,P4,P5),(0,1,2,3,4,5),
  (usize,usize,usize,usize,usize,usize),(None,None,None,None,None,None),6
];
#[cfg(feature="7d")] impl_tree![Tree7,Branch7,Node7,MState7,get_bounds7,build_data7,
  (P0,P1,P2,P3,P4,P5,P6),(0,1,2,3,4,5,6),
  (usize,usize,usize,usize,usize,usize,usize),(None,None,None,None,None,None,None),7
];
#[cfg(feature="8d")] impl_tree![Tree8,Branch8,Node8,MState8,get_bounds8,build_data8,
  (P0,P1,P2,P3,P4,P5,P6,P7),(0,1,2,3,4,5,6,7),
  (usize,usize,usize,usize,usize,usize,usize,usize),(None,None,None,None,None,None,None,None),8
];

#[async_trait::async_trait]
pub trait Tree<P,V>: Send+Sync+ToBytes+FromBytes+CountBytes+std::fmt::Debug+'static
where P: Point, V: Value {
  fn build<'a>(branch_factor: usize, max_depth: usize,
    rows: &[(P,InsertValue<'a,P,V>)], next_tree: &mut TreeId)
    -> (TreeRef<P>,Self,HashMap<TreeId,Arc<Mutex<Self>>>) where Self: Sized;
  fn list(&mut self) -> (Vec<(P,V)>,Vec<TreeRef<P>>);
  fn query<S>(&mut self, storage: Arc<Mutex<Box<dyn Storage<S>>>>,
    bbox: &P::Bounds) -> Arc<Mutex<QStream<P,V>>>
    where S: RA;
}

pub struct Merge<'a,S,T,P,V> where P: Point, V: Value, T: Tree<P,V>, S: RA {
  pub fields: Arc<SetupFields>,
  pub inserts: &'a [(&'a P,&'a V)],
  pub roots: &'a [TreeRef<P>],
  pub trees: &'a mut TreeFile<S,T,P,V>,
  pub next_tree: &'a mut TreeId,
}

// return value: (tree, remove_trees, create_trees)
impl<'a,S,T,P,V> Merge<'a,S,T,P,V> where P: Point, V: Value, T: Tree<P,V>, S: RA {
  pub async fn merge(&mut self)
  -> Result<(TreeRef<P>,T,Vec<TreeId>,HashMap<TreeId,Arc<Mutex<T>>>),Error> {
    let mut lists = vec![];
    let mut l_refs = vec![];
    let mut rm_trees = vec![];
    let mut rows: Vec<(P,InsertValue<'_,P,V>)> = vec![];

    // TODO: quotas for deconstructed external tree per merge
    {
      let bounds = self.roots.iter().map(|r| r.bounds.clone()).collect::<Vec<P>>();
      let intersecting = calc_overlap::<P>(&bounds);
      // these nearly always intersect each other
      //eprintln!["TOP {:?}", intersecting];
      for (r,overlap) in self.roots.iter().zip(intersecting) {
        if overlap {
          let (list,xrefs) = self.trees.get(&r.id).await?.lock().await.list();
          lists.push(list);
          l_refs.extend(xrefs);
          rm_trees.push(r.id);
        } else {
          rows.push((r.bounds.clone(), InsertValue::Ref(r.clone())));
        }
      }
    }
    {
      // TODO : limits on number of trees to expand?
      let bounds = l_refs.iter().map(|r| r.bounds.clone()).collect::<Vec<P>>();
      let intersecting = calc_overlap::<P>(&bounds);
      //eprintln!["SUB {:?}", intersecting];
      for (_i,(r,overlap)) in l_refs.iter().zip(intersecting).enumerate() {
        if overlap {
          let (list,xrefs) = self.trees.get(&r.id).await?.lock().await.list();
          lists.push(list);
          for r in xrefs.iter() {
            rows.push((r.bounds.clone(), InsertValue::Ref(r.clone())));
          }
          rm_trees.push(r.id);
        } else {
          rows.push((r.bounds.clone(), InsertValue::Ref(r.clone())));
        }
      }
    }

    rows.extend(self.inserts.iter().map(|pv| {
      (pv.0.clone(),InsertValue::Value(pv.1))
    }).collect::<Vec<_>>());
    for list in lists.iter_mut() {
      rows.extend(list.iter().map(|pv| {
        (pv.0.clone(),InsertValue::Value(&pv.1))
      }).collect::<Vec<_>>());
    }
    let (tr, t, create_trees) = T::build(
      self.fields.branch_factor,
      self.fields.max_depth,
      &rows,
      &mut self.next_tree
    );
    Ok((tr, t, rm_trees, create_trees))
  }
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

fn intersect_coord_coord<X>(a: &Coord<X>, b: &Coord<X>) -> bool where X: Scalar {
  match (a,b) {
    (Coord::Scalar(x),Coord::Scalar(y)) => *x == *y,
    (Coord::Scalar(x),Coord::Interval(y0,y1)) => *y0 <= *x && *x <= *y1,
    (Coord::Interval(x0,x1),Coord::Scalar(y)) => *x0 <= *y && *y <= *x1,
    (Coord::Interval(x0,x1),Coord::Interval(y0,y1)) => intersect_iv(x0,x1,y0,y1),
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
