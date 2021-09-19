use desert::{ToBytes,FromBytes,CountBytes};
use crate::{Scalar,Point,Value,Coord,Error,EyrosErrorKind,Overlap,RA,Root,
  query::QStream, tree_file::TreeFile, SetupFields};
use async_std::{sync::{Arc,Mutex}};
use crate::unfold::unfold;
use std::collections::{HashMap,HashSet};
#[path="./join.rs"] mod join;
use join::Join;

pub type TreeId = u64;
#[derive(Debug,Clone,PartialEq)]
pub struct TreeRef<P> {
  pub id: TreeId,
  pub bounds: P,
}

pub struct Build {
  pub level: usize,
  pub range: (usize,usize), // (start,end) indexes into sorted
  pub count: usize,
}
impl Build {
  fn ext(&self) -> Self {
    Self {
      range: self.range,
      level: 0,
      count: 0,
    }
  }
}

#[derive(Debug,Clone)]
pub enum InsertValue<'a,P,V> where P: Point, V: Value {
  Value(&'a V),
  Ref(TreeRef<P>),
}

macro_rules! impl_tree {
  ($Tree:ident,$Branch:ident,$Node:ident,$MState:ident,$get_bounds:ident,$build_data:ident,
  ($($T:tt),+),($($i:tt),+),($($u:ty),+),($($n:tt),+),$dim:expr) => {
    #[derive(Debug,PartialEq)]
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

    #[derive(Debug,PartialEq)]
    pub struct $Branch<$($T),+,V> where $($T: Scalar),+, V: Value {
      pub pivots: ($(Option<Vec<$T>>),+),
      pub intersections: Vec<(u32,Arc<$Node<$($T),+,V>>)>,
      pub nodes: Vec<Arc<$Node<$($T),+,V>>>,
    }

    pub struct $MState<'a,$($T),+,V> where $($T: Scalar),+, V: Value {
      pub fields: Arc<SetupFields>,
      pub inserts: &'a [(($(Coord<$T>),+),InsertValue<'a,($(Coord<$T>),+),V>)],
      pub next_tree: TreeId,
      pub ext_trees: HashMap<TreeId,Arc<Mutex<$Tree<$($T),+,V>>>>,
      pub sorted: Vec<usize>,
    }

    impl<'a,$($T),+,V> $MState<'a,$($T),+,V> where $($T: Scalar),+, V: Value {
      fn next<T>(&mut self, x: &T, build: &Build, index: &mut usize,
      f: Box<dyn Fn (&T,&[(($(Coord<$T>),+),InsertValue<'a,($(Coord<$T>),+),V>)], &usize) -> bool>) -> Build {
        let inserts = &self.inserts;
        // partition:
        let n = self.sorted[build.range.0+*index..build.range.1]
          .iter_mut().partition_in_place(|j| {
            f(x, inserts, j)
          });
        let range = (build.range.0+*index,build.range.0+*index+n);
        // sort for next dimension:
        match (build.level+1)%$dim {
          $($i => self.sorted[range.0..range.1].sort_unstable_by(|a,b| {
            match coord_cmp(&(inserts[*a].0).$i,&(inserts[*b].0).$i) {
              Some(c) => c,
              None => panic!["comparison failed for sorting (1). a={:?} b={:?}",
                inserts[*a], inserts[*b]],
            }
          })),+,
          _ => panic!["unexpected level modulo dimension"]
        };
        *index += n;
        Build {
          level: build.level + 1,
          range,
          count: build.count + (build.range.1-build.range.0) - (range.1-range.0),
        }
      }
      fn build(&mut self, build: &Build, is_rm: bool) -> $Node<$($T),+,V> {
        let rlen = build.range.1 - build.range.0;
        if rlen == 0 {
          return $Node::Data(vec![],vec![]);
        } else if rlen < self.fields.inline || rlen <= 2 {
          let inserts = &self.inserts;
          return $build_data(&self.sorted[build.range.0..build.range.1].iter().map(|i| {
            inserts[*i].clone()
          }).collect::<Vec<(($(Coord<$T>),+),InsertValue<'_,($(Coord<$T>),+),V>)>>());
        } else if !is_rm && rlen <= self.fields.max_records {
          let r = self.next_tree;
          let tr = TreeRef {
            id: r,
            bounds: $get_bounds(
              self.sorted[build.range.0..build.range.1].iter().map(|i| *i),
              self.inserts
            ),
          };
          self.next_tree += 1;
          let inserts = &self.inserts;
          let root = $build_data(&self.sorted[build.range.0..build.range.1].iter().map(|i| {
            inserts[*i].clone()
          }).collect::<Vec<(($(Coord<$T>),+),InsertValue<'_,($(Coord<$T>),+),V>)>>());
          let t = $Tree::new(Arc::new(root));
          self.ext_trees.insert(r, Arc::new(Mutex::new(t)));
          return $Node::Data(vec![],vec![tr]);
        }
        if build.level >= self.fields.max_depth || build.count >= self.fields.max_records {
          let r = self.next_tree;
          let tr = TreeRef {
            id: r,
            bounds: $get_bounds(
              self.sorted[build.range.0..build.range.1].iter().map(|i| *i),
              self.inserts
            ),
          };
          self.next_tree += 1;
          let t = $Tree::new(Arc::new(self.build(&build.ext(), is_rm)));
          self.ext_trees.insert(r, Arc::new(Mutex::new(t)));
          return $Node::Data(vec![],vec![tr]);
        }

        let n = (self.fields.branch_factor-1).min(rlen-1); // number of pivots
        let is_min = (build.level / $dim) % 2 != 0;
        let mut pivots = ($($n),+);
        match build.level % $dim {
          $($i => {
            let mut ps = match rlen {
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
                  //assert![n+1 > 0, "!(n+1 > 0). n={}", n];
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
            };
            // pivots aren't always sorted. make sure they are:
            ps.sort_unstable_by(|a,b| {
              match a.partial_cmp(b) {
                Some(c) => c,
                None => panic!["comparison failed for sorting pivots. a={:?} b={:?}", a, b],
              }
            });
            pivots.$i = Some(ps);
          }),+,
          _ => panic!["unexpected level modulo dimension"]
        };

        let mut index = 0;
        let intersections: Vec<(u32,Arc<$Node<$($T),+,V>>)> = match build.level % $dim {
          $($i => {
            let ps = pivots.$i.as_ref().unwrap();
            let mut ibuckets: HashMap<u32,HashSet<usize>> = HashMap::new();
            for j in self.sorted[build.range.0..build.range.1].iter() {
              let mut bitfield: u32 = 0;
              for (i,pivot) in ps.iter().enumerate() {
                if intersect_pivot(&(self.inserts[*j].0).$i, pivot) {
                  bitfield |= (1 << i);
                }
              }
              if bitfield > 0 && ibuckets.contains_key(&bitfield) {
                ibuckets.get_mut(&bitfield).unwrap().insert(*j);
              } else if bitfield > 0 {
                let mut hset = HashSet::new();
                hset.insert(*j);
                ibuckets.insert(bitfield, hset);
              }
            }
            ibuckets.iter_mut().map(|(bitfield,hset)| {
              let next = self.next(
                &hset,
                build,
                &mut index,
                Box::new(|hset, _inserts, j: &usize| {
                  hset.contains(j)
                })
              );
              (*bitfield, Arc::new(self.build(&next, is_rm)))
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
                  match &(inserts[*j].0).$i {
                    Coord::Scalar(x) => x < &pivot,
                    Coord::Interval(_,x) => x < &pivot,
                  }
                })
              );
              Arc::new(self.build(&next, is_rm))
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
              nodes.push(Arc::new(self.build(&next, is_rm)));
            }
            if (pv.len() > 1) {
              nodes.push({
                let pivot = pv.last().unwrap();
                let next = self.next(
                  &pivot,
                  build,
                  &mut index,
                  Box::new(|pivot, inserts, j: &usize| {
                    match &(inserts[*j].0).$i {
                      Coord::Scalar(x) => x > &pivot,
                      Coord::Interval(x,_) => x > &pivot,
                    }
                  })
                );
                Arc::new(self.build(&next, is_rm))
              });
            }
            nodes
          }),+,
          _ => panic!["unexpected level modulo dimension"]
        };
        assert![index == build.range.1 - build.range.0,
          "{} leftover records not built into nodes or intersections",
          (build.range.1 - build.range.0) - index
        ];
        $Node::Branch($Branch {
          pivots,
          intersections,
          nodes,
        })
      }
    }

    impl<$($T),+,V> $Branch<$($T),+,V> where $($T: Scalar),+, V: Value {
      pub fn new(pivots: ($(Option<Vec<$T>>),+), intersections: Vec<(u32,Arc<$Node<$($T),+,V>>)>,
      nodes: Vec<Arc<$Node<$($T),+,V>>>) -> Self {
        Self {
          pivots,
          intersections,
          nodes,
        }
      }
      pub fn build<'a>(
        fields: Arc<SetupFields>,
        inserts: &[(($(Coord<$T>),+),InsertValue<'a,($(Coord<$T>),+),V>)],
        next_tree: &mut TreeId,
        is_rm: bool,
      ) -> (Option<TreeRef<($(Coord<$T>),+)>>,CreateTrees<$Tree<$($T),+,V>>) {
        if inserts.is_empty() { return (None, HashMap::new()) }
        let mut mstate = $MState {
          fields,
          inserts,
          next_tree: *next_tree,
          ext_trees: HashMap::new(),
          sorted: {
            let mut xs: Vec<usize> = (0..inserts.len()).collect();
            xs.sort_unstable_by(|a,b| {
              match coord_cmp(&(inserts[*a].0).0,&(inserts[*b].0).0) {
                Some(c) => c,
                None => panic!["comparison failed for sorting (2). a={:?} b={:?}",
                  inserts[*a], inserts[*b]],
              }
            });
            xs
          }
        };
        //assert![mstate.sorted.len() >= 1, "sorted.len()={}, must be >= 1", mstate.sorted.len()];
        let bounds = $get_bounds(
          mstate.sorted.iter().map(|i| *i),
          mstate.inserts
        );
        let root = mstate.build(&Build {
          range: (0, inserts.len()),
          level: 0,
          count: 0,
        }, is_rm);
        *next_tree = mstate.next_tree;
        let tr = TreeRef {
          id: *next_tree,
          bounds,
        };
        *next_tree += 1;
        mstate.ext_trees.insert(tr.id, Arc::new(Mutex::new($Tree {
          root: Arc::new(root)
        })));
        (Some(tr), mstate.ext_trees)
      }
    }

    /// Tree in N dimensions. You might need to manually specify the appropriate
    /// Tree{N} as T for `DB<_,T,_,_>`.
    #[derive(Debug,PartialEq)]
    pub struct $Tree<$($T),+,V> where $($T: Scalar),+, V: Value {
      pub root: Arc<$Node<$($T),+,V>>
    }
    impl<$($T),+,V> $Tree<$($T),+,V> where $($T: Scalar),+, V: Value {
      pub fn new(root: Arc<$Node<$($T),+,V>>) -> Self {
        Self { root }
      }
    }

    #[async_trait::async_trait]
    impl<$($T),+,V> Tree<($(Coord<$T>),+),V> for $Tree<$($T),+,V>
    where $($T: Scalar),+, V: Value {
      fn empty() -> Self {
        Self { root: Arc::new($Node::Data(vec![],vec![])) }
      }
      fn build<'a>(
        fields: Arc<SetupFields>,
        rows: &[(($(Coord<$T>),+),InsertValue<'a,($(Coord<$T>),+),V>)],
        next_tree: &mut TreeId,
        is_rm: bool,
      ) -> (Option<TreeRef<($(Coord<$T>),+)>>,HashMap<TreeId,Arc<Mutex<Self>>>) {
        $Branch::build(fields, rows, next_tree, is_rm)
      }
      fn list(&mut self) -> (Vec<(($(Coord<$T>),+),V)>,Vec<TreeRef<($(Coord<$T>),+)>>) {
        let mut cursors = vec![Arc::clone(&self.root)];
        let mut rows = vec![];
        let mut refs = vec![];
        while let Some(c) = cursors.pop() {
          match c.as_ref() {
            $Node::Branch(branch) => {
              for (_bitfield,b) in branch.intersections.iter() {
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
      fn query<S>(&mut self, trees: Arc<Mutex<TreeFile<S,Self,($(Coord<$T>),+),V>>>,
        bbox: &(($($T),+),($($T),+)), fields: Arc<SetupFields>,
        root_index: usize, root_id: TreeId,
      ) -> QStream<($(Coord<$T>),+),V> where S: RA {
        let istate = (
          bbox.clone(),
          vec![], // queue
          vec![(0usize,Arc::clone(&self.root))], // cursors
          vec![], // refs
          Arc::clone(&trees),
          Arc::clone(&fields)
        );
        Box::new(unfold(istate, async move |mut state| {
          let bbox = &state.0;
          let queue = &mut state.1;
          let cursors = &mut state.2;
          let refs = &mut state.3;
          let trees = &mut state.4;
          let fields = &mut state.5;
          loop {
            if let Some(q) = queue.pop() {
              return Some((Ok(q),state));
            }
            if cursors.is_empty() && !refs.is_empty() {
              match Arc::clone(trees).lock().await.get(&refs.pop().unwrap()).await {
                Err(e) => return Some((Err(e.into()),state)),
                Ok(t) => cursors.push((0usize,Arc::clone(&t.lock().await.root))),
              };
              continue;
            } else if cursors.is_empty() {
              if let Err(e) = fields.log(&format!["query end root_index={} root_id={}",
              root_index, root_id]).await {
                return Some((Err(e.into()),state));
              }
              return None;
            }
            let (level,c) = cursors.pop().unwrap();
            match c.as_ref() {
              $Node::Branch(branch) => {
                match level % $dim {
                  $($i => {
                    let pivots = branch.pivots.$i.as_ref().unwrap();

                    {
                      let mut matching: u32 = 0;
                      if &(bbox.0).$i <= pivots.first().unwrap() {
                        matching |= (1<<0);
                      }
                      let ranges = pivots.iter().zip(pivots.iter().skip(1));
                      for (i,(start,end)) in ranges.enumerate() {
                        if intersect_iv(start, end, &(bbox.0).$i, &(bbox.1).$i) {
                          matching |= (1<<i);
                          matching |= (1<<(i+1));
                        }
                      }
                      if &(bbox.1).$i >= pivots.last().unwrap() {
                        matching |= (1<<(pivots.len()-1));
                      }
                      for (bitfield,b) in branch.intersections.iter() {
                        if (matching & bitfield) > 0 {
                          cursors.push((level+1,Arc::clone(b)));
                        }
                      }
                    }

                    {
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
                    }
                  }),+
                  _ => panic!["unexpected level modulo dimension"]
                }
              },
              $Node::Data(data,rs) => {
                queue.extend(data.iter()
                  .filter(|pv| {
                    true $(&& intersect_coord(&(pv.0).$i, &(bbox.0).$i, &(bbox.1).$i))+
                  })
                  .map(|pv| { pv.clone() })
                  .collect::<Vec<_>>()
                );
                refs.extend(rs.iter()
                  .filter(|r| {
                    true $(&& intersect_coord(&r.bounds.$i, &(bbox.0).$i, &(bbox.1).$i))+
                  })
                  .map(|r| { r.id })
                  .collect::<Vec<TreeId>>()
                );
              },
            }
          }
        }))
      }
      async fn remove<S>(&mut self, xids: Arc<Mutex<HashMap<V::Id,($(Coord<$T>),+)>>>)
      -> (Option<(Vec<(($(Coord<$T>),+),V)>,Vec<TreeRef<($(Coord<$T>),+)>>)>,Vec<TreeId>) where S: RA {
        let (mut list, refs) = self.list();
        let len = list.len();
        let mut ids = xids.lock().await;
        list.drain_filter(|(_,v)| {
          let id = v.get_id();
          let x = ids.contains_key(&id);
          if x {
            ids.remove(&id);
          }
          x
        });
        let rs = refs.iter()
          .filter(|r| {
            for (_,p) in ids.iter() {
              if true $(&& intersect_coord_coord(&r.bounds.$i, &p.$i))+ {
                return true;
              }
            }
            false
          })
          .map(|r| { r.id })
          .collect::<Vec<TreeId>>();
        if len == list.len() {
          (None,rs)
        } else {
          (Some((list,refs)),rs)
        }
      }
    }

    fn $get_bounds<$($T),+,V,I>(mut indexes: I,
    rows: &[(($(Coord<$T>),+),InsertValue<'_,($(Coord<$T>),+),V>)]) -> ($(Coord<$T>),+)
    where $($T: Scalar),+, V: Value, I: Iterator<Item=usize> {
      let first = indexes.next().unwrap();
      let ibounds = (
        ($(
          match &(rows[first].0).$i {
            Coord::Scalar(x) => x.clone(),
            Coord::Interval(x,_) => x.clone(),
          }
        ),+),
        ($(
          match &(rows[first].0).$i {
            Coord::Scalar(x) => x.clone(),
            Coord::Interval(_,x) => x.clone(),
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

type CreateTrees<T> = HashMap<TreeId,Arc<Mutex<T>>>;

#[async_trait::async_trait]
pub trait Tree<P,V>: Send+Sync+ToBytes+FromBytes+CountBytes+std::fmt::Debug+'static
where P: Point, V: Value {
  fn empty() -> Self;
  fn build<'a>(
    fields: Arc<SetupFields>,
    rows: &[(P,InsertValue<'a,P,V>)],
    next_tree: &mut TreeId,
    is_rm: bool,
  ) -> (Option<TreeRef<P>>,CreateTrees<Self>) where Self: Sized;
  fn list(&mut self) -> (Vec<(P,V)>,Vec<TreeRef<P>>);
  fn query<S>(&mut self, trees: Arc<Mutex<TreeFile<S,Self,P,V>>>, bbox: &P::Bounds,
    fields: Arc<SetupFields>, root_index: usize, root_id: TreeId) -> QStream<P,V> where S: RA;
  async fn remove<S>(&mut self, ids: Arc<Mutex<HashMap<V::Id,P>>>)
    -> (Option<(Vec<(P,V)>,Vec<TreeRef<P>>)>,Vec<TreeId>) where S: RA;
}

pub struct Merge<'a,S,T,P,V>
where P: Point, V: Value, T: Tree<P,V>, S: RA {
  pub fields: Arc<SetupFields>,
  pub inserts: &'a [(&'a P,&'a V)],
  pub deletes: Arc<Vec<(P,V::Id)>>,
  pub inputs: Arc<Vec<TreeRef<P>>>,
  pub roots: Vec<Root<P>>,
  pub trees: Arc<Mutex<TreeFile<S,T,P,V>>>,
  pub next_tree: &'a mut TreeId,
  pub rebuild_depth: usize,
  pub error_if_missing: bool,
}

// return value: (tree, remove_trees, create_trees)
impl<'a,S,T,P,V> Merge<'a,S,T,P,V>
where P: Point, V: Value, T: Tree<P,V>, S: RA {
  pub async fn merge(&mut self)
  -> Result<(Option<TreeRef<P>>,Vec<TreeId>,HashMap<TreeId,Arc<Mutex<T>>>),Error> {
    self.remove().await?;

    let mut lists = vec![];
    let mut rm_trees = vec![];
    let mut rows: Vec<(P,InsertValue<'_,P,V>)> = vec![];
    let mut l_refs = Vec::with_capacity(self.inputs.len());
    l_refs.extend_from_slice(&self.inputs);

    for _ in 0..self.rebuild_depth {
      let mut trees = self.trees.lock().await;
      let bounds = l_refs.iter().map(|r| r.bounds.clone()).collect::<Vec<P>>();
      let intersecting = calc_overlap::<P>(&bounds);
      let mut n_refs = vec![];
      for (r,overlap) in l_refs.iter().zip(intersecting) {
        if overlap {
          let (list,xrefs) = trees.get(&r.id).await?.lock().await.list();
          lists.push(list);
          n_refs.extend(xrefs);
          rm_trees.push(r.id);
        } else {
          rows.push((r.bounds.clone(), InsertValue::Ref(r.clone())));
        }
      }
      l_refs = n_refs;
      if l_refs.is_empty() { break }
    }
    for r in l_refs.iter() {
      rows.push((r.bounds.clone(), InsertValue::Ref(r.clone())));
    }

    rows.extend(self.inserts.iter().map(|pv| {
      (pv.0.clone(),InsertValue::Value(pv.1))
    }).collect::<Vec<_>>());
    for list in lists.iter_mut() {
      rows.extend(list.iter().map(|pv| {
        (pv.0.clone(),InsertValue::Value(&pv.1))
      }).collect::<Vec<_>>());
    }
    //assert![rows.len()>0, "rows.len()={}. must be >0", rows.len()];
    let (tr, create_trees) = T::build(
      Arc::clone(&self.fields),
      &rows,
      &mut self.next_tree,
      false
    );
    Ok((tr, rm_trees, create_trees))
  }
  pub async fn remove(&mut self) -> Result<(),Error> {
    if self.deletes.is_empty() { return Ok(()) }
    let mut join = Join::new();
    let ids = {
      let mut map = HashMap::new();
      for d in self.deletes.iter() {
        map.insert(d.1.clone(), d.0.clone());
      }
      Arc::new(Mutex::new(map))
    };
    let fields = {
      let mut f = (*self.fields).clone();
      f.max_records = usize::MAX;
      f.max_depth = usize::MAX;
      Arc::new(f)
    };
    for ro in self.roots.iter() {
      if ro.is_none() { continue }
      let r = ro.as_ref().unwrap();
      // TODO: remove the delete when found
      let trees = Arc::clone(&self.trees);
      let xids = Arc::clone(&ids);
      let id = r.id;
      let xfields = Arc::clone(&fields);
      join.push(async move {
        let mut refs = vec![id];
        while !refs.is_empty() {
          let r = refs.pop().unwrap();
          let tm = trees.lock().await.get(&r).await?;
          let mut t = tm.lock().await;
          let (built,nrefs) = t.remove::<S>(
            Arc::clone(&xids),
          ).await;
          refs.extend(nrefs);
          if let Some((list,refs)) = built {
            let mut rows = Vec::with_capacity(list.len() + refs.len());
            rows.extend(list.iter().map(|(p,v)| {
              (p.clone(),InsertValue::Value(v))
            }).collect::<Vec<_>>());
            rows.extend(refs.iter().map(|r| {
              (r.bounds.clone(),InsertValue::Ref(r.clone()))
            }).collect::<Vec<_>>());
            let mut next_tree = r;
            if rows.is_empty() {
              trees.lock().await.put(&r, Arc::new(Mutex::new(T::empty()))).await?;
            } else {
              let (tr, create_trees) = T::build(
                Arc::clone(&xfields),
                &rows,
                &mut next_tree,
                true
              );
              let tr_id = tr.map(|x| x.id);
              assert![tr_id == Some(r),
                "unexpected id constructing replacement tree for remove(). \
                expected: {:?}, received: {:?}", Some(r), tr_id
              ];
              assert![create_trees.len() == 1, "unexpected external sub-trees during remove()"];
              for (r,t) in create_trees {
                trees.lock().await.put(&r, t).await?;
              }
            }
          }
        }
        Ok(())
      });
    }
    join.try_join().await?;
    if self.error_if_missing {
      let xids = ids.lock().await;
      if !xids.is_empty() {
        return EyrosErrorKind::RemoveIdsMissing {
          ids: xids.keys().map(|id| format!["{:?}",id]).collect()
        }.raise();
      }
    }
    Ok(())
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
    amin.clone()/2.into() + bmin.clone()/2.into()
  } else if !is_min && intersect_iv(amin, amax, bmin, bmax) {
    amax.clone()/2.into() + bmax.clone()/2.into()
  } else {
    amax.clone()/2.into() + bmin.clone()/2.into()
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
  match x {
    Coord::Scalar(a) => cmp_min(a,r),
    Coord::Interval(a,_) => cmp_min(a,r),
  }
}

fn coord_max<X>(x: &Coord<X>, r: &X) -> X where X: Scalar {
  match x {
    Coord::Scalar(a) => cmp_max(a,r),
    Coord::Interval(_,a) => cmp_max(a,r),
  }
}

fn cmp_min<X>(a: &X, b: &X) -> X where X: Scalar {
  (if a < b { a } else { b }).clone()
}
fn cmp_max<X>(a: &X, b: &X) -> X where X: Scalar {
  (if a > b { a } else { b }).clone()
}
