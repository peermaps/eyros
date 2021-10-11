#![warn(clippy::future_not_send)]
#![feature(async_closure, iter_partition_in_place, drain_filter, backtrace, available_concurrency)]
#![doc=include_str!("../readme.md")]
mod error;
pub use error::{EyrosError,EyrosErrorKind,Error};
mod store;
pub use store::Storage;
#[cfg(not(feature="wasm"))] #[doc(hidden)] pub use store::FileStore;
mod setup;
pub use setup::{Setup,SetupFields};
pub mod tree;
#[doc(hidden)] pub use tree::{Tree,TreeRef,TreeId,Merge};
mod bytes;
mod query;
pub use query::QTrace;
mod unfold;
mod tree_file;
use tree_file::TreeFile;
mod value;
pub use value::Value;
#[cfg(feature="wasm")]
mod wasm;
mod batch;
pub use batch::{BatchFields,BatchOptions};
mod debugger;
pub use debugger::Debugger;

use async_std::{sync::{Arc,Mutex,RwLock}};
use random_access_storage::RandomAccess;
use desert::{ToBytes,FromBytes,CountBytes};
use core::ops::{Add,Div};
use std::fmt::Debug;
use std::collections::VecDeque;

/// All coordinate values must implement this collection of traits.
pub trait Scalar: Clone+PartialOrd+From<u8>+Debug
  +Send+Sync+'static+PartialEq
  +ToBytes+CountBytes+FromBytes
  +Add<Output=Self>+Div<Output=Self> {}
impl Scalar for f32 {}
impl Scalar for f64 {}
impl Scalar for u8 {}
impl Scalar for u16 {}
impl Scalar for u32 {}
impl Scalar for u64 {}
impl Scalar for i16 {}
impl Scalar for i32 {}
impl Scalar for i64 {}

#[doc(hidden)] pub trait RA: RandomAccess<Error=Error>+Unpin+Send+Sync+'static {}
impl<S> RA for S where S: RandomAccess<Error=Error>+Unpin+Send+Sync+'static {}

/// The `Coord` enum represents the value for a dimension instead of a `Point` tuple.
/// Use `Coord::Scalar(x)` to represent a single value and `Coord::Interval(min,max)`
/// to represent a range of values from `min` to `max`, inclusive.
#[derive(Debug,Clone,PartialEq,PartialOrd)]
pub enum Coord<X> where X: Scalar {
  Scalar(X),
  Interval(X,X)
}

/// The `Point` trait represents the geometric coordinates of a feature.
/// Each `Point` defines a `Bounds` that represents how to express bounding boxes.
/// `Points` and `Bounds` are converted between each other with the `to_bounds()` and
/// `from_bounds()` methods.
#[async_trait::async_trait]
pub trait Point: 'static+Overlap+Clone+Send+Sync+Debug{
  type Bounds: Clone+Send+Sync+Debug+ToBytes+FromBytes+CountBytes+Overlap;
  /// Convert to a `Bounds`, which may not be possible.
  fn to_bounds(&self) -> Result<Self::Bounds,Error>;
  /// Convert a `Bounds` into a `Point`.
  fn from_bounds(bounds: &Self::Bounds) -> Self;
  /// Return an Error when the current `Point` is invalid.
  /// For example, for an interval `(min,max)` it may be that `min > max`.
  fn check(&self) -> Result<(),Error>;
}

/// Intersection tests used by `Point` and `Point::Bounds`.
pub trait Overlap {
  /// Return whether two features intersect.
  fn overlap(&self, other: &Self) -> bool;
}

macro_rules! impl_point {
  ($Tree:ident,$open_from_path:ident,($($T:tt),+),($($i:tt),+)) => {
    pub use tree::$Tree;
    #[cfg(not(feature="wasm"))] pub use store::$open_from_path;
    #[async_trait::async_trait]
    impl<$($T),+> Point for ($(Coord<$T>),+) where $($T: Scalar),+ {
      type Bounds = (($($T),+),($($T),+));
      fn to_bounds(&self) -> Result<Self::Bounds,Error> {
        Ok((
          ($(match &self.$i {
            Coord::Scalar(_) => {
              return EyrosErrorKind::ScalarInBounds {}.raise();
            },
            Coord::Interval(min,_) => min.clone(),
          }),+),
          ($(match &self.$i {
            Coord::Scalar(_) => {
              return EyrosErrorKind::ScalarInBounds {}.raise();
            },
            Coord::Interval(_,max) => max.clone(),
          }),+),
        ))
      }
      fn from_bounds(bounds: &Self::Bounds) -> Self {
        ($(Coord::Interval((bounds.0).$i.clone(),(bounds.1).$i.clone())),+)
      }
      fn check(&self) -> Result<(),Error> {
        $(match &(self.$i) {
          Coord::Interval(min,max) => {
            match min.partial_cmp(&max) {
              None | Some(std::cmp::Ordering::Greater) => {
                return EyrosErrorKind::IntervalSides {
                  dimension: $i,
                  min: format!["{:?}", min],
                  max: format!["{:?}", max],
                }.raise();
              },
              _ => {},
            }
          },
          _ => {}
        };)+
        Ok(())
      }
    }
  }
}

#[cfg(feature="2d")] impl_point![Tree2,open_from_path2,(P0,P1),(0,1)];
#[cfg(feature="3d")] impl_point![Tree3,open_from_path3,(P0,P1,P2),(0,1,2)];
#[cfg(feature="4d")] impl_point![Tree4,open_from_path4,(P0,P1,P2,P3),(0,1,2,3)];
#[cfg(feature="5d")] impl_point![Tree5,open_from_path5,(P0,P1,P2,P3,P4),(0,1,2,3,4)];
#[cfg(feature="6d")] impl_point![Tree6,open_from_path6,(P0,P1,P2,P3,P4,P5),(0,1,2,3,4,5)];
#[cfg(feature="7d")] impl_point![Tree7,open_from_path7,(P0,P1,P2,P3,P4,P5,P6),(0,1,2,3,4,5,6)];
#[cfg(feature="8d")] impl_point![Tree8,open_from_path8,(P0,P1,P2,P3,P4,P5,P6,P7),(0,1,2,3,4,5,6,7)];

/// Enum container for batch operations on the database.
#[derive(Debug,Clone)]
pub enum Row<P,V> where P: Point, V: Value {
  Insert(P,V),
  Delete(P,V::Id)
}

#[doc(hidden)]
pub type Root<P> = Option<tree::TreeRef<P>>;
#[doc(hidden)]
#[derive(Debug,Clone)]
pub struct Meta<P> where P: Point {
  pub roots: Vec<Root<P>>,
  pub next_tree: TreeId,
}

/// Top-level database API.
pub struct DB<S,T,P,V>
where S: RA, P: Point, V: Value, T: Tree<P,V> {
  pub storage: Arc<Mutex<Box<dyn Storage<S>>>>,
  pub fields: Arc<SetupFields>,
  pub meta_store: Arc<Mutex<S>>,
  pub meta: Arc<RwLock<Meta<P>>>,
  pub trees: Arc<TreeFile<S,T,P,V>>,
}

impl<S,P,V,T> Clone for DB<S,T,P,V>
where S: RA, P: Point, V: Value, T: Tree<P,V> {
  fn clone(&self) -> Self {
    Self {
      storage: self.storage.clone(),
      fields: self.fields.clone(),
      meta_store: self.meta_store.clone(),
      meta: self.meta.clone(),
      trees: self.trees.clone(),
    }
  }
}

impl<S,T,P,V> DB<S,T,P,V>
where S: RA, P: Point, V: Value, T: Tree<P,V> {
  /// Create a database instance from `setup`, a configuration builder.
  ///
  /// ```rust,no_run
  /// # use eyros::{DB,Coord,Tree2,Setup};
  /// # use std::path::PathBuf;
  /// # #[async_std::main]
  /// # async fn main () -> Result<(),Box<dyn std::error::Error+Sync+Send>> {
  /// # type P = (Coord<f32>,Coord<f32>);
  /// # type V = u32;
  /// # type T = Tree2<f32,f32,V>;
  /// let mut db: DB<_,T,P,V> = DB::open_from_setup(
  ///   Setup::from_path(&PathBuf::from("/tmp/eyros-db/"))
  ///     .branch_factor(5)
  ///     .max_depth(8)
  ///     .max_records(20_000)
  /// ).await?;
  /// # Ok(()) }
  /// ```
  ///
  /// You can also use `Setup`'s `.build()?` method to get a `DB` instance:
  ///
  /// ```rust,no_run
  /// use eyros::{DB,Coord,Tree2,Setup};
  /// # use std::path::PathBuf;
  ///
  /// # type P = (Coord<f32>,Coord<f32>);
  /// # type V = u32;
  /// # type T = Tree2<f32,f32,V>;
  /// # #[async_std::main]
  /// # async fn main () -> Result<(),Box<dyn std::error::Error+Sync+Send>> {
  /// let mut db: DB<_,T,P,V> = Setup::from_path(&PathBuf::from("/tmp/eyros-db/"))
  ///   .branch_factor(5)
  ///   .max_depth(8)
  ///   .max_records(20_000)
  ///   .build()
  ///   .await?;
  /// # Ok(()) }
  /// ```
  ///
  /// Always open a database with the same types. There are no runtime checks yet
  /// to ensure a database is opened with the same types that it was created with.
  /// It's fine to change the Setup settings on a previously-created database,
  /// but those settings will only affect new operations.
  pub async fn open_from_setup(setup: Setup<S>) -> Result<Self,Error> {
    let fields = Arc::new(setup.fields);
    fields.log("opening db").await?;
    let mut meta_store = setup.storage.lock().await.open("meta").await?;
    let meta = match meta_store.len().await? {
      0 => {
        fields.log("no existing db found. initialized new meta").await?;
        Meta { roots: vec![], next_tree: 0 }
      },
      n => {
        fields.log(&format!["existing db found. reading {} bytes from meta store", n]).await?;
        Meta::from_bytes(&meta_store.read(0,n).await?)?.1
      },
    };
    let trees = TreeFile::new(Arc::clone(&fields), Arc::clone(&setup.storage));
    Ok(Self {
      storage: Arc::clone(&setup.storage),
      fields,
      meta_store: Arc::new(Mutex::new(meta_store)),
      meta: Arc::new(RwLock::new(meta)),
      trees: Arc::new(trees),
    })
  }
  /// Create a database instance from `storage`, an interface for reading, writing, and removing
  /// files.
  pub async fn open_from_storage(storage: Box<dyn Storage<S>>) -> Result<Self,Error> {
    Setup::from_storage(storage).build().await
  }
  /// Write a collection of updates to the database with default options.
  /// This does not sync the changes to disk: call `sync()` for that.
  /// Each update can be a `Row::Insert(point,value)` or a `Row::Delete(point,id)`
  /// (where the type of `id` is defined in `Value::Id`). For deletes, you need not
  /// have exactly the same `point` as the original record, only a point that will
  /// intersect it.
  pub async fn batch(&mut self, rows: &[Row<P,V>]) -> Result<(),Error> {
    let opts = BatchOptions::new().rebuild_depth(self.fields.rebuild_depth);
    self.batch_with_options(rows, &opts).await
  }
  /// Perform a batch update with an explicit rebuild depth to override the rebuild depth defined in
  /// the `Setup`. A greater rebuild depth trades write performance for better query performance,
  /// which you can also obtain by calling `optimize()`.
  pub async fn batch_with_rebuild_depth(&mut self, rebuild_depth: usize, rows: &[Row<P,V>])
  -> Result<(),Error> {
    let opts = BatchOptions::new().rebuild_depth(rebuild_depth);
    self.batch_with_options(rows, &opts).await
  }
  /// Perform a batch update with explicit batch options.
  pub async fn batch_with_options(&mut self, rows: &[Row<P,V>], opts: &BatchOptions) -> Result<(),Error> {
    if rows.is_empty() { return Ok(()) }
    for row in rows.iter() {
      match row {
        Row::Insert(p,_) => p.check()?,
        Row::Delete(p,_) => p.check()?,
      }
    }
    let inserts: Vec<(&P,&V)> = rows.iter()
      .map(|row| match row {
        Row::Insert(p,v) => Some((p,v)),
        _ => None
      })
      .filter(|row| !row.is_none())
      .map(|x| x.unwrap())
      .collect();
    let deletes: Vec<(P,V::Id)> = rows.iter()
      .map(|row| match row {
        Row::Delete(p,x) => Some((p.clone(),x.clone())),
        _ => None
      })
      .filter(|row| !row.is_none())
      .map(|x| x.unwrap())
      .collect();

    let mut meta = self.meta.write().await;
    let merge_trees = Arc::new(
      meta.roots.iter()
        .take_while(|r| r.is_some())
        .map(|r| r.as_ref().unwrap().clone())
        .collect::<Vec<TreeRef<P>>>()
    );
    let mut m = Merge {
      fields: Arc::clone(&self.fields),
      inserts: inserts.as_slice(),
      deletes: Arc::new(deletes),
      inputs: merge_trees.clone(),
      roots: meta.roots.clone(),
      trees: self.trees.clone(),
      next_tree: &mut meta.next_tree,
      rebuild_depth: opts.fields.rebuild_depth,
      error_if_missing: opts.fields.error_if_missing,
    };
    if inserts.is_empty() {
      m.remove().await?;
      return Ok(());
    }
    let (tr,rm_trees,create_trees) = m.merge().await?;
    //eprintln!["root {}={} bytes", t.count_bytes(), t.to_bytes()?.len()];
    for r in rm_trees.iter() {
      self.trees.remove(r).await?;
    }
    for (r,t) in create_trees.iter() {
      self.trees.put(r,Arc::clone(t)).await?;
    }
    for i in 0..merge_trees.len() {
      if i < meta.roots.len() {
        meta.roots[i] = None;
      } else {
        meta.roots.push(None);
      }
    }
    if merge_trees.len() < meta.roots.len() {
      meta.roots[merge_trees.len()] = tr;
    } else {
      meta.roots.push(tr);
    }
    Ok(())
  }
  /// Improve query performance by rebuilding the first `rebuild_depth` levels of the tree.
  /// A higher value for `rebuild_depth` will use more memory, as the trees are read into memory
  /// during rebuilding and not written back out again until `sync()` is called.
  pub async fn optimize(&mut self, rebuild_depth: usize) -> Result<(),Error> {
    let mut refs = VecDeque::new();
    for root in self.meta.read().await.roots.iter() {
      if let Some(r) = root {
        refs.push_back(r.clone());
      }
    }
    while let Some(tree_ref) = refs.pop_front() {
      self.optimize_tree(&tree_ref, rebuild_depth).await?;
      self.sync().await?;
      //refs.extend(self.optimize_get_depth_refs(tree_ref.id, rebuild_depth).await?);
      let rs = self.optimize_get_depth_refs(tree_ref.id, rebuild_depth).await?;
      refs.extend(rs);
    }
    Ok(())
  }

  async fn optimize_tree(&mut self, tree_ref: &TreeRef<P>, rebuild_depth: usize) -> Result<(),Error> {
    // copy tree_ref to a new tree slot and run merge on that
    // so that refs to this tree still work
    let mut n_ref = tree_ref.clone();
    let mut meta = self.meta.write().await;
    n_ref.id = meta.next_tree;
    meta.next_tree += 1;
    {
      let t = self.trees.get(&tree_ref.id).await?;
      self.trees.remove(&tree_ref.id).await?;
      self.trees.put(&n_ref.id, t).await?;
    }

    let mut m = Merge {
      fields: Arc::clone(&self.fields),
      inserts: &[],
      deletes: Arc::new(vec![]),
      inputs: Arc::new(vec![n_ref]),
      roots: vec![],
      trees: self.trees.clone(),
      next_tree: &mut meta.next_tree,
      rebuild_depth,
      error_if_missing: true,
    };
    let (tr,rm_trees,create_trees) = m.merge().await?;
    let tr_id = tr.map(|r| r.id);
    for r in rm_trees.iter() {
      self.trees.remove(r).await?;
    }
    for (r,t) in create_trees.iter() {
      if tr_id == Some(*r) {
        self.trees.put(&tree_ref.id,Arc::clone(t)).await?;
      } else {
        self.trees.put(r,Arc::clone(t)).await?;
      }
    }
    Ok(())
  }

  async fn optimize_get_depth_refs(
    &mut self, tree_id: TreeId, rebuild_depth: usize
  ) -> Result<Vec<TreeRef<P>>,Error> {
    let mut cursors = VecDeque::new();
    cursors.push_back((0,tree_id));
    let mut depth_refs = vec![];
    while let Some((level,id)) = cursors.pop_front() {
      let tree = self.trees.get(&id).await?;
      let refs = tree.lock().await.list_refs();
      if level+1 < rebuild_depth {
        cursors.extend(refs.iter().map(|r| (level+1,r.id)).collect::<Vec<_>>());
      } else {
        depth_refs.extend(refs);
      }
    }
    Ok(depth_refs)
  }

  /// Write the changes made to the database to file storage.
  pub async fn sync(&mut self) -> Result<(),Error> {
    self.trees.sync().await?;
    let rbytes = self.meta.read().await.to_bytes()?;
    self.meta_store.lock().await.write(0, &rbytes).await?;
    Ok(())
  }
  /// Query the database for every feature that intersects `bbox`. Results are provided as a
  /// readable stream of `(point,value)` records.
  /// Queries hold a lock on the database, so you probably shouldn't leave them open for very long
  /// if you need to make more writes.
  pub async fn query(&mut self, bbox: &P::Bounds) -> Result<query::QStream<P,V>,Error> {
    self.fields.log(&format!["query bbox={:?}", bbox]).await?;
    let mut queries = vec![];
    for (i,root) in self.meta.read().await.roots.iter().enumerate() {
      if let Some(r) = root {
        self.fields.log(&format!["query root i={} id={}", i, r.id]).await?;
        let t = self.trees.get(&r.id).await?;
        queries.push(t.lock().await.query(
          self.trees.clone(), bbox, Arc::clone(&self.fields), i, r
        ));
      }
    }
    query::from_queries(queries)
  }
  pub async fn query_trace(
    &mut self,
    bbox: &P::Bounds,
    trace: Box<dyn query::QTrace<P>>,
  ) -> Result<query::QStream<P,V>,Error> {
    self.fields.log(&format!["query bbox={:?}", bbox]).await?;
    let mut queries = vec![];
    let trace_r = Arc::new(Mutex::new(trace));
    for (i,root) in self.meta.read().await.roots.iter().enumerate() {
      if let Some(r) = root {
        self.fields.log(&format!["query root i={} id={}", i, r.id]).await?;
        let t = self.trees.get(&r.id).await?;
        queries.push(t.lock().await.query_trace(
          self.trees.clone(),
          bbox,
          Arc::clone(&self.fields),
          i,
          r,
          Some(trace_r.clone()),
        ));
      }
    }
    query::from_queries(queries)
  }
}
