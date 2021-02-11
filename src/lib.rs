#![feature(async_closure,iter_partition_in_place,drain_filter)]
mod store;
pub use store::Storage;
#[doc(hidden)] pub use store::FileStore;
mod setup;
pub use setup::{Setup,SetupFields};
mod tree;
pub use tree::{Tree,TreeRef,TreeId,Merge};
mod bytes;
mod query;
pub use query::QueryStream;
mod unfold;
mod tree_file;
use tree_file::TreeFile;
mod get_id;
pub use get_id::{GetId,Id};

use async_std::{sync::{Arc,Mutex}};
use random_access_storage::RandomAccess;
use desert::{ToBytes,FromBytes,CountBytes};
use core::ops::{Add,Div};

pub type Error = Box<dyn std::error::Error+Sync+Send>;

pub trait Value: Clone+core::fmt::Debug+Send+Sync+'static+PartialEq
  +ToBytes+CountBytes+FromBytes {}
pub trait Scalar: Clone+PartialOrd+From<u8>+core::fmt::Debug
  +ToBytes+CountBytes+FromBytes
  +Value+Add<Output=Self>+Div<Output=Self> {}
impl Value for f32 {}
impl Value for f64 {}
impl Value for u8 {}
impl Value for u16 {}
impl Value for u32 {}
impl Value for u64 {}
impl Value for i16 {}
impl Value for i32 {}
impl Value for i64 {}
impl<T> Value for Vec<T> where T: Value {}
impl Scalar for f32 {}
impl Scalar for f64 {}
impl Scalar for u8 {}
impl Scalar for u16 {}
impl Scalar for u32 {}
impl Scalar for u64 {}
impl Scalar for i16 {}
impl Scalar for i32 {}
impl Scalar for i64 {}

pub trait RA: RandomAccess<Error=Error>+Unpin+Send+Sync+std::fmt::Debug+'static {}
impl<S> RA for S where S: RandomAccess<Error=Error>+Unpin+Send+Sync+std::fmt::Debug+'static {}

#[derive(Debug,Clone,PartialEq,PartialOrd)]
pub enum Coord<X> where X: Scalar {
  Scalar(X),
  Interval(X,X)
}

#[async_trait::async_trait]
pub trait Point: 'static+Overlap+Clone+Send+Sync+core::fmt::Debug {
  type Bounds: Clone+Send+Sync+core::fmt::Debug+ToBytes+FromBytes+CountBytes+Overlap;
  fn to_bounds(&self) -> Result<Self::Bounds,Error>;
  fn bounds_to_point(bounds: &Self::Bounds) -> Self;
}

pub trait Overlap {
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
              return Err(Box::new(failure::err_msg("scalar found in bounds").compat()));
            },
            Coord::Interval(min,_) => min.clone(),
          }),+),
          ($(match &self.$i {
            Coord::Scalar(_) => {
              return Err(Box::new(failure::err_msg("scalar found in bounds").compat()));
            },
            Coord::Interval(_,max) => max.clone(),
          }),+),
        ))
      }
      fn bounds_to_point(bounds: &Self::Bounds) -> Self {
        ($(Coord::Interval((bounds.0).$i.clone(),(bounds.1).$i.clone())),+)
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

#[derive(Debug,Clone)]
pub enum Row<P,V,X> where P: Point, V: Value, X: Id {
  Insert(P,V),
  Delete(P,X)
}

pub type Root<P> = Option<tree::TreeRef<P>>;
#[derive(Debug,Clone)]
pub struct Meta<P> where P: Point {
  pub roots: Vec<Root<P>>,
  pub next_tree: TreeId,
}

pub struct DB<S,T,P,V,X>
where S: RA, P: Point, V: Value+GetId<X>, T: Tree<P,V,X>, X: Id {
  pub storage: Arc<Mutex<Box<dyn Storage<S>>>>,
  pub fields: Arc<SetupFields>,
  pub meta_store: Arc<Mutex<S>>,
  pub meta: Meta<P>,
  pub trees: Arc<Mutex<TreeFile<S,T,P,V,X>>>,
  _marker: std::marker::PhantomData<X>,
}

impl<S,T,P,V,X> DB<S,T,P,V,X>
where S: RA, P: Point, V: Value+GetId<X>, T: Tree<P,V,X>, X: Id {
  pub async fn open_from_setup(setup: Setup<S>) -> Result<Self,Error> {
    let mut meta_store = setup.storage.lock().await.open("meta").await?;
    let meta = match meta_store.len().await? {
      0 => Meta { roots: vec![], next_tree: 0 },
      n => Meta::from_bytes(&meta_store.read(0,n).await?)?.1,
    };
    let fields = Arc::new(setup.fields);
    let trees = TreeFile::new(Arc::clone(&fields), Arc::clone(&setup.storage));
    Ok(Self {
      storage: Arc::clone(&setup.storage),
      fields,
      meta_store: Arc::new(Mutex::new(meta_store)),
      meta,
      trees: Arc::new(Mutex::new(trees)),
      _marker: std::marker::PhantomData,
    })
  }
  pub async fn batch(&mut self, rows: &[Row<P,V,X>]) -> Result<(),Error> {
    self.batch_with_rebuild_depth(self.fields.rebuild_depth, rows).await
  }
  pub async fn batch_with_rebuild_depth(&mut self, rebuild_depth: usize, rows: &[Row<P,V,X>])
  -> Result<(),Error> {
    if rows.is_empty() { return Ok(()) }
    let inserts: Vec<(&P,&V)> = rows.iter()
      .map(|row| match row {
        Row::Insert(p,v) => Some((p,v)),
        _ => None
      })
      .filter(|row| !row.is_none())
      .map(|x| x.unwrap())
      .collect();
    let deletes: Vec<(P,X)> = rows.iter()
      .map(|row| match row {
        Row::Delete(p,x) => Some((p.clone(),x.clone())),
        _ => None
      })
      .filter(|row| !row.is_none())
      .map(|x| x.unwrap())
      .collect();

    let merge_trees = Arc::new(
      self.meta.roots.iter()
        .take_while(|r| r.is_some())
        .map(|r| r.as_ref().unwrap().clone())
        .collect::<Vec<TreeRef<P>>>()
    );
    let mut m = Merge {
      fields: Arc::clone(&self.fields),
      inserts: inserts.as_slice(),
      deletes: Arc::new(deletes),
      inputs: Arc::clone(&merge_trees),
      roots: self.meta.roots.clone(),
      trees: Arc::clone(&self.trees),
      next_tree: &mut self.meta.next_tree,
      rebuild_depth,
    };
    if inserts.is_empty() {
      m.remove().await?;
      return Ok(());
    }
    let (tr,t,rm_trees,create_trees) = m.merge().await?;
    //eprintln!["root {}={} bytes", t.count_bytes(), t.to_bytes()?.len()];
    let trees = &mut self.trees.lock().await;
    for r in rm_trees.iter() {
      trees.remove(r).await;
    }
    for (r,t) in create_trees.iter() {
      trees.put(r,Arc::clone(t)).await;
    }
    trees.put(&tr.id, Arc::new(Mutex::new(t))).await;
    for i in 0..merge_trees.len() {
      if i < self.meta.roots.len() {
        self.meta.roots[i] = None;
      } else {
        self.meta.roots.push(None);
      }
    }
    if merge_trees.len() < self.meta.roots.len() {
      self.meta.roots[merge_trees.len()] = Some(tr);
    } else {
      self.meta.roots.push(Some(tr));
    }
    Ok(())
  }
  pub async fn optimize(&mut self, rebuild_depth: usize) -> Result<(),Error> {
    let merge_trees = Arc::new(
      self.meta.roots.iter()
        .filter(|r| r.is_some())
        .map(|r| r.as_ref().unwrap().clone())
        .collect::<Vec<TreeRef<P>>>()
    );
    let mut m = Merge {
      fields: Arc::clone(&self.fields),
      inserts: &vec![],
      deletes: Arc::new(vec![]),
      inputs: Arc::clone(&merge_trees),
      roots: self.meta.roots.clone(),
      trees: Arc::clone(&self.trees),
      next_tree: &mut self.meta.next_tree,
      rebuild_depth,
    };
    let (tr,t,rm_trees,create_trees) = m.merge().await?;
    let trees = &mut self.trees.lock().await;
    for r in rm_trees.iter() {
      trees.remove(r).await;
    }
    for (r,t) in create_trees.iter() {
      trees.put(r,Arc::clone(t)).await;
    }
    trees.put(&tr.id, Arc::new(Mutex::new(t))).await;
    self.meta.roots.clear();
    self.meta.roots.push(Some(tr));
    Ok(())
  }
  pub async fn sync(&mut self) -> Result<(),Error> {
    self.trees.lock().await.sync().await?;
    let rbytes = self.meta.to_bytes()?;
    self.meta_store.lock().await.write(0, &rbytes).await?;
    Ok(())
  }
  pub async fn query(&mut self, bbox: &P::Bounds) -> Result<query::QStream<P,V>,Error> {
    let mut queries = vec![];
    for root in self.meta.roots.iter() {
      if let Some(r) = root {
        let mut trees = self.trees.lock().await;
        let t = trees.get(&r.id).await?;
        queries.push(t.lock().await.query(Arc::clone(&self.trees), bbox));
      }
    }
    <QueryStream<P,V>>::from_queries(queries)
  }
}
