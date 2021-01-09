#![feature(async_closure,iter_partition_in_place)]
mod store;
pub use store::Storage;
#[doc(hidden)] pub use store::FileStore;
mod setup;
pub use setup::{Setup,SetupFields};
mod tree;
pub use tree::{Tree,TreeRef,TreeId};
mod bytes;
mod query;
pub use query::QueryStream;
mod unfold;
use std::collections::HashMap;

use async_std::{sync::{Arc,Mutex}};
use random_access_storage::RandomAccess;
use desert::{ToBytes,FromBytes,CountBytes};
use core::ops::{Add,Div};

pub type Error = Box<dyn std::error::Error+Sync+Send>;
pub type Location = (u64,u32);

pub trait Value: Clone+core::fmt::Debug+Send+Sync+'static
  +ToBytes+CountBytes+FromBytes {}
pub trait Scalar: Clone+PartialOrd+From<u8>+core::fmt::Debug
  +ToBytes+CountBytes+FromBytes
  +Value+Add<Output=Self>+Div<Output=Self> {}
impl Value for f32 {}
impl Value for f64 {}
impl Scalar for f32 {}
impl Scalar for f64 {}
impl Value for u8 {}
impl Value for u16 {}
impl Value for u32 {}
impl Value for u64 {}
impl<T> Value for Vec<T> where T: Value {}

#[derive(Debug,Clone)]
pub enum Coord<X> where X: Scalar {
  Scalar(X),
  Interval(X,X)
}

#[async_trait::async_trait]
pub trait Point: 'static+Overlap+Clone {
  type Bounds: Clone+Send+Sync+core::fmt::Debug+ToBytes+FromBytes+CountBytes+Overlap;
  async fn batch<S,T,V>(db: &mut DB<S,T,Self,V>, rows: &[Row<Self,V>]) -> Result<(),Error>
    where S: RandomAccess<Error=Error>+Unpin+Send+Sync, V: Value, Self: Sized, T: Tree<Self,V>;
}

pub trait Overlap {
  fn overlap(&self, other: &Self) -> bool;
}

macro_rules! impl_point {
  ($Tree:ident,$open_from_path:ident,$($T:tt),+) => {
    pub use tree::$Tree;
    #[cfg(not(feature="wasm"))] pub use store::$open_from_path;
    #[async_trait::async_trait]
    impl<$($T),+> Point for ($(Coord<$T>),+) where $($T: Scalar),+ {
      type Bounds = (($($T),+),($($T),+));
      async fn batch<S,T,V>(db: &mut DB<S,T,Self,V>, rows: &[Row<Self,V>]) -> Result<(),Error>
      where S: RandomAccess<Error=Error>+Unpin+Send+Sync, V: Value, T: Tree<($(Coord<$T>),+),V> {
        let inserts: Vec<(&Self,&V)> = rows.iter()
          .map(|row| match row {
            Row::Insert(p,v) => Some((p,v)),
            _ => None
          })
          .filter(|row| !row.is_none())
          .map(|x| x.unwrap())
          .collect();

        let trees = &mut db.trees;
        let merge_trees = db.roots.iter()
          .take_while(|r| r.is_some())
          .map(|r| r.as_ref().unwrap().clone())
          .collect::<Vec<TreeRef<Self>>>();
        let (tr,t,rm_trees,create_trees) = tree::merge(
          9, 6, inserts.as_slice(), merge_trees.as_slice(), trees, &mut db.next_tree
        ).await;
        //eprintln!["root {}={} bytes", t.count_bytes(), t.to_bytes()?.len()];
        rm_trees.iter().for_each(|r| {
          trees.remove(r);
        });
        create_trees.iter().for_each(|(r,t)| {
          trees.insert(*r,Arc::clone(t));
        });
        trees.insert(tr.id, Arc::new(Mutex::new(t)));
        for i in 0..merge_trees.len() {
          if i < db.roots.len() {
            db.roots[i] = None;
          } else {
            db.roots.push(None);
          }
        }
        if merge_trees.len() < db.roots.len() {
          db.roots[merge_trees.len()] = Some(tr);
        } else {
          db.roots.push(Some(tr));
        }
        Ok(())
      }
    }
  }
}

#[cfg(feature="2d")] impl_point![Tree2,open_from_path2,P0,P1];
#[cfg(feature="3d")] impl_point![Tree3,open_from_path3,P0,P1,P2];
#[cfg(feature="4d")] impl_point![Tree4,open_from_path4,P0,P1,P2,P3];
#[cfg(feature="5d")] impl_point![Tree5,open_from_path5,P0,P1,P2,P3,P4];
#[cfg(feature="6d")] impl_point![Tree6,open_from_path6,P0,P1,P2,P3,P4,P5];
#[cfg(feature="7d")] impl_point![Tree7,open_from_path7,P0,P1,P2,P3,P4,P5,P6];
#[cfg(feature="8d")] impl_point![Tree8,open_from_path8,P0,P1,P2,P3,P4,P5,P6,P7];

pub enum Row<P,V> where P: Point, V: Value {
  Insert(P,V),
  Delete(Location)
}

pub struct DB<S,T,P,V> where S: RandomAccess<Error=Error>+Unpin+Send+Sync,
P: Point, V: Value, T: Tree<P,V> {
  pub storage: Arc<Mutex<Box<dyn Storage<S>+Unpin+Send+Sync>>>,
  pub meta: Arc<Mutex<S>>,
  pub fields: SetupFields,
  pub roots: Vec<Option<tree::TreeRef<P>>>,
  pub trees: HashMap<tree::TreeId,Arc<Mutex<T>>>,
  pub next_tree: TreeId,
  _point: std::marker::PhantomData<P>,
  _value: std::marker::PhantomData<V>,
}

impl<S,T,P,V> DB<S,T,P,V> where S: RandomAccess<Error=Error>+Unpin+Send+Sync+'static,
P: Point, V: Value, T: Tree<P,V> {
  pub async fn open_from_setup(setup: Setup<S>) -> Result<Self,Error> {
    let meta = setup.storage.lock().await.open("meta").await?;
    Ok(Self {
      storage: Arc::clone(&setup.storage),
      meta: Arc::new(Mutex::new(meta)),
      fields: setup.fields,
      roots: vec![],
      next_tree: 0,
      trees: HashMap::new(),
      _point: std::marker::PhantomData,
      _value: std::marker::PhantomData,
    })
  }
  pub async fn batch(&mut self, rows: &[Row<P,V>]) -> Result<(),Error> {
    P::batch(self, rows).await
  }
  pub async fn query(&mut self, bbox: &P::Bounds) -> Result<query::QStream<P,V>,Error> {
    let mut queries = vec![];
    for root in self.roots.iter() {
      if let Some(r) = root {
        let t = self.trees.get(&r.id).unwrap();
        queries.push(t.lock().await.query(Arc::clone(&self.storage), bbox));
      }
    }
    <QueryStream<P,V>>::from_queries(queries)
  }
}
