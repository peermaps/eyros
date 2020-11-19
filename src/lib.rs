#![feature(async_closure)]
mod store;
pub use store::Storage;
#[doc(hidden)] pub use store::FileStore;
mod setup;
pub use setup::{Setup,SetupFields};
mod tree;
pub use tree::{Tree,Tree2};
mod bytes;
mod query;
pub use query::QueryStream;
mod unfold;
use unfold::Unfold;

use async_std::{stream::Stream,sync::{Arc,Mutex}};
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
pub trait Point: 'static {
  type Bounds: Clone+Send+Sync+core::fmt::Debug+ToBytes+FromBytes+CountBytes;
  async fn batch<S,V>(db: &mut DB<S,Self,V>, rows: &[Row<Self,V>]) -> Result<(),Error>
    where S: RandomAccess<Error=Error>+Unpin+Send+Sync, V: Value, Self: Sized;
  /*
  async fn query<S,V>(db: &mut DB<S,Self,V>, bbox: &Self::Bounds)
    -> Result<query::QStream<Self,V>,Error>
    where S: RandomAccess<Error=Error>+Unpin+Send+Sync, V: Value, Self: Sized;
  */
}

#[async_trait::async_trait]
impl<X,Y> Point for (Coord<X>,Coord<Y>) where X: Scalar, Y: Scalar {
  type Bounds = ((X,Y),(X,Y));
  async fn batch<S,V>(db: &mut DB<S,Self,V>, rows: &[Row<Self,V>]) -> Result<(),Error>
  where S: RandomAccess<Error=Error>+Unpin+Send+Sync, V: Value {
    let inserts: Vec<(&(Coord<X>,Coord<Y>),&V)> = rows.iter()
      .map(|row| match row {
        Row::Insert(p,v) => Some((p,v)),
        _ => None
      })
      .filter(|row| !row.is_none())
      .map(|x| x.unwrap())
      .collect();
    db.trees.push({
      Arc::new(Mutex::new(Tree2::build(9, inserts.as_slice())))
    });
    Ok(())
  }
}

pub enum Row<P,V> where P: Point, V: Value {
  Insert(P,V),
  Delete(Location)
}

pub struct DB<S,P,V> where S: RandomAccess<Error=Error>+Unpin+Send+Sync, P: Point, V: Value {
  pub storage: Box<dyn Storage<S>+Unpin+Send+Sync>,
  pub fields: SetupFields,
  pub trees: Vec<Arc<Mutex<dyn Tree<P,V>>>>,
}

impl<S,P,V> DB<S,P,V> where S: RandomAccess<Error=Error>+Unpin+Send+Sync, P: Point, V: Value {
  pub async fn open_from_setup(setup: Setup<S>) -> Result<Self,Error> {
    Ok(Self {
      storage: setup.storage.into(),
      fields: setup.fields,
      trees: vec![],
    })
  }
  pub async fn batch(&mut self, rows: &[Row<P,V>]) -> Result<(),Error> {
    P::batch(self, rows).await
  }
  /*
  pub async fn query(&mut self, bbox: &P::Bounds) -> Result<query::QStream<P,V>,Error> {
    P::query::<S,V>(self, bbox).await
  */
  pub async fn query(&mut self, bbox: &P::Bounds) -> Result<query::QStream<P,V>,Error> {
    let mut queries = vec![];
    for t in self.trees.iter() {
      queries.push(t.lock().await.query(bbox));
    }
    <QueryStream<P,V>>::from_queries(queries)
  }
}
