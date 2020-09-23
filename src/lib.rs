mod store;
pub use store::Storage;
#[doc(hidden)] pub use store::FileStore;
mod setup;
pub use setup::{Setup,SetupFields};

use random_access_storage::RandomAccess;
use desert::{ToBytes,FromBytes,CountBytes};
use core::ops::{Add,Div};

pub type Error = Box<dyn std::error::Error+Sync+Send>;
pub type Location = (u64,u32);

pub trait Value: Copy+ToBytes+FromBytes+CountBytes
  +Send+Sync {}
pub trait Scalar: Copy+PartialOrd+From<u8>+core::fmt::Debug
  +Value+Add<Output=Self>+Div<Output=Self> {}
impl Value for f32 {}
impl Scalar for f32 {}
impl Value for u64 {}

pub enum Coord<X> where X: Scalar {
  Scalar(X),
  Interval(X,X)
}

#[async_trait::async_trait]
pub trait Point {
  async fn batch<S,V>(db: &mut DB<S,Self,V>, rows: &[Row<Self,V>]) -> Result<(),Error>
    where S: RandomAccess<Error=Error>+Unpin+Send+Sync, V: Value, Self: Sized;
}
#[async_trait::async_trait]
impl<X,Y> Point for (Coord<X>,Coord<Y>) where X: Scalar, Y: Scalar {
  async fn batch<S,V>(db: &mut DB<S,Self,V>, rows: &[Row<Self,V>]) -> Result<(),Error>
  where S: RandomAccess<Error=Error>+Unpin+Send+Sync, V: Value {
    Ok(())
  }
}

pub enum Row<P,V> where P: Point, V: Value {
  Insert(P,V),
  Delete(Location)
}

pub struct DB<S,P,V> where S: RandomAccess<Error=Error>+Unpin+Send+Sync, P: Point, V: Value {
  storage: Box<dyn Storage<S>+Unpin+Send+Sync>,
  fields: SetupFields,
  _markerP: std::marker::PhantomData<P>,
  _markerV: std::marker::PhantomData<V>,
}

impl<S,P,V> DB<S,P,V> where S: RandomAccess<Error=Error>+Unpin+Send+Sync, P: Point, V: Value {
  pub async fn open_from_setup(setup: Setup<S>) -> Result<Self,Error> {
    Ok(Self {
      storage: setup.storage.into(),
      fields: setup.fields,
      _markerP: std::marker::PhantomData,
      _markerV: std::marker::PhantomData,
    })
  }
  pub async fn batch(&mut self, rows: &[Row<P,V>]) -> Result<(),Error> {
    P::batch(self, rows).await
  }
}
