mod store;
pub use store::Storage;
#[doc(hidden)] pub use store::FileStore;
mod setup;
pub use setup::SetupFields;

use desert::{ToBytes,FromBytes,CountBytes};
use core::ops::{Add,Div};

pub type Error = Box<dyn std::error::Error+Sync+Send>;
pub type Location = (u64,u32);

pub trait Value: Copy+ToBytes+FromBytes+CountBytes {}
pub trait Scalar: Copy+PartialOrd+From<u8>+core::fmt::Debug
  +Value
  +Add<Output=Self>+Div<Output=Self> {}
impl Value for f32 {}
impl Scalar for f32 {}
impl Value for u64 {}

pub enum Coord<X> where X: Scalar {
  Scalar(X),
  Interval(X,X)
}

pub mod d2 {
  pub use crate::setup::Setup2 as Setup;
  pub use crate::{Storage,Scalar,Coord,Value,Location,Error};
  use random_access_storage::RandomAccess;

  pub enum Row<X,Y,V> where X: Scalar, Y: Scalar, V: Value {
    Insert(Coord<X>,Coord<Y>,V),
    Delete(Location)
  }

  pub struct DB<S,X,Y,V> where S: RandomAccess<Error=Error>, X: Scalar, Y: Scalar, V: Value {
    _markerS: std::marker::PhantomData<S>,
    _markerX: std::marker::PhantomData<X>,
    _markerY: std::marker::PhantomData<Y>,
    _markerV: std::marker::PhantomData<V>,
  }
  impl<S,X,Y,V> DB<S,X,Y,V>
  where S: RandomAccess<Error=Error>+Unpin+Send+Sync, X: Scalar, Y: Scalar, V: Value {
    pub async fn open_from_setup(setup: Setup<S>) -> Result<Self,Error> {
      unimplemented![]
    }
    pub async fn batch(&self, rows: &[Row<X,Y,V>]) -> Result<(),Error> {
      Ok(())
    }
  }
}
