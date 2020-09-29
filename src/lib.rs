mod store;
pub use store::Storage;
#[doc(hidden)] pub use store::FileStore;
mod setup;
pub use setup::{Setup,SetupFields};
mod tree;
pub use tree::{Tree,Tree2};

use random_access_storage::RandomAccess;
use desert::{ToBytes,FromBytes,CountBytes};
use core::ops::{Add,Div};

pub type Error = Box<dyn std::error::Error+Sync+Send>;
pub type Location = (u64,u32);

pub trait Value: Clone+ToBytes+FromBytes+CountBytes+core::fmt::Debug
  +Send+Sync+'static {}
pub trait Scalar: Clone+PartialOrd+From<u8>+core::fmt::Debug
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
pub trait Point {
  async fn batch<S,V>(db: &mut DB<S,Self,V>, rows: &[Row<Self,V>]) -> Result<(),Error>
    where S: RandomAccess<Error=Error>+Unpin+Send+Sync, V: Value, Self: Sized;
}

#[async_trait::async_trait]
impl<X,Y> Point for (Coord<X>,Coord<Y>) where X: Scalar, Y: Scalar {
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
    //db.trees.push(Box::new(Tree2::build(9, inserts.as_slice())));
    db.trees.push({
      let t = Tree2::build(9, inserts.as_slice());
      eprintln!["{:?}",t];
      Box::new(t)
    });
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
  trees: Vec<Box<dyn Tree<P,V>>>,
  _markerP: std::marker::PhantomData<P>,
  _markerV: std::marker::PhantomData<V>,
}

impl<S,P,V> DB<S,P,V> where S: RandomAccess<Error=Error>+Unpin+Send+Sync, P: Point, V: Value {
  pub async fn open_from_setup(setup: Setup<S>) -> Result<Self,Error> {
    Ok(Self {
      storage: setup.storage.into(),
      fields: setup.fields,
      trees: vec![],
      _markerP: std::marker::PhantomData,
      _markerV: std::marker::PhantomData,
    })
  }
  pub async fn batch(&mut self, rows: &[Row<P,V>]) -> Result<(),Error> {
    P::batch(self, rows).await
  }
}
