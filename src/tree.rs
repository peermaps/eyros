use random_access_storage::RandomAccess;
use failure::Error;
use std::marker::PhantomData;
use std::fmt::Debug;

pub enum Row<A,B,C,D,E,F,V> {
  Insert(Point<A,B,C,D,E,F,V>),
  Delete(Point<A,B,C,D,E,F,V>)
}

pub enum Coord<T> {
  Point(T),
  Range(T,T)
}

pub enum Point<A,B,C,D,E,F,V> {
  P1(Coord<A>,V),
  P2(Coord<A>,Coord<B>,V),
  P3(Coord<A>,Coord<B>,Coord<C>,V),
  P4(Coord<A>,Coord<B>,Coord<C>,Coord<D>,V),
  P5(Coord<A>,Coord<B>,Coord<C>,Coord<D>,Coord<E>,V),
  P6(Coord<A>,Coord<B>,Coord<C>,Coord<D>,Coord<E>,Coord<F>,V)
}

#[derive(Debug)]
pub struct Tree<S,A,B,C,D,E,F,V> where
A: Debug,
B: Debug,
C: Debug,
D: Debug,
E: Debug,
F: Debug,
V: Debug,
S: Debug+RandomAccess<Error=Error> {
  _marker0: PhantomData<A>,
  _marker1: PhantomData<B>,
  _marker2: PhantomData<C>,
  _marker3: PhantomData<D>,
  _marker4: PhantomData<E>,
  _marker5: PhantomData<F>,
  _marker6: PhantomData<V>,
  storage: S
}

impl<S,A,B,C,D,E,F,V> Tree<S,A,B,C,D,E,F,V> where
A: Debug,
B: Debug,
C: Debug,
D: Debug,
E: Debug,
F: Debug,
V: Debug,
S: Debug+RandomAccess<Error=Error> {
  pub fn new(b: usize, storage: S) -> Self {
    Self {
      storage,
      _marker0: PhantomData,
      _marker1: PhantomData,
      _marker2: PhantomData,
      _marker3: PhantomData,
      _marker4: PhantomData,
      _marker5: PhantomData,
      _marker6: PhantomData
    }
  }
  pub fn vacate(&mut self) -> Result<(),Error> {
    self.storage.truncate(0)
  }
  pub fn build(&mut self, rows: Vec<&Row<A,B,C,D,E,F,V>>) -> Result<(),Error> {
    if rows.is_empty() { return Ok(()) }
    Ok(())
  }
}
