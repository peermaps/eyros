use random_access_storage::RandomAccess;
use failure::Error;
use std::marker::PhantomData;

use point::Point;
use ::{Row,Value};

pub struct Tree<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  store: S,
  _marker: PhantomData<(P,V)>
}

impl<S,P,V> Tree<S,P,V>
where S: RandomAccess<Error=Error>, P: Point, V: Value {
  pub fn new (rows: &Vec<(P,V)>) -> Self {
    unimplemented!();
  }
}
