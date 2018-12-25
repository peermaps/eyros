use random_access_storage::RandomAccess;
use failure::Error;

pub struct Tree<S> where
S: RandomAccess<Error=Error> {
  store: S
}
