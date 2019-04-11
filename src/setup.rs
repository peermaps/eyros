use ::{DB,Point,Value};
use failure::Error;
use random_access_storage::RandomAccess;

pub struct Setup<S,U> where
S: RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>) {
  max_data_size: usize,
  base_size: usize,
  branch_factor: usize,
  bbox_cache_size: usize,
  open_store: U
}

impl<S,U> Setup<S,U> where
S: RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>) {
  pub fn new (open_store: U) -> Self {
    Self {
      open_store,
      branch_factor: 9,
      max_data_size: 1000,
      base_size: 9_000,
      bbox_cache_size: 10_000
    }
  }
  pub fn branch_factor (mut self, bf: usize) -> Self {
    self.branch_factor = bf;
    self
  }
  pub fn base_size (mut self, size: usize) -> Self {
    self.base_size = size;
    self
  }
  pub fn max_data_size (mut self, size: usize) -> Self {
    self.max_data_size = size;
    self
  }
  pub fn bbox_cache_size (mut self, size: usize) -> Self {
    self.bbox_cache_size = size;
    self
  }
  pub fn build<P,V> (self) -> Result<DB<S,U,P,V>,Error>
  where P: Point, V: Value {
    DB::open_opts(self.open_store,
      self.branch_factor,
      self.max_data_size,
      self.base_size
    )
  }
}
