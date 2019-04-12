use ::{DB,Point,Value};
use failure::Error;
use random_access_storage::RandomAccess;

pub struct SetupFields {
  pub max_data_size: usize,
  pub base_size: usize,
  pub branch_factor: usize,
  pub bbox_cache_size: usize,
  pub block_cache_size: usize,
  pub block_cache_count: usize,
}

pub struct Setup<S,U> where
S: RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>) {
  pub open_store: U,
  pub fields: SetupFields
}

impl<S,U> Setup<S,U> where
S: RandomAccess<Error=Error>,
U: (Fn(&str) -> Result<S,Error>) {
  pub fn new (open_store: U) -> Self {
    Self {
      open_store,
      fields: SetupFields {
        branch_factor: 9,
        max_data_size: 1000,
        base_size: 9_000,
        bbox_cache_size: 10_000,
        block_cache_size: 4096,
        block_cache_count: 10_000
      }
    }
  }
  pub fn branch_factor (mut self, bf: usize) -> Self {
    self.fields.branch_factor = bf;
    self
  }
  pub fn base_size (mut self, size: usize) -> Self {
    self.fields.base_size = size;
    self
  }
  pub fn max_data_size (mut self, size: usize) -> Self {
    self.fields.max_data_size = size;
    self
  }
  pub fn block_cache_size (mut self, size: usize) -> Self {
    self.fields.block_cache_size = size;
    self
  }
  pub fn block_cache_count (mut self, count: usize) -> Self {
    self.fields.block_cache_count = count;
    self
  }
  pub fn bbox_cache_size (mut self, size: usize) -> Self {
    self.fields.bbox_cache_size = size;
    self
  }
  pub fn build<P,V> (self) -> Result<DB<S,U,P,V>,Error>
  where P: Point, V: Value {
    DB::open_from_setup(self)
  }
}
