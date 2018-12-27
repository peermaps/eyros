use failure::Error;
use random_access_storage::RandomAccess;

#[derive(Debug)]
pub struct Meta<'a,S> where S: RandomAccess<Error=Error> {
  store: &'a S,
  pub staging_size: usize,
  pub branch_factor: usize,
  pub mask: Vec<bool>
}

impl<'a,S> Meta<'a,S> where S: RandomAccess<Error=Error> {
  pub fn open(store: &'a mut S) -> Result<Self,Error> {
    if store.is_empty()? {
      Ok(Self {
        store: store,
        staging_size: 0,
        branch_factor: 5,
        mask: vec![]
      })
    } else {
      let len = store.len()?;
      let buf = store.read(0,len)?;
      Self::from_buffer(store, &buf)
    }
  }
  fn from_buffer(store: &'a mut S, buf: &Vec<u8>) -> Result<Self,Error> {
    unimplemented!();
  }
}
