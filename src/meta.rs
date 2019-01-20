use failure::Error;
use random_access_storage::RandomAccess;

#[derive(Debug)]
pub struct Meta<'a,S> where S: RandomAccess<Error=Error> {
  store: &'a S,
  pub branch_factor: usize
}

impl<'a,S> Meta<'a,S> where S: RandomAccess<Error=Error> {
  pub fn open(store: &'a mut S) -> Result<Self,Error> {
    if store.is_empty()? {
      Ok(Self {
        store: store,
        branch_factor: 5
      })
    } else {
      let len = store.len()?;
      let buf = store.read(0,len)?;
      Self::from_buffer(store, &buf)
    }
  }
  pub fn save () -> Result<(),Error> {
    Ok(())
  }
  fn from_buffer(store: &'a mut S, buf: &Vec<u8>) -> Result<Self,Error> {
    unimplemented!();
  }
}
