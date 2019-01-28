use failure::Error;
use random_access_storage::RandomAccess;

#[derive(Debug)]
pub struct Meta<S> where S: RandomAccess<Error=Error> {
  store: S,
  pub branch_factor: usize
}

impl<S> Meta<S> where S: RandomAccess<Error=Error> {
  pub fn open(mut store: S) -> Result<Self,Error> {
    if store.is_empty()? {
      Ok(Self {
        store: store,
        branch_factor: 5
      })
    } else {
      let len = store.len()?;
      let buf = store.read(0,len)?;
      Self::from_buffer(&mut store, &buf)
    }
  }
  pub fn save () -> Result<(),Error> {
    Ok(())
  }
  fn from_buffer(store: &mut S, buf: &Vec<u8>) -> Result<Self,Error> {
    unimplemented!();
  }
}
