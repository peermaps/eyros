use failure::{Error,bail};
//use std::mem::size_of;
use random_access_storage::RandomAccess;

#[derive(Debug)]
pub struct Meta<S> where S: RandomAccess<Error=Error> {
  store: S,
  pub mask: Vec<bool>,
  pub branch_factor: u16
}

impl<S> Meta<S> where S: RandomAccess<Error=Error> {
  pub fn open(store: S) -> Result<Self,Error> {
    let mut meta = Self {
      store,
      mask: vec![],
      branch_factor: 9
    };
    if !meta.store.is_empty()? {
      let len = meta.store.len()?;
      let buf = meta.store.read(0,len)?;
      meta.load_buffer(&buf)?;
    }
    Ok(meta)
  }
  pub fn save (&mut self) -> Result<(),Error> {
    let mut bytes = vec![];
    bytes.extend(&self.branch_factor.to_be_bytes());
    bytes.extend(&(self.mask.len() as u32).to_be_bytes());
    let mbytes: Vec<u8> = (0..(self.mask.len()+7)/8).map(|i| {
      let mut b = 0u8;
      for j in 0..8 {
        b += (self.mask[i] as u8)*(1<<j);
      }
      b
    }).collect();
    bytes.extend(&mbytes);
    self.store.write(0, &bytes)?;
    Ok(())
  }
  fn load_buffer(&mut self, buf: &Vec<u8>) -> Result<(),Error> {
    self.branch_factor = u16::from_be_bytes([buf[0],buf[1]]);
    self.mask.clear();
    let len = u32::from_be_bytes([buf[2],buf[3],buf[4],buf[5]]) as usize;
    if (len+7)/8+6 != buf.len() {
      bail!("unexpected buffer length");
    }
    for i in 0..(len+7)/8 {
      let b = buf[i+6];
      for j in 0..8 {
        if i*8+j >= len { break }
        self.mask.push((b>>j)&1 == 1);
      }
    }
    if self.mask.len() != len {
      bail!("mask has unexpected length");
    }
    Ok(())
  }
}
