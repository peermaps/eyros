use failure::Error;

#[derive(Debug)]
pub struct Meta {
  pub staging_size: usize,
  pub branch_factor: usize,
  pub mask: Vec<bool>
}

impl Meta {
  pub fn from_buffer(buf: &Vec<u8>) -> Result<Self,Error> {
    unimplemented!();
  }
}
