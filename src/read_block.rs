use failure::{Error,bail};
use random_access_storage::RandomAccess;
use std::cmp::Ordering;

pub fn read_block<S> (store: &mut S, offset: u64, max_size: u64, guess: u64)
-> Result<Vec<u8>,Error>
where S: RandomAccess<Error=Error> {
  let size_guess = guess.min(max_size - offset.min(max_size));
  if size_guess < 4 { bail!["block too small for length field"] }
  let fbuf: Vec<u8> = store.read(offset as usize, size_guess as usize)?;
  let len = u32::from_be_bytes([fbuf[0],fbuf[1],fbuf[2],fbuf[3]]) as u64;
  if len < 4 { bail!["length field must be at least 4"] }
  if offset + len > max_size { bail!["length exceeds end of file"] }
  let mut buf = Vec::with_capacity((len-4) as usize);
  //println!("READ offset={} len={}", offset, len);
  match size_guess.cmp(&len) {
    Ordering::Equal => {
      buf.extend_from_slice(&fbuf[4..]);
    },
    Ordering::Greater => {
      buf.extend_from_slice(&fbuf[4..len as usize]);
    },
    Ordering::Less => {
      buf.extend_from_slice(&fbuf[4..]);
      buf.extend(store.read(
        (offset+(fbuf.len() as u64)) as usize,
        (len-(fbuf.len() as u64)) as usize
      )?);
    }
  };
  Ok(buf)
}
