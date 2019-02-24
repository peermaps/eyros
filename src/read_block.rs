use failure::Error;
use random_access_storage::RandomAccess;
use std::cmp::Ordering;

pub fn read_block<S> (store: &mut S, offset: u64, max_size: u64, guess: u64)
-> Result<Vec<u8>,Error>
where S: RandomAccess<Error=Error> {
  let size_guess = guess.min(max_size - offset);
  let fbuf: Vec<u8> = store.read(offset as usize, size_guess as usize)?;
  let len = u32::from_be_bytes([fbuf[0],fbuf[1],fbuf[2],fbuf[3]]) as u64;
  let mut buf = Vec::with_capacity((len-4) as usize);
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
        (offset+len) as usize,
        (len-size_guess) as usize
      )?);
    }
  };
  Ok(buf)
}
