use failure::{Error,bail};

pub fn decode(buf: &[u8]) -> Result<(usize,u64),Error> {
  let mut value = 0u64;
  let mut m = 1u64;
  let mut offset = 0usize;
  for _i in 0..8 {
    if offset >= buf.len() {
      bail!["buffer supplied to varint decoding too small"]
    }
    let byte = buf[offset];
    offset += 1;
    value += m * u64::from(byte & 127);
    m *= 128;
    if byte & 128 == 0 { break }
  }
  Ok((offset,value))
}

pub fn encode(value: u64, buf: &mut [u8]) -> Result<usize,Error> {
  let len = length(value);
  if buf.len() < len { bail!["buffer is too small to write varint"] }
  let mut offset = 0;
  let mut v = value;
  while v > 127 {
    buf[offset] = (v as u8) | 128;
    offset += 1;
    v >>= 7;
  }
  buf[offset] = v as u8;
  Ok(len)
}

pub fn length(value: u64) -> usize {
  let msb = (64 - value.leading_zeros()) as usize;
  (msb.max(1)+6)/7
}
