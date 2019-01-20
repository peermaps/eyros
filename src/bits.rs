pub fn num_to_bits (n: u64) -> Vec<bool> {
  let mut bits = vec![];
  let mut i = 1;
  while i <= n {
    bits.push((n/i)%2 == 1);
    i *= 2;
  }
  bits
}

pub fn bits_to_num (bits: Vec<bool>) -> u64 {
  let mut n = 0;
  for (i,bit) in bits.iter().enumerate() {
    n += ((*bit) as u64)*(1<<i);
  }
  n
}
