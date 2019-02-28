pub fn num_to_bits (n: u64) -> Vec<bool> {
  let mut bits = vec![];
  let mut i = 1;
  while i <= n {
    bits.push((n/i)%2 == 1);
    i *= 2;
  }
  bits
}
