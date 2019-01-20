pub struct Planner {
  pub inputs: Vec<bool>,
  pub outputs: Vec<bool>
}

impl Planner {
  pub fn new (n: u64, trees: Vec<bool>) -> Self {
    let staging = Self::num_to_bits(n);
    let sum = add(&trees, &staging);
    let mut inputs = vec![];
    let mut outputs = vec![];
    for i in 0..sum.len() {
      let t = if i < trees.len() { trees[i] } else { false };
      let s = sum[i];
      inputs.push(t && !s);
      outputs.push(!t && s);
    }
    trim(&mut inputs);
    trim(&mut outputs);
    Self { inputs, outputs }
  }
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
}

fn add (a: &Vec<bool>, b: &Vec<bool>) -> Vec<bool> {
  let len = a.len().max(b.len());
  let mut out = Vec::with_capacity(len);
  let mut carry = 0;
  for i in 0..len {
    let n = (if i < a.len() { a[i] as u64 } else { 0 })
      + (if i < b.len() { b[i] as u64 } else { 0 })
      + carry;
    out.push(n % 2 == 1);
    carry = n / 2;
  }
  out
}

fn trim (v: &mut Vec<bool>) -> () {
  let mut i = (v.len() as i32) - 1;
  while i >= 0 && !v[i as usize] { i -= 1 }
  v.truncate((i+1) as usize);
}
