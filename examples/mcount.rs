use eyros::DB;
use failure::Error;
use std::path::PathBuf;
use std::rc::Rc;
use random_access_disk::RandomAccessDisk;

type S = RandomAccessDisk;
type P = ((T,T),(T,T));
type V = u32;
type T = f32;
type R = ((T,T),(T,T));
type I = (u32,u64);

fn main() -> Result<(),Error> {
  let args: Vec<String> = std::env::args().collect();
  let base = PathBuf::from(args[1].clone());
  let mut db: DB<_,_,R,I> = DB::open(|name| {
    let mut p = base.clone();
    p.push(name);
    Ok(RandomAccessDisk::builder(p)
      .auto_sync(false)
      .build()?)
  })?;
  let n = args.len()-1;
  let mut dstores = {
    let mut res = vec![];
    for dir in args[2..n].iter() {
      let mut bfile = PathBuf::from(dir.clone());
      let mut dfile = PathBuf::from(dir.clone());
      bfile.push("range");
      dfile.push("data");
      res.push(<eyros::DataStore<S,P,V>>::open(
        RandomAccessDisk::open(dfile)?,
        RandomAccessDisk::open(bfile)?,
        db.fields.max_data_size,
        db.fields.bbox_cache_size,
        db.fields.data_list_cache_size,
        Rc::clone(&db.bincode)
      )?);
    }
    res
  };
  let bbox = {
    let parts: Vec<&str> = args[n].split(",").collect();
    (
      (parts[0].parse::<T>()?,parts[1].parse::<T>()?),
      (parts[2].parse::<T>()?,parts[3].parse::<T>()?)
    )
  };
  let mut count = 0;
  let mut counts = std::collections::HashMap::new();
  for result in db.query(&bbox)? {
    let (_,(b_index,offset),_) = result?;
    let ds = &mut dstores[b_index as usize];
    for _r in ds.query(offset, &bbox)? {
      count += 1;
      let prev = match counts.get(&b_index) {
        Some(x) => *x,
        None => 0
      };
      counts.insert(b_index, prev + 1);
    }
  }
  println!["{}",count];
  println!["{:?}",counts];
  Ok(())
}
