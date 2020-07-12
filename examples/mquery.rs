use eyros::DB;
use std::path::PathBuf;
use random_access_disk::RandomAccessDisk;
use async_std::prelude::*;

type S = RandomAccessDisk;
type P = ((T,T),(T,T));
type V = u32;
type T = f32;
type R = ((T,T),(T,T));
type I = (u32,u64);
type E = Box<dyn std::error::Error+Sync+Send>;

#[async_std::main]
async fn main() -> Result<(),E> {
  let args: Vec<String> = std::env::args().collect();
  let base = PathBuf::from(args[1].clone());
  let mut db: DB<_,R,I> = DB::open_from_path(&base).await?;
  let n = args.len()-1;
  let mut dstores = {
    let mut res = vec![];
    for dir in args[2..n].iter() {
      let mut bfile = PathBuf::from(dir.clone());
      let mut dfile = PathBuf::from(dir.clone());
      bfile.push("bbox");
      dfile.push("data");
      res.push(<eyros::DataStore<S,P,V>>::open(
        RandomAccessDisk::open(dfile).await?,
        RandomAccessDisk::open(bfile).await?,
        db.fields.max_data_size,
        db.fields.bbox_cache_size,
        db.fields.data_list_cache_size
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
  let mut stream = db.query(&bbox).await?;
  while let Some(result) = stream.next().await {
    let (_,(b_index,offset),_) = result?;
    let ds = &mut dstores[b_index as usize];
    for r in ds.query(offset, &bbox).await? {
      println!["{:?}", r];
    }
  }
  Ok(())
}
