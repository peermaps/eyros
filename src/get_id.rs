pub trait GetId<X> {
  fn get_id(&self) -> X;
}

// Self->Self definitions for built-in types

macro_rules! def_getid {
  ($T:ident) => {
    impl GetId<$T> for $T {
      fn get_id(&self) -> $T { self.clone() }
    }
  }
}

def_getid![u8];
def_getid![u16];
def_getid![u32];
def_getid![u64];
def_getid![i8];
def_getid![i16];
def_getid![i32];
def_getid![i64];
def_getid![f32];
def_getid![f64];
