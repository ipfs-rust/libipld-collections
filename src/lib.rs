use core::marker::PhantomData;
use libipld::{ipld, Cid, Dag, Ipld, IpldStore, Result};

pub trait Config {
    type Prefix: libipld::Prefix;
    const WIDTH: usize;
}

pub struct DefaultConfig;

impl Config for DefaultConfig {
    type Prefix = libipld::DefaultPrefix;
    const WIDTH: usize = 3;
}

#[derive(Debug)]
pub struct Vector<TConfig: Config, TStore: IpldStore> {
    config: PhantomData<TConfig>,
    dag: Dag<TStore>,
    root: Ipld,
}

impl<TConfig: Config, TStore: IpldStore> Vector<TConfig, TStore> {
    pub fn new() -> Self {
        let dag = Dag::new(Default::default());
        let root = ipld!({
            "width": TConfig::WIDTH as i128,
            "height": 0,
            "data": [],
        });
        Self {
            config: PhantomData,
            dag,
            root,
        }
    }

    pub fn from<T: Into<Ipld>>(_items: Vec<T>) -> Self {
        Self::new()
    }

    pub fn load(cid: Cid) -> Result<Self> {
        let mut vec = Self::new();
        vec.root = vec.dag.get(&cid.into())?;
        Ok(vec)
    }
    
    pub fn push(&mut self, _value: Ipld) {
    }

    pub fn pop(&mut self) -> Ipld {
        Ipld::Null
    }

    pub fn get(&self, _index: usize) -> Ipld {
        Ipld::Null
    }

    pub fn set(&mut self, _index: usize, _value: Ipld) {
    }

    pub fn len(&self) -> usize {
        0
    }

    pub fn iter(&self) {
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libipld::{BlockStore, format_err};
    use std::collections::HashMap;

    #[derive(Default)]
    struct Store(HashMap<String, Box<[u8]>>);

    impl BlockStore for Store {
        unsafe fn read(&self, cid: &Cid) -> Result<Box<[u8]>> {
            if let Some(data) = self.0.get(&cid.to_string()) {
                Ok(data.to_owned())
            } else {
                Err(format_err!("Block not found"))
            }
        }

        unsafe fn write(&mut self, cid: &Cid, data: Box<[u8]>) -> Result<()> {
            self.0.insert(cid.to_string(), data);
            Ok(())
        }

        fn delete(&mut self, cid: &Cid) -> Result<()> {
            self.0.remove(&cid.to_string());
            Ok(())
        }
    }

    #[test]
    fn create_vector() {
        let _vec = Vector::<DefaultConfig, Store>::new();
    }
}
