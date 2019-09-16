use core::marker::PhantomData;
use dag_cbor_derive::DagCbor;
use libipld::{BlockStore, Cache, Cid, Hash, Ipld, IpldError, Result, Store};

pub struct List<TStore: Store, TCache: Cache, THash: Hash> {
    prefix: PhantomData<THash>,
    store: BlockStore<TStore, TCache>,
    root: Cid,
}

impl<TStore: Store, TCache: Cache, THash: Hash> List<TStore, TCache, THash> {
    pub fn load(store: BlockStore<TStore, TCache>, root: Cid) -> Self {
        Self {
            prefix: PhantomData,
            store,
            root,
        }
    }

    pub fn new(mut store: BlockStore<TStore, TCache>, width: u32) -> Result<Self> {
        let node = Node::new(width, 0, vec![]);
        let root = store.write_cbor::<THash, _>(&node)?;
        Ok(Self {
            prefix: PhantomData,
            store,
            root,
        })
    }

    // TODO take an iterator instead of a vec.
    pub fn from(
        mut store: BlockStore<TStore, TCache>,
        width: u32,
        mut items: Vec<Ipld>,
    ) -> Result<Self> {
        let width = width as usize;
        let mut height = 0;

        loop {
            let mut node_count = items.len() / width;
            if items.len() % width != 0 {
                node_count += 1;
            }

            let mut nodes = Vec::with_capacity(node_count);
            for i in 0..node_count {
                let start = i * width;
                let end = (i + 1) * width;
                let end = core::cmp::min(end, items.len());
                let mut data = Vec::with_capacity(width);
                for i in start..end {
                    data.push(items[i].clone());
                }
                let node = Node::new(width as u32, height, data);
                let cid = store.write_cbor::<THash, _>(&node)?;
                nodes.push(Ipld::Link(cid));
            }
            if node_count == 1 {
                let root = ipld_cid_ref(&nodes[0])?.to_owned();
                return Ok(Self {
                    prefix: PhantomData,
                    store,
                    root,
                });
            } else {
                items = nodes;
                height += 1;
            }
        }
    }

    pub fn push(&mut self, mut value: Ipld) -> Result<()> {
        let root = self.store.read_cbor::<Node>(&self.root)?;
        let width = root.width();
        let root_height = root.height();
        let mut height = root_height;
        let mut chain = Vec::with_capacity(height as usize + 1);
        chain.push(root);

        while height > 0 {
            let link = chain
                .last()
                .expect("at least one block")
                .data()
                .last()
                .expect("at least one link");
            let cid = ipld_cid_ref(link)?;
            let node = self.store.read_cbor::<Node>(cid)?;
            height = node.height();
            chain.push(node);
        }

        let mut mutated = false;
        for mut node in chain.into_iter().rev() {
            if mutated {
                let data = node.data_mut();
                data.pop();
                data.push(value);
                value = Ipld::Link(self.store.write_cbor::<THash, _>(&node)?);
            } else {
                let data = node.data_mut();
                if data.len() < width {
                    data.push(value);
                    value = Ipld::Link(self.store.write_cbor::<THash, _>(&node)?);
                    mutated = true;
                } else {
                    let node = Node::new(width as u32, node.height(), vec![value]);
                    value = Ipld::Link(self.store.write_cbor::<THash, _>(&node)?);
                    mutated = false;
                }
            }
        }

        if !mutated {
            let height = root_height + 1;
            let node = Node::new(
                width as u32,
                height,
                vec![Ipld::Link(self.root.clone()), value],
            );
            self.root = self.store.write_cbor::<THash, _>(&node)?;
        } else {
            self.root = ipld_cid(value)?;
        }

        Ok(())
    }

    pub fn pop(&mut self) -> Result<Option<Ipld>> {
        // TODO
        /*let root = self.get_node(&self.root)?;
        let width = root.width()?;
        let root_height = root.height()?;
        let mut height = root_height*/
        Ok(None)
    }

    pub fn get(&mut self, mut index: usize) -> Result<Option<Ipld>> {
        let root = self.store.read_cbor::<Node>(&self.root)?;
        let width = root.width();
        let mut height = root.height();
        let mut node;
        let mut node_ref = &root;

        if index > width.pow(height + 1) {
            return Ok(None);
        }

        loop {
            let data_index = index / width.pow(height);
            if let Some(ipld) = node_ref.data().get(data_index) {
                if height == 0 {
                    return Ok(Some(ipld.to_owned()));
                }
                let cid = ipld_cid_ref(ipld)?;
                node = self.store.read_cbor::<Node>(cid)?;
                node_ref = &node;
                index %= width.pow(height);
                height = node.height();
            } else {
                return Ok(None);
            }
        }
    }

    pub fn set(&mut self, _index: usize, _value: Ipld) -> Result<()> {
        // TODO
        Ok(())
    }

    pub fn len(&mut self) -> Result<usize> {
        let root = self.store.read_cbor::<Node>(&self.root)?;
        let width = root.width();
        let mut height = root.height();
        let mut size = width.pow(height + 1);
        let mut node = root;
        loop {
            let data = node.data();
            size -= width.pow(height) * (width - data.len());
            if height == 0 {
                return Ok(size);
            }
            let cid = ipld_cid_ref(data.last().unwrap())?;
            node = self.store.read_cbor::<Node>(cid)?;
            height = node.height();
        }
    }
}

fn ipld_cid(ipld: Ipld) -> Result<Cid> {
    if let Ipld::Link(cid) = ipld {
        Ok(cid)
    } else {
        Err(IpldError::NotLink.into())
    }
}

fn ipld_cid_ref<'a>(ipld: &'a Ipld) -> Result<&'a Cid> {
    if let Ipld::Link(cid) = ipld {
        Ok(cid)
    } else {
        Err(IpldError::NotLink.into())
    }
}

#[derive(Clone, Debug, DagCbor)]
struct Node {
    width: u32,
    height: u32,
    data: Vec<Ipld>,
}

impl Node {
    fn new(width: u32, height: u32, data: Vec<Ipld>) -> Self {
        Node {
            width,
            height,
            data,
        }
    }

    fn width(&self) -> usize {
        self.width as usize
    }

    fn height(&self) -> u32 {
        self.height
    }

    fn data(&self) -> &[Ipld] {
        &self.data
    }

    fn data_mut(&mut self) -> &mut Vec<Ipld> {
        &mut self.data
    }
}

// TODO: make more efficient
pub struct Iter<'a, TStore: Store, TCache: Cache, THash: Hash> {
    list: &'a mut List<TStore, TCache, THash>,
    index: usize,
}

impl<'a, TStore: Store, TCache: Cache, THash: Hash> Iterator for Iter<'a, TStore, TCache, THash> {
    type Item = Result<Ipld>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.list.get(self.index) {
            Ok(Some(ipld)) => {
                self.index += 1;
                Some(Ok(ipld))
            }
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

impl<TStore: Store, TCache: Cache, THash: Hash> List<TStore, TCache, THash> {
    pub fn iter<'a>(&'a mut self) -> Iter<'a, TStore, TCache, THash> {
        Iter {
            list: self,
            index: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libipld::{
        mock::{MemCache, MemStore},
        DefaultHash,
    };

    type MemList = List<MemStore, MemCache, DefaultHash>;

    fn int(i: usize) -> Option<Ipld> {
        Some(Ipld::Integer(i as i128))
    }

    #[test]
    fn test_list() -> Result<()> {
        let store = BlockStore::new(16);
        let mut list = MemList::new(store, 3)?;
        for i in 0..13 {
            assert_eq!(list.get(i)?, None);
            assert_eq!(list.len()?, i);
            list.push(Ipld::Integer(i as i128))?;
            for j in 0..i {
                assert_eq!(list.get(j)?, int(j));
            }
        }
        /*for i in 0..13 {
            list.set(i, (i as i128 + 1).into())?;
            assert_eq!(list.get(i)?, int(i + 1));
        }*/
        /*for i in (0..13).rev() {
            assert_eq!(vec.len()?, i + 1);
            assert_eq!(vec.pop()?, int(i));
        }*/
        Ok(())
    }

    #[test]
    fn test_list_from() -> Result<()> {
        let data: Vec<Ipld> = (0..13).map(|i| Ipld::Integer(i as i128)).collect();
        let store = BlockStore::new(16);
        let mut list = MemList::from(store, 3, data.clone())?;
        let data2: Vec<Ipld> = list.iter().map(|ipld| ipld.unwrap()).collect();
        assert_eq!(data, data2);
        Ok(())
    }
}
