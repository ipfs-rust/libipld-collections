#![allow(unreachable_code)] // looks like a nightly bug 15.9.19
use crate::Result;
use core::marker::PhantomData;
use dag_cbor_derive::DagCbor;
use libipld::{Cid, Hash, Ipld, IpldError, Store, StoreCborExt};

pub struct List<TStore, THash> {
    prefix: PhantomData<THash>,
    store: TStore,
    link: &'static str,
}

impl<TStore: Store, THash: Hash> List<TStore, THash> {
    pub async fn new(store: TStore, link: &'static str, width: u32) -> Result<Self> {
        let root = store.read_link(link).await?;
        if root.is_none() {
            let node = Node::new(width, 0, vec![]);
            let root = store.write_cbor::<THash, _>(&node).await?;
            store.pin(&root).await?;
            store.write_link(link, &root).await?;
        }

        Ok(Self {
            prefix: PhantomData,
            store,
            link,
        })
    }

    pub async fn flush(&self) -> Result<()> {
        self.store.flush().await?;
        Ok(())
    }

    // TODO take an iterator instead of a vec.
    pub async fn from(
        store: TStore,
        link: &'static str,
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
                let cid = store.write_cbor::<THash, _>(&node).await?;
                nodes.push(Ipld::Link(cid));
            }
            if node_count == 1 {
                let root = ipld_cid_ref(&nodes[0])?;
                store.pin(&root).await?;
                store.write_link(link, &root).await?;
                return Ok(Self {
                    prefix: PhantomData,
                    store,
                    link,
                });
            } else {
                items = nodes;
                height += 1;
            }
        }
    }

    pub async fn push(&self, mut value: Ipld) -> Result<()> {
        let root = self.store.read_link(self.link).await??;
        let node = self.store.read_cbor::<Node>(&root).await??;
        let width = node.width();
        let root_height = node.height();
        let mut height = root_height;
        let mut chain = Vec::with_capacity(height as usize + 1);
        chain.push(node);

        while height > 0 {
            let link = chain
                .last()
                .expect("at least one block")
                .data()
                .last()
                .expect("at least one link");
            let cid = ipld_cid_ref(link)?;
            let node = self.store.read_cbor::<Node>(cid).await??;
            height = node.height();
            chain.push(node);
        }

        let mut mutated = false;
        for mut node in chain.into_iter().rev() {
            if mutated {
                let data = node.data_mut();
                data.pop();
                data.push(value);
                value = Ipld::Link(self.store.write_cbor::<THash, _>(&node).await?);
            } else {
                let data = node.data_mut();
                if data.len() < width {
                    data.push(value);
                    value = Ipld::Link(self.store.write_cbor::<THash, _>(&node).await?);
                    mutated = true;
                } else {
                    let node = Node::new(width as u32, node.height(), vec![value]);
                    value = Ipld::Link(self.store.write_cbor::<THash, _>(&node).await?);
                    mutated = false;
                }
            }
        }

        let new_root = if !mutated {
            let height = root_height + 1;
            let node = Node::new(width as u32, height, vec![Ipld::Link(root.clone()), value]);
            self.store.write_cbor::<THash, _>(&node).await?
        } else {
            ipld_cid(value)?
        };

        self.store.pin(&new_root).await?;
        self.store.write_link(self.link, &new_root).await?;
        self.store.unpin(&root).await?;

        Ok(())
    }

    pub async fn pop(&self) -> Result<Option<Ipld>> {
        // TODO
        /*let root = self.get_node(&self.root)?;
        let width = root.width()?;
        let root_height = root.height()?;
        let mut height = root_height*/
        Ok(None)
    }

    pub async fn get(&self, mut index: usize) -> Result<Option<Ipld>> {
        let root = self.store.read_link(self.link).await??;
        let node = self.store.read_cbor::<Node>(&root).await??;
        let mut node_ref = &node;
        let width = node.width();
        let mut height = node.height();
        let mut node;

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
                node = self.store.read_cbor::<Node>(cid).await??;
                node_ref = &node;
                index %= width.pow(height);
                height = node.height();
            } else {
                return Ok(None);
            }
        }
    }

    pub async fn set(&self, _index: usize, _value: Ipld) -> Result<()> {
        // TODO
        Ok(())
    }

    pub async fn len(&self) -> Result<usize> {
        let root_cid = self.store.read_link(self.link).await??;
        let root = self.store.read_cbor::<Node>(&root_cid).await??;
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
            node = self.store.read_cbor::<Node>(cid).await??;
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
pub struct Iter<'a, TStore: Store, THash: Hash> {
    list: &'a List<TStore, THash>,
    index: usize,
}

impl<'a, TStore: Store, THash: Hash> Iter<'a, TStore, THash> {
    pub async fn next(&mut self) -> Result<Option<Ipld>> {
        let elem = self.list.get(self.index).await?;
        self.index += 1;
        Ok(elem)
    }
}

impl<TStore: Store, THash: Hash> List<TStore, THash> {
    pub fn iter<'a>(&'a self) -> Iter<'a, TStore, THash> {
        Iter {
            list: self,
            index: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use libipld::{DefaultHash as H, MemStore};
    use model::*;
    use std::sync::Arc;

    fn int(i: usize) -> Option<Ipld> {
        Some(Ipld::Integer(i as i128))
    }

    async fn test_list() -> Result<()> {
        let store = Arc::new(MemStore::default());
        let list = List::<_, H>::new(store, "test_list", 3).await?;
        for i in 0..13 {
            assert_eq!(list.get(i).await?, None);
            assert_eq!(list.len().await?, i);
            list.push(Ipld::Integer(i as i128)).await?;
            for j in 0..i {
                assert_eq!(list.get(j).await?, int(j));
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
    fn list_vec_eqv() {
        const LEN: usize = 25;
        model! {
            Model => let mut vec = Vec::new(),
            Implementation => let list = {
                let store = MemStore::default();
                let fut = List::<_, H>::new(store, "test_list", 3);
                task::block_on(fut).unwrap()
            },
            Push(usize)(i in 0..LEN) => {
                vec.push(Ipld::Integer(i as i128));
                task::block_on(list.push(Ipld::Integer(i as i128))).unwrap();
            },
            Get(usize)(i in 0..LEN) => {
                let r1 = vec.get(i).cloned();
                let r2 = task::block_on(list.get(i)).unwrap();
                assert_eq!(r1, r2);
            },
            Len(usize)(_ in 0..LEN) => {
                let r1 = vec.len();
                let r2 = task::block_on(list.len()).unwrap();
                assert_eq!(r1, r2);
            }
        }
    }

    #[test]
    fn list_linearizable() {
        const LEN: usize = 25;
        linearizable! {
            Implementation => let list = {
                let store = MemStore::default();
                let fut = List::<_, H>::new(store, "test_list", 3);
                Shared::new(task::block_on(fut).unwrap())
            },
            Push(usize)(i in 0..LEN) -> () {
                task::block_on(list.push(Ipld::Integer(i as i128))).unwrap();
            },
            Get(usize)(i in 0..LEN) -> Option<Ipld> {
                task::block_on(list.get(i)).unwrap()
            },
            Len(usize)(_ in 0..LEN) -> usize {
                task::block_on(list.len()).unwrap()
            }
        }
    }

    #[test]
    fn test_list_run() {
        task::block_on(test_list()).unwrap();
    }

    async fn test_list_from() -> Result<()> {
        let data: Vec<Ipld> = (0..13).map(|i| Ipld::Integer(i as i128)).collect();
        let store = Arc::new(MemStore::default());
        let list = List::<_, H>::from(store, "test_list_from", 3, data.clone()).await?;
        let mut data2: Vec<Ipld> = Vec::new();
        let mut iter = list.iter();
        while let Some(elem) = iter.next().await? {
            data2.push(elem)
        }
        assert_eq!(data, data2);
        Ok(())
    }

    #[test]
    fn test_list_from_run() {
        task::block_on(test_list_from()).unwrap();
    }
}
