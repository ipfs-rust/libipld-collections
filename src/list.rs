use core::convert::TryFrom;
use core::marker::PhantomData;
use libipld::{format_err, ipld, Cid, Dag, Ipld, IpldGet, IpldGetMut, IpldStore, Prefix, Result};

#[derive(Debug)]
pub struct List<TPrefix: Prefix, TStore: IpldStore> {
    prefix: PhantomData<TPrefix>,
    dag: Dag<TStore>,
    root: Cid,
}

impl<TPrefix: Prefix, TStore: IpldStore> List<TPrefix, TStore> {
    fn create_node(width: usize, height: u32, data: Vec<Ipld>) -> Node {
        Node(ipld!({
            "width": width as i128,
            "height": height as i128,
            "data": data,
        }))
    }

    fn get_node(&self, cid: &Cid) -> Result<Node> {
        Ok(Node(self.dag.get_block(cid)?))
    }

    fn put_node(&mut self, node: &Node) -> Result<Cid> {
        Ok(self.dag.put_block::<TPrefix>(&node.0)?)
    }
}

impl<TPrefix: Prefix, TStore: IpldStore> List<TPrefix, TStore> {
    pub fn load(root: Cid) -> Self {
        let dag = Dag::new(Default::default());
        Self {
            prefix: PhantomData,
            dag,
            root,
        }
    }

    pub fn new(width: u32) -> Result<Self> {
        let mut dag = Dag::new(Default::default());
        let node = Self::create_node(width as usize, 0, vec![]);
        let root = dag.put_block::<TPrefix>(&node.0)?;
        Ok(Self {
            prefix: PhantomData,
            dag,
            root,
        })
    }

    // TODO take an iterator instead of a vec.
    pub fn from(width: u32, mut items: Vec<Ipld>) -> Result<Self> {
        let mut dag = Dag::new(Default::default());
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
                let node = Self::create_node(width, height, data);
                let cid = dag.put_block::<TPrefix>(&node.0)?;
                nodes.push(Ipld::Link(cid));
            }
            if node_count == 1 {
                let root = nodes[0].as_link().unwrap().to_owned();
                return Ok(Self {
                    prefix: PhantomData,
                    dag,
                    root,
                });
            } else {
                items = nodes;
                height += 1;
            }
        }
    }

    pub fn push(&mut self, mut value: Ipld) -> Result<()> {
        let root = self.get_node(&self.root)?;
        let width = root.width()?;
        let root_height = root.height()?;
        let mut height = root_height;
        let mut chain = Vec::with_capacity(height as usize + 1);
        chain.push(root);

        while height > 0 {
            let cid = chain
                .last()
                .unwrap()
                .data()?
                .last()
                .expect("at least one node")
                .as_link()
                .expect("must be a link");
            let node = self.get_node(&cid)?;
            height = node.height()?;
            chain.push(node);
        }

        let mut mutated = false;
        for mut node in chain.into_iter().rev() {
            if mutated {
                let data = node.data_mut()?;
                data.pop();
                data.push(value);
                value = self.put_node(&node)?.into();
            } else {
                let data = node.data_mut()?;
                if data.len() < width {
                    data.push(value);
                    value = self.put_node(&node)?.into();
                    mutated = true;
                } else {
                    let node = Self::create_node(width, node.height()?, vec![value]);
                    value = self.put_node(&node)?.into();
                    mutated = false;
                }
            }
        }

        if !mutated {
            let height = root_height + 1;
            let node = Self::create_node(width, height, vec![(&self.root).into(), value]);
            self.root = self.put_node(&node)?;
        } else {
            if let Ipld::Link(cid) = value {
                self.root = cid;
            } else {
                return Err(format_err!("expected link but found {:?}", value));
            }
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

    pub fn get(&self, mut index: usize) -> Result<Option<Ipld>> {
        let root = self.get_node(&self.root)?;
        let width = root.width()?;
        let mut height = root.height()?;
        let mut node;
        let mut node_ref = &root;

        if index > width.pow(height + 1) {
            return Ok(None);
        }

        loop {
            let data_index = index / width.pow(height);
            if let Some(ipld) = node_ref.data()?.get(data_index) {
                if height == 0 {
                    return Ok(Some(ipld.to_owned()));
                }
                if let Some(cid) = ipld.as_link() {
                    node = self.get_node(cid)?;
                    node_ref = &node;
                    index %= width.pow(height);
                    height = node.height()?;
                } else {
                    return Err(format_err!("expected link but found {:?}", ipld));
                }
            } else {
                return Ok(None);
            }
        }
    }

    pub fn set(&mut self, _index: usize, _value: Ipld) -> Result<()> {
        // TODO
        Ok(())
    }

    pub fn len(&self) -> Result<usize> {
        let root = self.get_node(&self.root)?;
        let width = root.width()?;
        let mut height = root.height()?;
        let mut size = width.pow(height + 1);
        let mut node = root;
        loop {
            let data = node.data()?;
            size -= width.pow(height) * (width - data.len());
            if height == 0 {
                return Ok(size);
            }
            let cid = data.last().unwrap().as_link().unwrap();
            node = self.get_node(&cid)?;
            height = node.height()?;
        }
    }
}

#[derive(Clone, Debug)]
struct Node(Ipld);

impl Node {
    #[allow(unused)]
    fn width(&self) -> Result<usize> {
        let width = self
            .0
            .get("width")
            .map(|ipld| ipld.as_int())
            .unwrap()
            .map(|int| usize::try_from(*int).ok())
            .unwrap();
        if let Some(width) = width {
            Ok(width)
        } else {
            Err(format_err!("invalid width"))
        }
    }

    fn height(&self) -> Result<u32> {
        let height = self
            .0
            .get("height")
            .map(|ipld| ipld.as_int())
            .unwrap()
            .map(|int| u32::try_from(*int).ok())
            .unwrap();
        if let Some(height) = height {
            Ok(height)
        } else {
            Err(format_err!("invalid node"))
        }
    }

    fn data(&self) -> Result<&Vec<Ipld>> {
        let data = self.0.get("data").map(|ipld| ipld.as_list()).unwrap();
        if let Some(data) = data {
            Ok(data)
        } else {
            Err(format_err!("invalid node"))
        }
    }

    fn data_mut(&mut self) -> Result<&mut Vec<Ipld>> {
        let data = self
            .0
            .get_mut("data")
            .map(|ipld| ipld.as_list_mut())
            .unwrap();
        if let Some(data) = data {
            Ok(data)
        } else {
            Err(format_err!("invalid node"))
        }
    }
}

// TODO: make more efficient
pub struct Iter<'a, TPrefix: Prefix, TStore: IpldStore> {
    list: &'a List<TPrefix, TStore>,
    index: usize,
}

impl<'a, TPrefix: Prefix, TStore: IpldStore> Iterator for Iter<'a, TPrefix, TStore> {
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

impl<TPrefix: Prefix, TStore: IpldStore> List<TPrefix, TStore> {
    pub fn iter<'a>(&'a self) -> Iter<'a, TPrefix, TStore> {
        Iter {
            list: self,
            index: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libipld::{DefaultPrefix, mock::MemStore};

    fn int(i: usize) -> Option<Ipld> {
        Some(Ipld::Integer(i as i128))
    }

    #[test]
    fn test_list() -> Result<()> {
        let mut list = List::<DefaultPrefix, MemStore>::new(3)?;
        for i in 0..13 {
            assert_eq!(list.get(i)?, None);
            assert_eq!(list.len()?, i);
            list.push((i as i128).into())?;
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
        let list = List::<DefaultPrefix, MemStore>::from(3, data.clone())?;
        let data2: Vec<Ipld> = list.iter().map(|ipld| ipld.unwrap()).collect();
        assert_eq!(data, data2);
        Ok(())
    }
}
