use std::fmt::Debug;
use std::rc::Rc;

use smallvec::SmallVec;

use crate::sql::Attribute;
use crate::*;

mod cell {
    use std::cell::UnsafeCell;

    pub struct OnceCell<T, Z> {
        inner: UnsafeCell<Result<T, Z>>,
    }

    impl<T, Z> OnceCell<T, Z> {
        /// Creates a new empty cell.
        pub const fn new(init: Z) -> OnceCell<T, Z> {
            OnceCell {
                inner: UnsafeCell::new(Err(init)),
            }
        }

        pub fn get(&self) -> Option<&T> {
            match unsafe { &*self.inner.get() } {
                Ok(v) => Some(v),
                Err(_) => None,
            }
        }

        pub fn get_init_data(&mut self) -> &mut Z {
            match self.inner.get_mut() {
                Ok(_) => unsafe { std::hint::unreachable_unchecked() },
                Err(z) => z,
            }
        }

        pub fn get_or_init<F>(&self, f: F) -> &T
        where
            F: FnOnce(Z) -> T,
        {
            if let Some(v) = self.get() {
                return v;
            }
            let z = match unsafe { &mut *self.inner.get() } {
                Ok(_) => unreachable!(),
                Err(z) => unsafe { std::ptr::read(z) },
            };
            let v = f(z);
            unsafe { std::ptr::write(self.inner.get(), Ok(v)) };
            self.get().unwrap()
        }
    }
}

type Id = i32;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub enum Value {
    Str(Rc<String>),
    Num(i32),
}

impl Value {
    pub fn as_num(&self) -> i32 {
        match self {
            Value::Num(n) => *n,
            Value::Str(_) => panic!("Value is not a number"),
        }
    }
}

pub struct Trie {
    inner: cell::OnceCell<TrieInner, Thunk>,
}

struct Thunk {
    col: u32,
    schema: Rc<TrieSchema>,
    indexes: IdxBuf,
}

impl Thunk {
    fn for_each(&self, mut f: impl FnMut(u32, Id)) {
        let col = &self.schema.id_cols[self.col as usize];
        if self.indexes.is_empty() {
            for (i, id) in col.iter().enumerate() {
                f(i as u32, id.as_num());
            }
        } else {
            for &idx in &self.indexes {
                f(idx, col[idx as usize].as_num());
            }
        }
    }
}

type IdxBuf = SmallVec<[u32; 4]>;

enum TrieInner {
    Node(HashMap<Id, Box<Trie>>),
    SetNode(HashSet<Id>, Box<Trie>),
    Data(Rc<TrieSchema>, IdxBuf),
}

#[derive(Debug, Clone)]
pub struct TrieSchema {
    pub id_cols: Vec<Col>,
    pub data_cols: Vec<Col>,
}

pub type Schema = Vec<Attribute>;

pub enum Table {
    Trie(Trie),
    Arr {
        id_cols: Vec<Col>,
        data_cols: Vec<Col>,
    },
}

impl Trie {
    // pub fn is_empty(&self) -> bool {
    //     self.len() == 0
    // }

    pub fn new(schema: TrieSchema) -> Self {
        Self {
            inner: cell::OnceCell::new(Thunk {
                col: 0,
                schema: Rc::new(schema),
                indexes: IdxBuf::new(),
            }),
        }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        match self.force() {
            TrieInner::Node(map) => map.len(),
            TrieInner::SetNode(set, _t) => set.len(),
            TrieInner::Data(_, indexes) => indexes.len(),
        }
    }

    pub fn guess_len(&self) -> usize {
        self.len()
    }

    #[inline(never)]
    fn from_thunk(thunk: Thunk) -> TrieInner {
        let id_cols_left = thunk.schema.id_cols.len() - thunk.col as usize;

        let mk_thunk = || {
            Box::new(Trie {
                inner: cell::OnceCell::new(Thunk {
                    col: thunk.col + 1,
                    schema: thunk.schema.clone(),
                    indexes: Default::default(),
                }),
            })
        };

        if id_cols_left == 0 {
            TrieInner::Data(thunk.schema, thunk.indexes)
        } else if id_cols_left == 1 && thunk.schema.data_cols.is_empty() {
            let mut set = HashSet::<Id>::default();
            thunk.for_each(|_i, id| {
                set.insert(id);
            });
            TrieInner::SetNode(set, mk_thunk())
        } else {
            let mut map = HashMap::<Id, Box<Trie>>::default();

            thunk.for_each(|i, id| {
                map.entry(id)
                    .or_insert_with(mk_thunk)
                    .inner
                    .get_init_data()
                    .indexes
                    .push(i);
            });

            // println!("Trie len: {}", map.len());

            TrieInner::Node(map)
        }
    }

    #[inline(always)]
    fn force(&self) -> &TrieInner {
        self.inner.get_or_init(Self::from_thunk)
    }

    pub fn get(&self, id: Id) -> Option<&Trie> {
        match self.force() {
            TrieInner::Node(map) => match map.get(&id) {
                Some(t) => Some(t),
                None => None,
            },
            TrieInner::SetNode(set, trie) => {
                if set.contains(&id) {
                    Some(trie)
                } else {
                    None
                }
            }
            TrieInner::Data(..) => panic!("Trie is not a node"),
        }
    }

    pub fn for_each(&self, mut f: impl FnMut(Id, &Self)) {
        match self.force() {
            TrieInner::Node(map) => {
                for (id, trie) in map {
                    f(*id, trie);
                }
            }
            TrieInner::SetNode(set, trie) => {
                for id in set {
                    f(*id, trie);
                }
            }
            TrieInner::Data(..) => panic!("Trie is not a node"),
        }
    }

    pub fn has_data(&self) -> bool {
        match self.force() {
            TrieInner::SetNode(..) => false,
            TrieInner::Node(_) => panic!(),
            TrieInner::Data(schema, _idxs) => !schema.data_cols.is_empty(),
        }
    }

    pub fn for_each_data(&self, mut f: impl FnMut(&[Value])) {
        match self.force() {
            TrieInner::SetNode(..) => panic!(),
            TrieInner::Node(_) => panic!(),
            TrieInner::Data(schema, idxs) => {
                let mut row = vec![];
                for idx in idxs {
                    for col in &schema.data_cols {
                        row.push(col[*idx as usize].clone());
                    }
                    f(&row);
                    row.clear();
                }
            }
        }
    }

    // pub fn insert(&mut self, ids: &[Id], data: &[Value]) {
    //     match self {
    //         Trie::Nil => {
    //             *self = Trie::Vec(ids.to_vec(), data.to_vec().into());
    //         }
    //         Trie::Vec(ids_0, data_0) => {
    //             let mut m = HashMap::default();
    //             if ids_0[1..].is_empty() {
    //                 m.insert(ids_0[0], Trie::Data(data_0.len(), data_0.clone()));
    //             } else {
    //                 m.insert(ids_0[0], Trie::Vec(ids_0[1..].to_vec(), data_0.clone()));
    //             }
    //             *self = Trie::Node(m);
    //             self.insert(ids, data);
    //         }
    //         Trie::Node(m) => {
    //             let id = ids[0];
    //             let entry = m.entry(id);
    //             match entry {
    //                 Entry::Occupied(mut e) => {
    //                     if ids[1..].is_empty() {
    //                         e.insert(Trie::Data(data.len(), data.to_vec().into()));
    //                     } else {
    //                         e.get_mut().insert(&ids[1..], data);
    //                     }
    //                 }
    //                 Entry::Vacant(e) => {
    //                     if ids[1..].is_empty() {
    //                         e.insert(Trie::Data(data.len(), data.to_vec().into()));
    //                     } else {
    //                         e.insert(Trie::Vec(ids[1..].to_vec(), data.to_vec().into()));
    //                     }
    //                 }
    //             }
    //         }
    //         Trie::Data(arity, vec) => {
    //             assert_eq!(*arity, data.len());
    //             vec.extend(data.iter().cloned());
    //         }
    //     }

    //     let mut trie = self;
    //     for id in &ids[..ids.len() - 1] {
    //         trie = trie.get_map_mut().entry(*id).or_default();
    //     }

    //     match trie.get_map_mut().entry(ids[ids.len() - 1]) {
    //         Entry::Vacant(e) => {
    //             e.insert(Trie::Data(data.len(), data.into()));
    //         }
    //         Entry::Occupied(mut e) => match e.get_mut() {
    //             Trie::Node(_) | Trie::Vec(..) => unreachable!(),
    //             Trie::Data(arity, vec) => {
    //                 assert_eq!(*arity, data.len());
    //                 vec.extend(data.iter().cloned());
    //             }
    //         },
    //     }
    // }
}
