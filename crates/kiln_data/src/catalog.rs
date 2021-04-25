use crate::record::{Locked, Record};
use std::{
    fmt::Debug,
    sync::{Arc, Condvar, Mutex},
};

#[derive(Default)]
pub struct Catalog<R>
where
    R: Record,
{
    pub(crate) inner: Arc<CatalogInner<R>>,
    pub(crate) reads: Mutex<Vec<Arc<R>>>,
}

#[derive(Debug, Default)]
pub(crate) struct CatalogInner<R>
where
    R: Record,
{
    pub(crate) locks_cv: Condvar,
    pub(crate) state: Mutex<CatalogState<R>>,
}

#[derive(Debug, Default)]
pub(crate) struct CatalogState<R>
where
    R: Record,
{
    records: Vec<Arc<R>>,
    pub(crate) locks: Vec<bool>,
}

impl<R> Catalog<R>
where
    R: Record,
{
    pub fn create(&self, record: R) -> usize {
        let mut state = self.inner.state.lock().unwrap();
        let id = state.records.len();
        state.records.push(Arc::from(record));
        state.locks.push(false);
        id
    }

    fn get_internal(&self, id: usize, lock: bool) -> &R {
        let mut state = self.inner.state.lock().unwrap();
        if lock {
            state = self
                .inner
                .locks_cv
                .wait_while(state, |library| library.locks[id])
                .unwrap();
        }

        state.locks[id] = true;
        let record = &state.records[id];
        self.reads.lock().unwrap().push(record.clone());
        unsafe { Arc::as_ptr(record).as_ref().unwrap() }
    }

    pub fn get(&self, id: usize) -> &R {
        self.get_internal(id, false)
    }

    pub fn lock(&self, id: usize) -> Locked<R> {
        Locked {
            id,
            value: self.get_internal(id, true),
            catalog: self,
        }
    }

    pub fn commit(&self, locked: &Locked<R>, new_record: R) {
        let mut state = self.inner.state.lock().unwrap();
        state.records[locked.id] = Arc::from(new_record);
    }
}
