use crate::record::{Locked, Record, RecordId, RecordWrapper};
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
    pub(crate) reads: Mutex<Vec<Arc<RecordWrapper<R>>>>,
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
    records: Vec<Arc<RecordWrapper<R>>>,
    pub(crate) locks: Vec<bool>,
}

impl<R> Catalog<R>
where
    R: Record,
{
    pub fn create(&self, record: R) -> RecordId {
        self.create_internal(RecordWrapper {
            prototype_id: None,
            prototype_instances: Default::default(),
            inner: record,
        })
    }

    pub fn create_from_prototype(&self, prototype_id: RecordId) -> RecordId {
        let prototype_wrapper = self.get_internal(prototype_id, true);
        let instance = prototype_wrapper.inner.clone();
        let instance_id = self.create_internal(RecordWrapper {
            prototype_id: Some(prototype_id),
            prototype_instances: Default::default(),
            inner: instance,
        });

        prototype_wrapper
            .prototype_instances
            .lock()
            .unwrap()
            .insert(instance_id);

        self.unlock(prototype_id);
        instance_id
    }

    fn create_internal(&self, record_wrapper: RecordWrapper<R>) -> RecordId {
        let mut state = self.inner.state.lock().unwrap();
        let id = state.records.len();
        state.records.push(Arc::from(record_wrapper));
        state.locks.push(false);
        RecordId(id)
    }

    pub fn get(&self, id: RecordId) -> &R {
        self.unwrap_record_wrapper(&self.get_internal(id, false))
    }

    pub fn lock(&self, id: RecordId) -> Locked<R> {
        Locked {
            id,
            value: self.unwrap_record_wrapper(&self.get_internal(id, true)),
            catalog: self,
        }
    }

    fn get_internal(&self, id: RecordId, lock: bool) -> Arc<RecordWrapper<R>> {
        let mut state = self.inner.state.lock().unwrap();
        if lock {
            state = self
                .inner
                .locks_cv
                .wait_while(state, |library| library.locks[id.0])
                .unwrap();
            state.locks[id.0] = true;
        }

        let record = &state.records[id.0];
        record.clone()
    }

    fn unwrap_record_wrapper(&self, record_wrapper: &Arc<RecordWrapper<R>>) -> &R {
        self.reads.lock().unwrap().push(record_wrapper.clone());
        unsafe { &Arc::as_ptr(record_wrapper).as_ref().unwrap().clone().inner }
    }

    pub fn unlock(&self, id: RecordId) {
        let mut state = self.inner.state.lock().unwrap();
        state.locks[id.0] = false;
        self.inner.locks_cv.notify_all();
    }

    pub fn commit(&self, locked: &Locked<R>, new_record: R) {
        let old_record = self.get_internal(locked.id, false);
        self.commit_internal(locked.id, old_record.as_ref(), new_record)
    }

    fn commit_internal(&self, id: RecordId, old_record: &RecordWrapper<R>, new_record: R) {
        let old_prototype_instances = old_record.prototype_instances.lock().unwrap();
        let new_instance = Arc::from(RecordWrapper {
            prototype_id: old_record.prototype_id,
            prototype_instances: Mutex::from(old_prototype_instances.clone()),
            inner: new_record,
        });

        {
            let mut state = self.inner.state.lock().unwrap();
            state.records[id.0] = new_instance.clone();
        }

        for instance_id in old_prototype_instances.iter() {
            let instance_wrapper = self.get_internal(*instance_id, true);
            let new_instance = instance_wrapper
                .inner
                .merge(&old_record.inner, &new_instance.inner);
            self.commit_internal(*instance_id, &instance_wrapper, new_instance);
            self.unlock(*instance_id);
        }
    }
}
