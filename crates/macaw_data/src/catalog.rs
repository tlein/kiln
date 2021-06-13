use crate::{
    library::Sequencer,
    record::{Locked, Record, RecordId, RecordWrapper},
};
use std::{
    fmt::Debug,
    sync::{Arc, Condvar, Mutex, MutexGuard},
};

#[derive(Default)]
pub struct Catalog<R>
where
    R: Record,
{
    pub(crate) state: Arc<CatalogState<R>>,
    pub(crate) reads: Mutex<Vec<Arc<RecordWrapper<R>>>>,
    pub(crate) sequencer: Sequencer,
}

#[derive(Debug, Default)]
pub(crate) struct CatalogState<R>
where
    R: Record,
{
    pub(crate) locks_cv: Condvar,
    pub(crate) inner: Mutex<CatalogStateInner<R>>,
}

#[derive(Clone, Debug)]
pub(crate) struct ChangeRecord<R>
where
    R: Record,
{
    pub(crate) record_id: RecordId,
    pub lsn: u64,
    pub(crate) old_record: Option<Arc<RecordWrapper<R>>>,
    pub(crate) new_record: Arc<RecordWrapper<R>>,
}

#[derive(Debug, Default)]
pub(crate) struct CatalogStateInner<R>
where
    R: Record,
{
    pub(crate) locks: Vec<bool>,
    pub(crate) change_log: Vec<ChangeRecord<R>>,
    records: Vec<Arc<RecordWrapper<R>>>,
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
        let mut state = self.state.inner.lock().unwrap();
        let id = state.records.len();
        let record_wrapper = Arc::from(record_wrapper);
        state.records.push(record_wrapper.clone());
        state.locks.push(false);
        let record_id = RecordId(id);
        self.write_change_log(record_id, None, record_wrapper, state);
        record_id
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
        let mut state = self.state.inner.lock().unwrap();
        if lock {
            state = self
                .state
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
        unsafe {
            let record_ref = Arc::as_ptr(record_wrapper).as_ref().unwrap();
            &<&RecordWrapper<R>>::clone(&record_ref).inner
        }
    }

    pub fn unlock(&self, id: RecordId) {
        let mut state = self.state.inner.lock().unwrap();
        state.locks[id.0] = false;
        self.state.locks_cv.notify_all();
    }

    pub fn commit(&self, locked: &Locked<R>, new_record: R) {
        let old_record = self.get_internal(locked.id, false);
        self.commit_internal(locked.id, old_record, new_record)
    }

    fn commit_internal(&self, id: RecordId, old_record: Arc<RecordWrapper<R>>, new_record: R) {
        let old_prototype_instances = old_record.prototype_instances.lock().unwrap();
        let new_instance = Arc::from(RecordWrapper {
            prototype_id: old_record.prototype_id,
            prototype_instances: Mutex::from(old_prototype_instances.clone()),
            inner: new_record,
        });

        let mut state_inner = self.state.inner.lock().unwrap();
        state_inner.records[id.0] = new_instance.clone();
        self.write_change_log(
            id,
            Some(old_record.clone()),
            new_instance.clone(),
            state_inner,
        );

        for instance_id in old_prototype_instances.iter() {
            let instance_wrapper = self.get_internal(*instance_id, true);
            let new_instance = instance_wrapper
                .inner
                .proto_update(&old_record.inner, &new_instance.inner);
            self.commit_internal(*instance_id, instance_wrapper, new_instance);
            self.unlock(*instance_id);
        }
    }

    fn write_change_log(
        &self,
        id: RecordId,
        old_record: Option<Arc<RecordWrapper<R>>>,
        new_record: Arc<RecordWrapper<R>>,
        mut state_inner: MutexGuard<CatalogStateInner<R>>,
    ) {
        let lsn = self.sequencer.next();
        state_inner.change_log.push(ChangeRecord {
            record_id: id,
            old_record,
            new_record,
            lsn,
        });
    }
}
