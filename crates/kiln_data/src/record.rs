use crate::catalog::Catalog;
use std::{
    collections::HashSet,
    fmt::Debug,
    marker::{Send, Sync},
    sync::Mutex,
};

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub struct RecordId(pub usize);

#[derive(Debug)]
pub(crate) struct RecordWrapper<R>
where
    R: Record,
{
    pub(crate) prototype_id: Option<RecordId>,
    pub(crate) prototype_instances: Mutex<HashSet<RecordId>>,
    pub(crate) inner: R,
}

pub trait Record: 'static + Clone + Debug + Default + Send + Sync {
    fn type_name() -> &'static str;
    fn merge(&self, old_prototype: &Self, new_prototype: &Self) -> Self;
}

pub struct Locked<'a, R>
where
    R: Record,
{
    pub id: RecordId,
    pub value: &'a R,
    pub(crate) catalog: &'a Catalog<R>,
}
impl<'a, R> Drop for Locked<'a, R>
where
    R: Record,
{
    fn drop(&mut self) {
        self.catalog.unlock(self.id);
    }
}

pub fn merge_field<'a, T>(
    instance_field: &'a T,
    old_prototype_field: &'a T,
    new_prototype_field: &'a T,
) -> &'a T
where
    T: PartialEq + Eq,
{
    if old_prototype_field != instance_field {
        instance_field
    } else {
        new_prototype_field
    }
}
