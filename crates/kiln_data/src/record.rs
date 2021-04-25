use crate::catalog::Catalog;
use std::{
    fmt::Debug,
    marker::{Send, Sync},
};

pub trait Record: 'static + Clone + Debug + Default + Send + Sync {
    fn type_name() -> &'static str;
}

pub struct Locked<'a, R>
where
    R: Record,
{
    pub id: usize,
    pub value: &'a R,
    pub(crate) catalog: &'a Catalog<R>,
}
impl<'a, R> Drop for Locked<'a, R>
where
    R: Record,
{
    fn drop(&mut self) {
        let inner = &self.catalog.inner;
        let mut state = inner.state.lock().unwrap();
        state.locks[self.id] = false;
        inner.locks_cv.notify_all();
    }
}
