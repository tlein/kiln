use crate::catalog::{Catalog, ChangeRecord};
use crate::record::{Record, RecordId};
use std::{iter::Iterator, marker::PhantomData};

#[derive(Copy, Clone)]
pub struct Watermark(usize);

pub struct Change<'a, R>
where
    R: Record,
{
    phantom: PhantomData<&'a ()>,
    inner: ChangeRecord<R>,
}

impl<'a, R> Change<'a, R>
where
    R: Record,
{
    pub fn record_id(&self) -> RecordId {
        self.inner.record_id
    }

    pub fn old_record(&self) -> Option<&R> {
        match &self.inner.old_record {
            None => None,
            Some(r) => Some(&r.inner),
        }
    }

    pub fn new_record(&self) -> &R {
        &self.inner.new_record.inner
    }
}

pub struct CatalogIterator<'a, R>
where
    R: Record,
{
    catalog: &'a Catalog<R>,
    cur_watermark: Watermark,
    end_watermark: Watermark,
}

impl<'a, R> Iterator for CatalogIterator<'a, R>
where
    R: Record,
{
    type Item = Change<'a, R>;

    fn next(&mut self) -> Option<Change<'a, R>> {
        if self.end_watermark.0 <= self.cur_watermark.0 {
            return None;
        }

        let state = self.catalog.state.inner.lock().unwrap();
        let change_record = state.change_log[self.cur_watermark.0].clone();
        self.cur_watermark.0 += 1;
        Some(Change {
            phantom: PhantomData::default(),
            inner: change_record,
        })
    }
}

impl<R> Catalog<R>
where
    R: Record,
{
    pub fn changes(&self, start_point: Watermark, end_point: Watermark) -> CatalogIterator<R> {
        CatalogIterator {
            catalog: self,
            cur_watermark: start_point,
            end_watermark: end_point,
        }
    }

    pub fn watermark(&self) -> Watermark {
        Watermark(self.state.inner.lock().unwrap().change_log.len())
    }
}

#[cfg(test)]
mod tests {
    use crate::{tests::Person, Library};
    use std::iter::FromIterator;

    #[test]
    fn test_change_detection() {
        let library = Library::default();
        library.register::<Person>();
        let catalog = library.checkout::<Person>();
        let start_watermark = catalog.watermark();

        let id = catalog.create(Person {
            name: String::from("Name0"),
            age: 0,
            fav_food: String::default(),
        });

        {
            let person = catalog.lock(id);
            let mut write = person.value.clone();
            write.name = String::from("Name1");
            catalog.commit(&person, write);
        }

        {
            let person = catalog.lock(id);
            let mut write = person.value.clone();
            write.name = String::from("Name2");
            catalog.commit(&person, write);
        }

        let end_watermark = catalog.watermark();

        let changes = Vec::from_iter(catalog.changes(start_watermark, end_watermark));
        assert_eq!(3, changes.len());
        assert_eq!(true, changes[0].inner.old_record.is_none());
        assert_eq!(
            String::from("Name0"),
            changes[0].inner.new_record.inner.name
        );
        assert_eq!(
            String::from("Name0"),
            changes[1].inner.old_record.as_ref().unwrap().inner.name
        );
        assert_eq!(
            String::from("Name1"),
            changes[1].inner.new_record.inner.name
        );
        assert_eq!(
            String::from("Name1"),
            changes[2].inner.old_record.as_ref().unwrap().inner.name
        );
        assert_eq!(
            String::from("Name2"),
            changes[2].inner.new_record.inner.name
        );
    }
}
