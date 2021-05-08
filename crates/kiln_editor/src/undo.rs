use kiln_data::{Library, Record, RecordId, Watermark};
use std::{boxed::Box, marker::PhantomData};

trait Undoable {
    fn undo(&mut self, library: &Library);
    fn redo(&mut self, library: &Library);
}

struct UndoRecord<R>
where
    R: Record,
{
    pub record_id: RecordId,
    pub old_record: Option<R>,
    pub new_record: R,
}

impl<R> Undoable for UndoRecord<R>
where
    R: Record,
{
    fn undo(&mut self, library: &Library) {
        if let Some(record) = &self.old_record {
            let catalog = library.checkout::<R>();
            let lock = catalog.lock(self.record_id);
            catalog.commit(&lock, record.clone());
        }
    }

    fn redo(&mut self, library: &Library) {
        let catalog = library.checkout::<R>();
        let lock = catalog.lock(self.record_id);
        catalog.commit(&lock, self.new_record.clone());
    }
}

trait Watcher {
    fn consume_change_log(&self, library: &Library) -> Vec<Box<dyn Undoable>>;
}
struct WatcherState<R>
where
    R: Record,
{
    cur_watermark: Watermark,
    phantom: PhantomData<R>,
}

impl<R> WatcherState<R>
where
    R: Record,
{
    pub fn new(library: &Library) -> WatcherState<R> {
        let catalog = library.checkout::<R>();
        let cur_watermark = catalog.watermark();
        WatcherState {
            cur_watermark,
            phantom: Default::default(),
        }
    }
}

impl<R> Watcher for WatcherState<R>
where
    R: Record,
{
    fn consume_change_log(&self, library: &Library) -> Vec<Box<dyn Undoable>> {
        let catalog = library.checkout::<R>();
        let new_watermark = catalog.watermark();
        let mut undoables: Vec<Box<dyn Undoable>> = vec![];
        for change in catalog.changes(self.cur_watermark, new_watermark) {
            undoables.push(Box::from(UndoRecord {
                record_id: change.record_id(),
                old_record: match change.old_record() {
                    Some(record_ref) => Some(record_ref.clone()),
                    None => None,
                },
                new_record: change.new_record().clone(),
            }));
        }

        return undoables;
    }
}

pub struct UndoRedo {
    library: Library,
    undo_stack: Vec<Box<dyn Undoable>>,
    redo_stack: Vec<Box<dyn Undoable>>,
    watchers: Vec<Box<dyn Watcher>>,
}

impl UndoRedo {
    pub fn new(library: Library) -> UndoRedo {
        UndoRedo {
            library,
            undo_stack: Default::default(),
            redo_stack: Default::default(),
            watchers: Default::default(),
        }
    }

    pub fn watch<R>(&mut self)
    where
        R: Record,
    {
        self.watchers
            .push(Box::from(WatcherState::<R>::new(&self.library)));
    }

    pub fn undo(&mut self) {
        self.consume_change_logs();
        if let Some(mut top) = self.undo_stack.pop() {
            top.undo(&self.library);
            self.redo_stack.push(top);
        }
    }

    pub fn redo(&mut self) {
        self.consume_change_logs();
        if let Some(mut top) = self.redo_stack.pop() {
            top.redo(&self.library);
            self.undo_stack.push(top);
        }
    }

    fn consume_change_logs(&mut self) {
        // TODO #4 (https://github.com/tlein/kiln/issues/4):
        // be aware of commit timestamps to preserve modification order between
        // catalogs
        for watcher in &self.watchers {
            let new_changes = &mut watcher.consume_change_log(&self.library);
            // TODO #5 (https://github.com/tlein/kiln/issues/5)
            // if new_changes contains non-undo changes, then clear redo_stack
            self.undo_stack.append(new_changes);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::UndoRedo;
    use kiln_data::{proto_update_field, Library, Record};

    #[test]
    fn test_undo_redo() {
        let library = Library::default();
        library.register::<Person>();
        let mut undo_redo = UndoRedo::new(library.clone());
        undo_redo.watch::<Person>();
        let catalog = library.checkout::<Person>();

        let id = catalog.create(Person::new(String::from("Tucker"), 29));

        {
            let person = catalog.lock(id);
            let mut write = person.value.clone();
            write.name = String::from("TuckerWrongName");
            catalog.commit(&person, write);
        }

        assert_eq!(String::from("TuckerWrongName"), catalog.get(id).name);

        undo_redo.undo();
        assert_eq!(String::from("Tucker"), catalog.get(id).name);

        undo_redo.redo();
        assert_eq!(String::from("TuckerWrongName"), catalog.get(id).name);
    }

    #[derive(Clone, Debug, Default)]
    pub(crate) struct Person {
        pub(crate) age: i32,
        pub(crate) name: String,
    }
    impl Person {
        fn new(name: String, age: i32) -> Person {
            Person { name, age }
        }
    }
    impl Record for Person {
        fn type_name() -> &'static str {
            "Person"
        }

        fn proto_update(&self, old: &Person, new: &Person) -> Person {
            return Person {
                age: *proto_update_field(&self.age, &old.age, &new.age),
                name: proto_update_field(&self.name, &old.name, &new.name).clone(),
            };
        }
    }
}
