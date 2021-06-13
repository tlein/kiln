use macaw_data::{Library, Record, RecordId, Watermark};
use std::{boxed::Box, fmt::Debug, marker::PhantomData};

trait Undoable: Debug {
    fn undo(&mut self, library: &Library);
    fn redo(&mut self, library: &Library);
    fn lsn(&self) -> u64;
}

#[derive(Debug)]
struct UndoRecord<R>
where
    R: Record,
{
    pub record_id: RecordId,
    pub old_record: Option<R>,
    pub new_record: R,
    pub lsn: u64,
}

impl<R> Undoable for UndoRecord<R>
where
    R: Record,
{
    fn undo(&mut self, library: &Library) {
        if let Some(old_record) = &self.old_record {
            let catalog = library.checkout::<R>();
            let lock = catalog.lock(self.record_id);
            catalog.commit(&lock, old_record.clone());
        }
    }

    fn redo(&mut self, library: &Library) {
        let catalog = library.checkout::<R>();
        let lock = catalog.lock(self.record_id);
        catalog.commit(&lock, self.new_record.clone());
    }

    fn lsn(&self) -> u64 {
        self.lsn
    }
}

#[derive(Debug)]
struct UndoableBundle {
    undoables: Vec<Box<dyn Undoable>>,
}

impl Undoable for UndoableBundle {
    fn undo(&mut self, library: &Library) {
        for undoable in &mut self.undoables.iter_mut().rev() {
            (*undoable).undo(library);
        }
    }

    fn redo(&mut self, library: &Library) {
        for undoable in &mut self.undoables {
            (*undoable).redo(library);
        }
    }

    fn lsn(&self) -> u64 {
        if self.undoables.is_empty() {
            panic!("UndoableBundle cannot be empty!");
        }

        self.undoables.last().unwrap().lsn()
    }
}

trait Watcher {
    fn consume_change_log(&mut self, library: &Library) -> Vec<Box<dyn Undoable>>;
    fn advance_watermark(&mut self, library: &Library);
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
    fn consume_change_log(&mut self, library: &Library) -> Vec<Box<dyn Undoable>> {
        let catalog = library.checkout::<R>();
        let new_watermark = catalog.watermark();
        let mut undoables: Vec<Box<dyn Undoable>> = vec![];
        for change in catalog.changes(self.cur_watermark, new_watermark) {
            undoables.push(Box::from(UndoRecord {
                record_id: change.record_id(),
                old_record: change.old_record().cloned(),
                new_record: change.new_record().clone(),
                lsn: change.lsn(),
            }));
        }

        self.cur_watermark = new_watermark;

        undoables
    }

    fn advance_watermark(&mut self, library: &Library) {
        let catalog = library.checkout::<R>();
        let new_watermark = catalog.watermark();
        self.cur_watermark = new_watermark;
    }
}

pub struct PauseScope<'a> {
    undo_redo: &'a mut UndoRedo,
}

impl Drop for PauseScope<'_> {
    fn drop(&mut self) {
        self.undo_redo.advance_watermarks();
    }
}

pub struct CombineScope<'a> {
    undo_redo: &'a mut UndoRedo,
}

impl Drop for CombineScope<'_> {
    fn drop(&mut self) {
        self.undo_redo.drop_combine_scope();
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
            self.advance_watermarks();
        }
    }

    pub fn redo(&mut self) {
        self.consume_change_logs();
        if let Some(mut top) = self.redo_stack.pop() {
            top.redo(&self.library);
            self.undo_stack.push(top);
            self.advance_watermarks();
        }
    }

    pub fn pause_scope(&mut self) -> PauseScope {
        self.consume_change_logs();
        PauseScope { undo_redo: self }
    }

    pub fn combine_scope(&mut self) -> CombineScope {
        self.consume_change_logs();
        CombineScope { undo_redo: self }
    }

    fn advance_watermarks(&mut self) {
        for watcher in &mut self.watchers {
            watcher.advance_watermark(&self.library);
        }
    }

    fn drop_combine_scope(&mut self) {
        let undoables = self.undoables_for_consumption();
        if !undoables.is_empty() {
            self.undo_stack
                .push(Box::from(UndoableBundle { undoables }));
        }
    }

    fn consume_change_logs(&mut self) {
        let mut undoables = self.undoables_for_consumption();
        self.undo_stack.append(&mut undoables);
    }

    fn undoables_for_consumption(&mut self) -> Vec<Box<dyn Undoable>> {
        let mut undoables: Vec<Box<dyn Undoable>> = Default::default();
        for watcher in &mut self.watchers {
            let new_changes = &mut watcher.consume_change_log(&self.library);
            if !new_changes.is_empty() {
                self.redo_stack.clear();
            }
            undoables.append(new_changes);
        }
        undoables.sort_by(|a, b| a.lsn().partial_cmp(&b.lsn()).unwrap());

        undoables
    }
}

#[cfg(test)]
mod tests {
    use crate::UndoRedo;
    use macaw_data::{proto_update_field, Library, Record};

    #[test]
    fn test_undo_redo() {
        let library = Library::default();
        library.register::<Person>();
        let mut undo_redo = UndoRedo::new(library.clone());
        undo_redo.watch::<Person>();
        let catalog = library.checkout::<Person>();

        let id = catalog.create(Person::new(29, String::from("0")));

        {
            let person = catalog.lock(id);
            let mut write = person.value.clone();
            write.name = String::from("1");
            catalog.commit(&person, write);
        }

        assert_eq!(String::from("1"), catalog.get(id).name);

        undo_redo.undo();
        assert_eq!(String::from("0"), catalog.get(id).name);

        undo_redo.redo();
        assert_eq!(String::from("1"), catalog.get(id).name);

        {
            let person = catalog.lock(id);
            let mut write = person.value.clone();
            write.name = String::from("2");
            catalog.commit(&person, write);
        }

        undo_redo.undo();
        assert_eq!(String::from("1"), catalog.get(id).name);
        undo_redo.undo();
        assert_eq!(String::from("0"), catalog.get(id).name);
    }

    #[test]
    fn test_clear_redo_stack() {
        let library = Library::default();
        library.register::<Person>();
        let mut undo_redo = UndoRedo::new(library.clone());
        undo_redo.watch::<Person>();
        let catalog = library.checkout::<Person>();

        let id = catalog.create(Person::new(29, String::from("0")));

        {
            let person = catalog.lock(id);
            let mut write = person.value.clone();
            write.name = String::from("1");
            catalog.commit(&person, write);
        }

        assert_eq!(String::from("1"), catalog.get(id).name);

        undo_redo.undo();
        assert_eq!(String::from("0"), catalog.get(id).name);

        {
            let person = catalog.lock(id);
            let mut write = person.value.clone();
            write.name = String::from("2");
            catalog.commit(&person, write);
        }

        undo_redo.redo();
        assert_eq!(String::from("2"), catalog.get(id).name);
    }

    #[test]
    fn test_pause_scope() {
        let library = Library::default();
        library.register::<Person>();
        let mut undo_redo = UndoRedo::new(library.clone());
        undo_redo.watch::<Person>();
        let catalog = library.checkout::<Person>();

        let id = catalog.create(Person::new(29, String::from("0")));

        {
            let person = catalog.lock(id);
            let mut write = person.value.clone();
            write.name = String::from("1");
            catalog.commit(&person, write);
        }

        assert_eq!(String::from("1"), catalog.get(id).name);

        {
            let _pause_scope = undo_redo.pause_scope();
            let person = catalog.lock(id);
            let mut write = person.value.clone();
            write.name = String::from("2");
            catalog.commit(&person, write);
        }

        undo_redo.undo();
        assert_eq!(String::from("0"), catalog.get(id).name);
        undo_redo.redo();
        assert_eq!(String::from("1"), catalog.get(id).name);
    }

    #[test]
    fn test_combine_scope() {
        let library = Library::default();
        library.register::<Person>();
        let mut undo_redo = UndoRedo::new(library.clone());
        undo_redo.watch::<Person>();
        let catalog = library.checkout::<Person>();

        let id = catalog.create(Person::new(29, String::from("0")));

        {
            let person = catalog.lock(id);
            let mut write = person.value.clone();
            write.name = String::from("1");
            catalog.commit(&person, write);
        }

        assert_eq!(String::from("1"), catalog.get(id).name);

        {
            let _combine_scope = undo_redo.combine_scope();
            let person = catalog.lock(id);
            let mut write = person.value.clone();
            write.name = String::from("2");
            catalog.commit(&person, write);
            let mut write = person.value.clone();
            write.name = String::from("3");
            catalog.commit(&person, write);
            let mut write = person.value.clone();
            write.name = String::from("4");
            catalog.commit(&person, write);
        }

        undo_redo.undo();
        assert_eq!(String::from("1"), catalog.get(id).name);

        undo_redo.redo();
        assert_eq!(String::from("4"), catalog.get(id).name);
    }

    #[test]
    fn test_multiple_record_type_order() {
        let library = Library::default();
        library.register::<Person>();
        library.register::<Dog>();
        let mut undo_redo = UndoRedo::new(library.clone());
        undo_redo.watch::<Person>();
        undo_redo.watch::<Dog>();
        let person_catalog = library.checkout::<Person>();
        let dog_catalog = library.checkout::<Dog>();

        let person_id = person_catalog.create(Person::new(29, String::from("Tucker")));
        let dog_id = dog_catalog.create(Dog::new(String::from("Red Heeler")));

        {
            let dog = dog_catalog.lock(dog_id);
            let mut write = dog.value.clone();
            write.breed = String::from("Blue Heeler");
            dog_catalog.commit(&dog, write);
        }

        {
            let person = person_catalog.lock(person_id);
            let mut write = person.value.clone();
            write.name = String::from("Jim");
            person_catalog.commit(&person, write);
        }

        undo_redo.undo();

        assert_eq!(String::from("Tucker"), person_catalog.get(person_id).name);
        assert_eq!(String::from("Blue Heeler"), dog_catalog.get(dog_id).breed);

        undo_redo.undo();

        assert_eq!(String::from("Tucker"), person_catalog.get(person_id).name);
        assert_eq!(String::from("Red Heeler"), dog_catalog.get(dog_id).breed);
    }

    #[derive(Clone, Debug, Default)]
    struct Person {
        age: i32,
        name: String,
    }
    impl Person {
        fn new(age: i32, name: String) -> Person {
            Person { age, name }
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
    #[derive(Clone, Debug, Default)]
    struct Dog {
        breed: String,
    }
    impl Dog {
        fn new(breed: String) -> Dog {
            Dog { breed }
        }
    }
    impl Record for Dog {
        fn type_name() -> &'static str {
            "Dog"
        }

        fn proto_update(&self, old: &Dog, new: &Dog) -> Dog {
            return Dog {
                breed: proto_update_field(&self.breed, &old.breed, &new.breed).clone(),
            };
        }
    }
}
