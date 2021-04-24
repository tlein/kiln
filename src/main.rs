use std::{
    any::Any,
    collections::HashMap,
    fmt::Debug,
    marker::{Send, Sync},
    sync::{Arc, Condvar, Mutex, MutexGuard},
    thread, time,
};

fn main() {}

#[test]
fn test_locking_consistency() {
    let library = Library::default();
    library.register::<Person>();
    let person_catalog = library.checkout::<Person>();
    let person_id = person_catalog.create(Person::default());

    library.register::<Dog>();
    let dog_catalog = library.checkout::<Dog>();
    let dog_id = dog_catalog.create(Dog::default());

    let thread_count = 10;
    let threads = (0..thread_count)
        .map(|_| {
            thread::spawn({
                let library_copy = library.clone();
                move || {
                    let person_catalog = library_copy.checkout::<Person>();
                    let locked_person = person_catalog.lock(person_id);
                    let mut writable_person = locked_person.value.clone();
                    writable_person.age = writable_person.age + 1;
                    thread::sleep(time::Duration::from_millis(1));
                    person_catalog.commit(&locked_person, writable_person);

                    let dog_catalog = library_copy.checkout::<Dog>();
                    let locked_dog = dog_catalog.lock(dog_id);
                    let mut writable_dog = locked_dog.value.clone();
                    writable_dog.dog_years = writable_dog.dog_years + 7;
                    thread::sleep(time::Duration::from_millis(1));
                    dog_catalog.commit(&locked_dog, writable_dog);
                }
            })
        })
        .collect::<Vec<_>>();

    for t in threads {
        t.join().unwrap();
    }

    assert_eq!(thread_count, person_catalog.get(person_id).age);
    assert_eq!(thread_count * 7, dog_catalog.get(dog_id).dog_years);
}

#[derive(Clone, Debug, Default)]
struct Library {
    catalogs: Arc<Mutex<HashMap<String, Arc<dyn Any + Send + Sync>>>>,
}

impl Library {
    pub fn register<R>(&self)
    where
        R: Record,
    {
        self.catalogs.lock().unwrap().insert(
            R::type_name().to_string(),
            Arc::from(CatalogInner::<R>::default()),
        );
    }

    pub fn checkout<R>(&self) -> Catalog<R>
    where
        R: Record,
    {
        let library_catalog = self
            .catalogs
            .lock()
            .unwrap()
            .get(R::type_name())
            .unwrap()
            .clone()
            .downcast::<CatalogInner<R>>()
            .unwrap();
        Catalog {
            inner: library_catalog,
            reads: Default::default(),
        }
    }
}

#[derive(Default)]
struct Catalog<R>
where
    R: Record,
{
    inner: Arc<CatalogInner<R>>,
    reads: Mutex<Vec<Arc<R>>>,
}

#[derive(Debug, Default)]
struct CatalogInner<R>
where
    R: Record,
{
    locks_cv: Condvar,
    state: Mutex<CatalogState<R>>,
}

#[derive(Debug, Default)]
struct CatalogState<R>
where
    R: Record,
{
    records: Vec<Arc<R>>,
    locks: Vec<bool>,
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

struct Locked<'a, R>
where
    R: Record,
{
    id: usize,
    value: &'a R,
    catalog: &'a Catalog<R>,
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

trait Record: 'static + Clone + Debug + Default + Send + Sync {
    fn type_name() -> &'static str;
}
#[derive(Copy, Clone, Debug, Default)]
struct Person {
    age: i32,
}
impl Record for Person {
    fn type_name() -> &'static str {
        "Person"
    }
}
#[derive(Copy, Clone, Debug, Default)]
struct Dog {
    dog_years: i32,
}
impl Record for Dog {
    fn type_name() -> &'static str {
        "Dog"
    }
}
