use crate::{
    catalog::{Catalog, CatalogInner},
    record::Record,
};
use std::{
    any::Any,
    collections::HashMap,
    fmt::Debug,
    marker::{Send, Sync},
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug, Default)]
pub struct Library {
    pub catalogs: Arc<Mutex<HashMap<String, Arc<dyn Any + Send + Sync>>>>,
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

#[cfg(test)]
mod tests {
    use crate::{Library, Record};
    use std::{thread, time};

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
    struct Dog {
        dog_years: i32,
    }
    impl Record for Dog {
        fn type_name() -> &'static str {
            "Dog"
        }
    }

    #[derive(Clone, Debug, Default)]
    struct Person {
        age: i32,
    }
    impl Record for Person {
        fn type_name() -> &'static str {
            "Person"
        }
    }
}
