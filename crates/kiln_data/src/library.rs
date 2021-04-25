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
    use crate::{merge_field, Library, Record};
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

    #[test]
    fn test_prototypes() {
        let library = Library::default();
        library.register::<Person>();
        let catalog = library.checkout::<Person>();
        let proto_id = catalog.create(Person::default());
        let instance_id = catalog.create_from_prototype(proto_id);

        {
            let person = catalog.lock(proto_id);
            let mut write = person.value.clone();
            write.age = 20;
            write.name = String::from("Atom");
            catalog.commit(&person, write);
        }

        assert_eq!(String::from("Atom"), catalog.get(proto_id).name);
        assert_eq!(String::from("Atom"), catalog.get(instance_id).name);

        {
            let person = catalog.lock(instance_id);
            let mut write = person.value.clone();
            write.name = String::from("Eva");
            catalog.commit(&person, write);
        }

        assert_eq!(String::from("Atom"), catalog.get(proto_id).name);
        assert_eq!(String::from("Eva"), catalog.get(instance_id).name);
    }

    #[test]
    fn test_prototypes_consistency() {
        assert_eq!(true, true);

        //TODO:
        // one thread that will constantly update the prototype and check that the prototype and child have expected state
        // another thread that will constantly update the child and check that the updates to the child have the expected state
        // a third thread that will constantly update the child's child and check that the updates to the child's child have the expected state
    }

    #[derive(Clone, Debug, Default)]
    struct Dog {
        dog_years: i32,
    }
    impl Record for Dog {
        fn type_name() -> &'static str {
            "Dog"
        }

        fn merge(&self, old: &Dog, new: &Dog) -> Dog {
            return Dog {
                dog_years: *merge_field(&self.dog_years, &old.dog_years, &new.dog_years),
            };
        }
    }

    #[derive(Clone, Debug, Default)]
    struct Person {
        age: i32,
        name: String,
    }
    impl Record for Person {
        fn type_name() -> &'static str {
            "Person"
        }

        fn merge(&self, old: &Person, new: &Person) -> Person {
            return Person {
                age: *merge_field(&self.age, &old.age, &new.age),
                name: merge_field(&self.name, &old.name, &new.name).clone(),
            };
        }
    }
}
