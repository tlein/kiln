use crate::{
    catalog::{Catalog, CatalogState},
    record::Record,
};
use std::{
    any::Any,
    collections::HashMap,
    fmt::Debug,
    marker::{Send, Sync},
    sync::{atomic::AtomicU64, atomic::Ordering, Arc, Mutex},
};

#[derive(Clone, Debug, Default)]
pub struct Library {
    pub catalogs: Arc<Mutex<HashMap<String, Arc<dyn Any + Send + Sync>>>>,
    sequencer: Sequencer,
}

impl Library {
    pub fn register<R>(&self)
    where
        R: Record,
    {
        self.catalogs.lock().unwrap().insert(
            R::type_name().to_string(),
            Arc::from(CatalogState::<R>::default()),
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
            .downcast::<CatalogState<R>>()
            .unwrap();
        Catalog {
            state: library_catalog,
            reads: Default::default(),
            sequencer: self.sequencer.clone(),
        }
    }
}

#[derive(Default, Clone, Debug)]
pub(crate) struct Sequencer {
    next_lsn: Arc<AtomicU64>,
}

impl Sequencer {
    pub fn next(&self) -> u64 {
        self.next_lsn.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::{proto_update_field, Library, Record};
    use rand::{distributions::Alphanumeric, Rng};
    use std::{
        collections::HashSet,
        thread,
        time::{Duration, Instant},
    };

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
                        writable_person.age += 1;
                        thread::sleep(Duration::from_millis(1));
                        person_catalog.commit(&locked_person, writable_person);

                        let dog_catalog = library_copy.checkout::<Dog>();
                        let locked_dog = dog_catalog.lock(dog_id);
                        let mut writable_dog = locked_dog.value.clone();
                        writable_dog.dog_years += 7;
                        thread::sleep(Duration::from_millis(1));
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
        let proto_id = catalog.create(Person {
            age: 20,
            name: String::from("Atom"),
            fav_food: String::from("Apples"),
        });
        let instance_id = catalog.create_from_prototype(proto_id);

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
        let library = Library::default();
        library.register::<Person>();
        let catalog = library.checkout::<Person>();
        let grandmother_id = catalog.create(Person::default());
        let mother_id = catalog.create_from_prototype(grandmother_id);
        let daughter_id = catalog.create_from_prototype(mother_id);

        {
            let grandmother = catalog.lock(grandmother_id);
            let mut write = grandmother.value.clone();
            write.name = String::from("Grandma");
            write.fav_food = String::from("Old Timey Pasta");
            catalog.commit(&grandmother, write);

            let mother = catalog.lock(mother_id);
            let mut write = grandmother.value.clone();
            write.fav_food = String::from("Pasta");
            catalog.commit(&mother, write);
        }

        // One thread that will constantly update the grandmother's age and check that
        // the mother and daughter's age is updated.
        let thread1 = thread::spawn({
            let library_copy = library.clone();
            move || {
                let catalog = library_copy.checkout::<Person>();
                let mut rng = rand::thread_rng();
                let start = Instant::now();
                while start.elapsed() < Duration::from_millis(50) {
                    let rand_age = rng.gen::<i32>();
                    let grandmother = catalog.lock(grandmother_id);
                    let mut write = grandmother.value.clone();
                    write.age = rand_age;
                    catalog.commit(&grandmother, write);
                    thread::sleep(Duration::from_millis(1));
                    assert_eq!(rand_age, catalog.get(grandmother_id).age);
                    assert_eq!(rand_age, catalog.get(mother_id).age);
                    assert_eq!(rand_age, catalog.get(daughter_id).age);
                }
            }
        });

        // A second thread that will constantly update the mother's name and check
        // that the mother and daughter's name is updated while the grandmother's
        // name remains the same.
        let thread2 = thread::spawn({
            let library_copy = library.clone();
            move || {
                let catalog = library_copy.checkout::<Person>();
                let start = Instant::now();
                while start.elapsed() < Duration::from_millis(50) {
                    let rand_name: String = rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(10)
                        .map(char::from)
                        .collect();
                    let mother = catalog.lock(mother_id);
                    let mut write = mother.value.clone();
                    write.name = rand_name.clone();
                    catalog.commit(&mother, write);
                    thread::sleep(Duration::from_millis(1));
                    assert_eq!(String::from("Grandma"), catalog.get(grandmother_id).name);
                    assert_eq!(rand_name, catalog.get(mother_id).name);
                    assert_eq!(rand_name, catalog.get(daughter_id).name);
                }
            }
        });

        // A third thread that will constantly update the daughter's favorite foood
        // and check that the daughter's favorite food is updated while the mother's
        // and grandmother's favorite food remains the same.
        let thread3 = thread::spawn({
            move || {
                let catalog = library.checkout::<Person>();
                let start = Instant::now();
                while start.elapsed() < Duration::from_millis(50) {
                    let rand_food: String = rand::thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(10)
                        .map(char::from)
                        .collect();
                    let daughter = catalog.lock(daughter_id);
                    let mut write = daughter.value.clone();
                    write.fav_food = rand_food.clone();
                    catalog.commit(&daughter, write);
                    thread::sleep(Duration::from_millis(1));
                    assert_eq!(
                        String::from("Old Timey Pasta"),
                        catalog.get(grandmother_id).fav_food
                    );
                    assert_eq!(String::from("Pasta"), catalog.get(mother_id).fav_food);
                    assert_eq!(rand_food, catalog.get(daughter_id).fav_food);
                }
            }
        });

        thread1.join().unwrap();
        thread2.join().unwrap();
        thread3.join().unwrap();

        assert_eq!(String::from("Grandma"), catalog.get(grandmother_id).name);
        assert_eq!(
            String::from("Old Timey Pasta"),
            catalog.get(grandmother_id).fav_food
        );
        assert_eq!(String::from("Pasta"), catalog.get(mother_id).fav_food);
    }

    #[test]
    fn test_unique_lsn() {
        let library = Library::default();
        library.register::<Person>();

        let person_catalog = library.checkout::<Person>();
        let person_id = person_catalog.create(Person::default());

        library.register::<Dog>();
        let dog_catalog = library.checkout::<Dog>();
        let dog_id = dog_catalog.create(Dog::default());

        let thread_one = thread::spawn({
            let library_copy = library.clone();
            move || {
                let start = Instant::now();
                while start.elapsed() < Duration::from_millis(50) {
                    let person_catalog = library_copy.checkout::<Person>();
                    let locked_person = person_catalog.lock(person_id);
                    let mut writable_person = locked_person.value.clone();
                    writable_person.age += 1;
                    person_catalog.commit(&locked_person, writable_person);
                }
            }
        });

        let thread_two = thread::spawn({
            let library_copy = library.clone();
            move || {
                let start = Instant::now();
                while start.elapsed() < Duration::from_millis(50) {
                    let dog_catalog = library_copy.checkout::<Dog>();
                    let locked_dog = dog_catalog.lock(dog_id);
                    let mut writable_dog = locked_dog.value.clone();
                    writable_dog.dog_years += 7;
                    dog_catalog.commit(&locked_dog, writable_dog);
                }
            }
        });

        thread_one.join().unwrap();
        thread_two.join().unwrap();

        let mut lsn_hash_set: HashSet<u64> = Default::default();
        let state_inner = person_catalog.state.inner.lock().unwrap();
        for change_record in &state_inner.change_log {
            assert!(!lsn_hash_set.contains(&change_record.lsn));
            lsn_hash_set.insert(change_record.lsn);
        }

        let state_inner = dog_catalog.state.inner.lock().unwrap();
        for change_record in &state_inner.change_log {
            assert!(!lsn_hash_set.contains(&change_record.lsn));
            lsn_hash_set.insert(change_record.lsn);
        }
    }

    #[derive(Clone, Debug, Default)]
    pub(crate) struct Dog {
        pub(crate) dog_years: i32,
    }
    impl Record for Dog {
        fn type_name() -> &'static str {
            "Dog"
        }

        fn proto_update(&self, old: &Dog, new: &Dog) -> Dog {
            return Dog {
                dog_years: *proto_update_field(&self.dog_years, &old.dog_years, &new.dog_years),
            };
        }
    }

    #[derive(Clone, Debug, Default)]
    pub(crate) struct Person {
        pub(crate) age: i32,
        pub(crate) name: String,
        pub(crate) fav_food: String,
    }
    impl Record for Person {
        fn type_name() -> &'static str {
            "Person"
        }

        fn proto_update(&self, old: &Person, new: &Person) -> Person {
            return Person {
                age: *proto_update_field(&self.age, &old.age, &new.age),
                name: proto_update_field(&self.name, &old.name, &new.name).clone(),
                fav_food: proto_update_field(&self.fav_food, &old.fav_food, &new.fav_food).clone(),
            };
        }
    }
}
