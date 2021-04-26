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
    use crate::{proto_update_field, Library, Record};
    use rand::{distributions::Alphanumeric, Rng};
    use std::{
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
                        writable_person.age = writable_person.age + 1;
                        thread::sleep(Duration::from_millis(1));
                        person_catalog.commit(&locked_person, writable_person);

                        let dog_catalog = library_copy.checkout::<Dog>();
                        let locked_dog = dog_catalog.lock(dog_id);
                        let mut writable_dog = locked_dog.value.clone();
                        writable_dog.dog_years = writable_dog.dog_years + 7;
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
            let library_copy = library.clone();
            move || {
                let catalog = library_copy.checkout::<Person>();
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

    #[derive(Clone, Debug, Default)]
    struct Dog {
        dog_years: i32,
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
    struct Person {
        age: i32,
        name: String,
        fav_food: String,
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
