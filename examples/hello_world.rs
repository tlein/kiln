use kiln::prelude::*;

fn main() {
    let library = Library::default();
    library.register::<Place>();
    let place_catalog = library.checkout::<Place>();
    let world_place_id = place_catalog.create(Place::default());

    {
        let locked_world_place = place_catalog.lock(world_place_id);
        let mut writable_world_place = locked_world_place.value.clone();
        writable_world_place.name = String::from("World");
        place_catalog.commit(&locked_world_place, writable_world_place);
    }

    let readonly_world_place = place_catalog.get(world_place_id);

    println!("Hello, {}!", readonly_world_place.name);
}

#[derive(Clone, Debug, Default)]
struct Place {
    name: String,
}
impl Record for Place {
    fn type_name() -> &'static str {
        "Place"
    }

    fn merge(&self, old: &Self, new: &Self) -> Self {
        return Place {
            name: merge_field(&self.name, &old.name, &new.name).clone(),
        };
    }
}
