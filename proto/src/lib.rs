pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}

pub mod snazzy {
    pub mod items {
        include!(concat!(env!("OUT_DIR"), "/snazzy.items.rs"));
    }
}

use snazzy::items;

pub fn create_large_shirt(color: String) -> items::Shirt {
    let mut shirt = items::Shirt {
        color: Some(color),
        ..items::Shirt::default()
    };
    shirt.set_size(items::shirt::Size::Large);
    shirt
}
