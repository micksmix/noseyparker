use lazy_static::lazy_static;
use std::sync::Mutex;

lazy_static! {
    pub static ref DATASTORE_PATH: Mutex<String> = Mutex::new(String::from(":memory:"));
}
