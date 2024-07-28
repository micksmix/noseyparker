use lazy_static::lazy_static;
use rusqlite::Connection;
use std::sync::Mutex;

lazy_static! {
    pub static ref CONN: Mutex<Connection> = Mutex::new(Connection::open_in_memory().unwrap());
}
