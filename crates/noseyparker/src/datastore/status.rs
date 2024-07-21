use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::fmt;
use std::str::FromStr;

// -------------------------------------------------------------------------------------------------
// Status
// -------------------------------------------------------------------------------------------------

/// A status assigned to a match group
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
// FIXME(overhaul): use an integer representation for serialization and db
pub enum Status {
    Accept,
    Reject,
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Status::Accept => write!(f, "accept"),
            Status::Reject => write!(f, "reject"),
        }
    }
}

impl FromStr for Status {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "accept" => Ok(Status::Accept),
            "reject" => Ok(Status::Reject),
            _ => Err(()),
        }
    }
}

// -------------------------------------------------------------------------------------------------
// Statuses
// -------------------------------------------------------------------------------------------------
/// A collection of statuses
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
// FIXME(overhaul): use a bitflag representation here?
pub struct Statuses(pub SmallVec<[Status; 16]>);

// -------------------------------------------------------------------------------------------------
// bson
// -------------------------------------------------------------------------------------------------
mod bson {
    use super::*;
    use polodb_core::bson::{Bson, Document};

    impl From<Status> for Bson {
        fn from(status: Status) -> Self {
            match status {
                Status::Accept => Bson::String("accept".to_string()),
                Status::Reject => Bson::String("reject".to_string()),
            }
        }
    }

    impl From<Bson> for Status {
        fn from(bson: Bson) -> Self {
            match bson.as_str().unwrap_or_default() {
                "accept" => Status::Accept,
                "reject" => Status::Reject,
                _ => panic!("Invalid status"),
            }
        }
    }

    impl From<Statuses> for Bson {
        fn from(statuses: Statuses) -> Self {
            let statuses_vec: Vec<Bson> = statuses.0.iter().cloned().map(Bson::from).collect();
            Bson::Array(statuses_vec)
        }
    }

    impl From<Bson> for Statuses {
        fn from(bson: Bson) -> Self {
            let array = bson.as_array().expect("Expected array");
            let statuses: SmallVec<[Status; 16]> = array.iter().cloned().map(Status::from).collect();
            Statuses(statuses)
        }
    }

    impl From<Statuses> for Document {
        fn from(statuses: Statuses) -> Self {
            let mut doc = Document::new();
            doc.insert("statuses", Bson::from(statuses));
            doc
        }
    }

    impl From<Document> for Statuses {
        fn from(doc: Document) -> Self {
            let bson = doc.get("statuses").expect("Expected statuses field");
            Statuses::from(bson.clone())
        }
    }

    impl From<Vec<Bson>> for Statuses {
        fn from(bson: Vec<Bson>) -> Self {
            let statuses = bson.into_iter()
                .map(|b| Status::from_str(b.as_str().expect("Expected string")).expect("Invalid status"))
                .collect();
            Statuses(statuses)
        }
    }
}
