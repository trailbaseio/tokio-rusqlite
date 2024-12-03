use serde::Deserialize;
use std::fmt::Display;

use crate::*;

#[test]
fn call_success_test() -> Result<()> {
    let conn = Connection::open_in_memory();

    let result = conn.call(|conn| {
        conn.execute(
            "CREATE TABLE person(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);",
            [],
        )
        .map_err(|e| e.into())
    });

    assert_eq!(0, result.unwrap());

    Ok(())
}

#[test]
fn call_failure_test() -> Result<()> {
    let conn = Connection::open_in_memory();

    let result = conn.call(|conn| conn.execute("Invalid sql", []).map_err(|e| e.into()));

    assert!(match result.unwrap_err() {
        crate::Error::Rusqlite(e) => {
            e == rusqlite::Error::SqlInputError {
                error: ffi::Error {
                    code: ErrorCode::Unknown,
                    extended_code: 1,
                },
                msg: "near \"Invalid\": syntax error".to_string(),
                sql: "Invalid sql".to_string(),
                offset: 0,
            }
        }
        _ => false,
    });

    Ok(())
}

#[test]
fn debug_format_test() -> Result<()> {
    let conn = Connection::open_in_memory();

    assert_eq!("Connection".to_string(), format!("{conn:?}"));

    Ok(())
}

#[test]
fn test_error_display() -> Result<()> {
    let conn = Connection::open_in_memory();

    let error = crate::Error::Close((conn, rusqlite::Error::InvalidQuery));
    assert_eq!(
        "Close((Connection, \"Query is not read-only\"))",
        format!("{error}")
    );

    let error = crate::Error::ConnectionClosed;
    assert_eq!("ConnectionClosed", format!("{error}"));

    let error = crate::Error::Rusqlite(rusqlite::Error::InvalidQuery);
    assert_eq!("Rusqlite(\"Query is not read-only\")", format!("{error}"));

    Ok(())
}

#[test]
fn test_error_source() -> Result<()> {
    let conn = Connection::open_in_memory();

    let error = crate::Error::Close((conn, rusqlite::Error::InvalidQuery));
    assert_eq!(
        std::error::Error::source(&error)
            .and_then(|e| e.downcast_ref::<rusqlite::Error>())
            .unwrap(),
        &rusqlite::Error::InvalidQuery,
    );

    let error = crate::Error::ConnectionClosed;
    assert_eq!(
        std::error::Error::source(&error).and_then(|e| e.downcast_ref::<rusqlite::Error>()),
        None,
    );

    let error = crate::Error::Rusqlite(rusqlite::Error::InvalidQuery);
    assert_eq!(
        std::error::Error::source(&error)
            .and_then(|e| e.downcast_ref::<rusqlite::Error>())
            .unwrap(),
        &rusqlite::Error::InvalidQuery,
    );

    Ok(())
}

fn failable_func(_: &rusqlite::Connection) -> std::result::Result<(), MyError> {
    Err(MyError::MySpecificError)
}

#[test]
fn test_ergonomic_errors() -> Result<()> {
    let conn = Connection::open_in_memory();

    let res = conn
        .call(|conn| failable_func(conn).map_err(|e| Error::Other(Box::new(e))))
        .unwrap_err();

    let err = std::error::Error::source(&res)
        .and_then(|e| e.downcast_ref::<MyError>())
        .unwrap();

    assert!(matches!(err, MyError::MySpecificError));

    Ok(())
}

#[test]
fn test_construction() {
    let _conn = Connection::from_conn(|| rusqlite::Connection::open_in_memory().unwrap());
}

#[test]
fn test_query() {
    let conn = Connection::open_in_memory();

    let result = conn.call(|conn| {
        conn.execute(
            "CREATE TABLE person(id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
            [],
        )
        .map_err(|e| e.into())
    });

    assert_eq!(0, result.unwrap());

    conn.query(
        "INSERT INTO person (id, name) VALUES ($1, $2)",
        params!(0, "foo"),
    )
    .unwrap();
    conn.query(
        "INSERT INTO person (id, name) VALUES (:id, :name)",
        named_params! {":id": 1, ":name": "bar"},
    )
    .unwrap();

    let rows = conn.query("SELECT * FROM person", ()).unwrap();
    assert_eq!(2, rows.len());
    assert!(matches!(rows.column_type(0).unwrap(), ValueType::Integer));
    assert_eq!(rows.column_name(0).unwrap(), "id");

    assert!(matches!(rows.column_type(1).unwrap(), ValueType::Text));
    assert_eq!(rows.column_name(1).unwrap(), "name");

    conn.execute("UPDATE person SET name = 'baz' WHERE id = $1", (1,))
        .unwrap();

    let row = conn
        .query_row("SELECT name FROM person WHERE id = $1", &[1])
        .unwrap()
        .unwrap();

    assert_eq!(row.get::<String>(0).unwrap(), "baz");

    #[derive(Deserialize)]
    struct Person {
        id: i64,
        name: String,
    }

    let person = conn
        .query_value::<Person>("SELECT * FROM person WHERE id = $1", &[1])
        .unwrap()
        .unwrap();
    assert_eq!(person.id, 1);
    assert_eq!(person.name, "baz");

    let rows = conn
        .execute_batch(
            r#"
            CREATE TABLE foo (id INTEGER) STRICT;
            INSERT INTO foo (id) VALUES (17);
            SELECT * FROM foo;
        "#,
        )
        .unwrap()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows.0.get(0).unwrap().get::<i64>(0), Ok(17));
}

#[test]
fn test_params() {
    let _ = named_params! {
        ":null": None::<String>,
        ":text": Some("test".to_string()),
    };

    let conn = Connection::open_in_memory();

    conn.call(|conn| {
        conn.execute(
            "CREATE TABLE person(id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
            [],
        )
        .map_err(|e| e.into())
    })
    .unwrap();

    conn.query(
        "INSERT INTO person (id, name) VALUES (:id, :name)",
        [
            (":id", Value::Integer(1)),
            (":name", Value::Text("Alice".to_string())),
        ],
    )
    .unwrap();

    let id = 3;
    conn.query(
        "INSERT INTO person (id, name) VALUES (:id, :name)",
        named_params! {
            ":id": id,
            ":name": Value::Text("Eve".to_string()),
        },
    )
    .unwrap();

    conn.query(
        "INSERT INTO person (id, name) VALUES ($1, $2)",
        [Value::Integer(2), Value::Text("Bob".to_string())],
    )
    .unwrap();

    conn.query(
        "INSERT INTO person (id, name) VALUES ($1, $2)",
        params!(4, "Jay"),
    )
    .unwrap();

    let rows = conn.query("SELECT COUNT(*) FROM person", ()).unwrap();

    assert_eq!(rows.0.get(0).unwrap().get::<i64>(0), Ok(4));
}

// The rest is boilerplate, not really that important

#[derive(Debug)]
enum MyError {
    MySpecificError,
}

impl Display for MyError {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl std::error::Error for MyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}
