use serde::Deserialize;
use std::fmt::Display;

use crate::*;

#[tokio::test]
async fn open_in_memory_test() -> Result<()> {
    let conn = Connection::open_in_memory().await;
    assert!(conn.is_ok());
    Ok(())
}

#[tokio::test]
async fn call_success_test() -> Result<()> {
    let conn = Connection::open_in_memory().await?;

    let result = conn
        .call(|conn| {
            conn.execute(
                "CREATE TABLE person(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);",
                [],
            )
            .map_err(|e| e.into())
        })
        .await;

    assert_eq!(0, result.unwrap());

    Ok(())
}

#[tokio::test]
async fn call_unwrap_success_test() -> Result<()> {
    let conn = Connection::open_in_memory().await?;

    let result = conn
        .call_unwrap(|conn| {
            conn.execute(
                "CREATE TABLE person(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);",
                [],
            )
            .unwrap()
        })
        .await;

    assert_eq!(0, result);

    Ok(())
}

#[tokio::test]
async fn call_failure_test() -> Result<()> {
    let conn = Connection::open_in_memory().await?;

    let result = conn
        .call(|conn| conn.execute("Invalid sql", []).map_err(|e| e.into()))
        .await;

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

#[tokio::test]
async fn close_success_test() -> Result<()> {
    let conn = Connection::open_in_memory().await?;

    assert!(conn.close().await.is_ok());

    Ok(())
}

#[tokio::test]
async fn double_close_test() -> Result<()> {
    let conn = Connection::open_in_memory().await?;

    let conn2 = conn.clone();

    assert!(conn.close().await.is_ok());
    assert!(conn2.close().await.is_ok());

    Ok(())
}

#[tokio::test]
async fn close_call_test() -> Result<()> {
    let conn = Connection::open_in_memory().await?;

    let conn2 = conn.clone();

    assert!(conn.close().await.is_ok());

    let result = conn2
        .call(|conn| conn.execute("SELECT 1;", []).map_err(|e| e.into()))
        .await;

    assert!(matches!(
        result.unwrap_err(),
        crate::Error::ConnectionClosed
    ));

    Ok(())
}

#[tokio::test]
#[should_panic]
async fn close_call_unwrap_test() {
    let conn = Connection::open_in_memory().await.unwrap();

    let conn2 = conn.clone();

    assert!(conn.close().await.is_ok());

    conn2
        .call_unwrap(|conn| conn.execute("SELECT 1;", []))
        .await
        .unwrap();
}

#[tokio::test]
async fn close_failure_test() -> Result<()> {
    let conn = Connection::open_in_memory().await?;

    conn.call(|conn| {
        conn.execute(
            "CREATE TABLE person(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);",
            [],
        )
        .map_err(|e| e.into())
    })
    .await?;

    conn.call(|conn| {
        // Leak a prepared statement to make the database uncloseable
        // See https://www.sqlite.org/c3ref/close.html for details regarding this behaviour
        let stmt = Box::new(conn.prepare("INSERT INTO person VALUES (1, ?1);").unwrap());
        Box::leak(stmt);
        Ok(())
    })
    .await?;

    assert!(match conn.close().await.unwrap_err() {
        crate::Error::Close((_, e)) => {
            e == rusqlite::Error::SqliteFailure(
                ffi::Error {
                    code: ErrorCode::DatabaseBusy,
                    extended_code: 5,
                },
                Some(
                    "unable to close due to unfinalized statements or unfinished backups"
                        .to_string(),
                ),
            )
        }
        _ => false,
    });

    Ok(())
}

#[tokio::test]
async fn debug_format_test() -> Result<()> {
    let conn = Connection::open_in_memory().await?;

    assert_eq!("Connection".to_string(), format!("{conn:?}"));

    Ok(())
}

#[tokio::test]
async fn test_error_display() -> Result<()> {
    let conn = Connection::open_in_memory().await?;

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

#[tokio::test]
async fn test_error_source() -> Result<()> {
    let conn = Connection::open_in_memory().await?;

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

#[tokio::test]
async fn test_ergonomic_errors() -> Result<()> {
    let conn = Connection::open_in_memory().await?;

    let res = conn
        .call(|conn| failable_func(conn).map_err(|e| Error::Other(Box::new(e))))
        .await
        .unwrap_err();

    let err = std::error::Error::source(&res)
        .and_then(|e| e.downcast_ref::<MyError>())
        .unwrap();

    assert!(matches!(err, MyError::MySpecificError));

    Ok(())
}

#[tokio::test]
async fn call_libsql_query() {
    let conn = Connection::open_in_memory().await.unwrap();

    let result = conn
        .call(|conn| {
            conn.execute(
                "CREATE TABLE person(id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
                [],
            )
            .map_err(|e| e.into())
        })
        .await;

    assert_eq!(0, result.unwrap());

    conn.query(
        "INSERT INTO person (id, name) VALUES ($1, $2)",
        libsql::params!(0, "foo"),
    )
    .await
    .unwrap();
    conn.query(
        "INSERT INTO person (id, name) VALUES (:id, :name)",
        libsql::named_params! {":id": 1, ":name": "bar"},
    )
    .await
    .unwrap();

    let rows = conn.query("SELECT * FROM person", ()).await.unwrap();
    assert_eq!(2, rows.len());
    assert!(matches!(
        rows.column_type(0).unwrap(),
        libsql::ValueType::Integer
    ));
    assert_eq!(rows.column_name(0).unwrap(), "id");

    assert!(matches!(
        rows.column_type(1).unwrap(),
        libsql::ValueType::Text
    ));
    assert_eq!(rows.column_name(1).unwrap(), "name");

    conn.execute("UPDATE person SET name = 'baz' WHERE id = $1", [1])
        .await
        .unwrap();

    let row = conn
        .query_row("SELECT name FROM person WHERE id = $1", [1])
        .await
        .unwrap()
        .unwrap();

    assert_eq!(row.get::<String>(0).unwrap(), "baz");

    #[derive(Deserialize)]
    struct Person {
        id: i64,
        name: String,
    }

    let person = conn
        .query_value::<Person>("SELECT * FROM person WHERE id = $1", [1])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(person.id, 1);
    assert_eq!(person.name, "baz");

    let rows = conn
        .execute_batch(r#"
            CREATE TABLE foo (id INTEGER) STRICT;
            INSERT INTO foo (id) VALUES (17);
            SELECT * FROM foo;
        "#)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows.0.get(0).unwrap().get::<i64>(0), Ok(17));
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
