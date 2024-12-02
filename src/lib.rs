//! Asynchronous handle for rusqlite library.
//!
//! # Guide
//!
//! This library provides [`Connection`] struct. [`Connection`] struct is a handle
//! to call functions in background thread and can be cloned cheaply.
//! [`Connection::call`] method calls provided function in the background thread
//! and returns its result asynchronously.
//!
//! # Design
//!
//! A thread is spawned for each opened connection handle. When `call` method
//! is called: provided function is boxed, sent to the thread through mpsc
//! channel and executed. Return value is then sent by oneshot channel from
//! the thread and then returned from function.
//!
//! # Example
//!
//! ```rust,no_run
//! use tokio_rusqlite::{params, Connection, Result};
//!
//! #[derive(Debug)]
//! struct Person {
//!     id: i32,
//!     name: String,
//!     data: Option<Vec<u8>>,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let conn = Connection::open_in_memory().await?;
//!
//!     let people = conn
//!         .call(|conn| {
//!             conn.execute(
//!                 "CREATE TABLE person (
//!                     id    INTEGER PRIMARY KEY,
//!                     name  TEXT NOT NULL,
//!                     data  BLOB
//!                 )",
//!                 [],
//!             )?;
//!
//!             let steven = Person {
//!                 id: 1,
//!                 name: "Steven".to_string(),
//!                 data: None,
//!             };
//!
//!             conn.execute(
//!                 "INSERT INTO person (name, data) VALUES (?1, ?2)",
//!                 rusqlite::params![steven.name, steven.data],
//!             )?;
//!
//!             let mut stmt = conn.prepare("SELECT id, name, data FROM person")?;
//!             let people = stmt
//!                 .query_map([], |row| {
//!                     Ok(Person {
//!                         id: row.get(0)?,
//!                         name: row.get(1)?,
//!                         data: row.get(2)?,
//!                     })
//!                 })?
//!                 .collect::<std::result::Result<Vec<Person>, rusqlite::Error>>()?;
//!
//!             Ok(people)
//!         })
//!         .await?;
//!
//!     for person in people {
//!         println!("Found person {:?}", person);
//!     }
//!
//!     Ok(())
//! }
//! ```

#![allow(clippy::needless_return)]
#![forbid(unsafe_code)]
#![warn(
    clippy::await_holding_lock,
    clippy::cargo_common_metadata,
    clippy::dbg_macro,
    clippy::empty_enum,
    clippy::enum_glob_use,
    clippy::inefficient_to_string,
    clippy::mem_forget,
    clippy::mutex_integer,
    clippy::needless_continue,
    clippy::todo,
    clippy::unimplemented,
    clippy::wildcard_imports,
    future_incompatible,
    missing_debug_implementations,
    unreachable_pub
)]

pub mod params;
#[cfg(test)]
mod tests;

use crossbeam_channel::{Receiver, Sender};
pub use rusqlite::types::{ToSqlOutput, Value};
pub use rusqlite::*;
use std::{
    fmt::{self, Debug, Display},
    path::Path,
    str::FromStr,
    sync::Arc,
    thread,
};
use tokio::sync::oneshot::{self};

pub use crate::params::Params;

#[macro_export]
macro_rules! params {
    () => {
        [] as [$crate::params::ToSqlType]
    };
    ($($param:expr),+ $(,)?) => {
        [$(Into::<$crate::params::ToSqlType>::into($param)),+]
    };
}

#[macro_export]
macro_rules! named_params {
    () => {
        [] as [(&str, $crate::params::ToSqlType)]
    };
    ($($param_name:literal: $param_val:expr),+ $(,)?) => {
        [$(($param_name as &str, Into::<$crate::params::ToSqlType>::into($param_val))),+]
    };
}

// #[macro_export]
// macro_rules! named_params {
//     () => {
//         [] as [(&str, &(dyn $crate::ToSql + Send))]
//     };
//     // Note: It's a lot more work to support this as part of the same macro as
//     // `params!`, unfortunately.
//     ($($param_name:literal: $param_val:expr),+ $(,)?) => {
//         [$(($param_name as &str, &$param_val as &(dyn $crate::ToSql + Send))),+]
//     };
// }

#[derive(Debug)]
/// Represents the errors specific for this library.
#[non_exhaustive]
pub enum Error {
    /// The connection to the SQLite has been closed and cannot be queried any more.
    ConnectionClosed,

    /// An error occured while closing the SQLite connection.
    /// This `Error` variant contains the [`Connection`], which can be used to retry the close operation
    /// and the underlying [`rusqlite::Error`] that made it impossile to close the database.
    Close((Connection, rusqlite::Error)),

    /// A `Rusqlite` error occured.
    Rusqlite(rusqlite::Error),

    /// An application-specific error occured.
    Other(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ConnectionClosed => write!(f, "ConnectionClosed"),
            Error::Close((_, e)) => write!(f, "Close((Connection, \"{e}\"))"),
            Error::Rusqlite(e) => write!(f, "Rusqlite(\"{e}\")"),
            Error::Other(ref e) => write!(f, "Other(\"{e}\")"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::ConnectionClosed => None,
            Error::Close((_, e)) => Some(e),
            Error::Rusqlite(e) => Some(e),
            Error::Other(ref e) => Some(&**e),
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(value: rusqlite::Error) -> Self {
        Error::Rusqlite(value)
    }
}

/// The result returned on method calls in this crate.
pub type Result<T> = std::result::Result<T, Error>;

type CallFn = Box<dyn FnOnce(&mut rusqlite::Connection) + Send + 'static>;

enum Message {
    Execute(CallFn),
    Close(oneshot::Sender<std::result::Result<(), rusqlite::Error>>),
}

/// A handle to call functions in background thread.
#[derive(Clone)]
pub struct Connection {
    sender: Sender<Message>,
}

#[derive(Debug, Copy, Clone)]
pub enum ValueType {
    Integer = 1,
    Real,
    Text,
    Blob,
    Null,
}

impl FromStr for ValueType {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<ValueType, Self::Err> {
        match s {
            "TEXT" => Ok(ValueType::Text),
            "INTEGER" => Ok(ValueType::Integer),
            "BLOB" => Ok(ValueType::Blob),
            "NULL" => Ok(ValueType::Null),
            "REAL" => Ok(ValueType::Real),
            _ => Err(()),
        }
    }
}

#[allow(unused)]
#[derive(Debug)]
pub struct Column {
    name: String,
    decl_type: Option<ValueType>,
}

#[derive(Debug)]
pub struct Rows(Vec<Row>, Arc<Vec<Column>>);

fn columns(stmt: &Statement<'_>) -> Vec<Column> {
    return stmt
        .columns()
        .into_iter()
        .map(|c| Column {
            name: c.name().to_string(),
            decl_type: c.decl_type().and_then(|s| ValueType::from_str(s).ok()),
        })
        .collect();
}

impl Rows {
    pub fn from_rows(mut rows: rusqlite::Rows) -> rusqlite::Result<Self> {
        let columns: Arc<Vec<Column>> = Arc::new(rows.as_ref().map_or(vec![], columns));

        let mut result = vec![];
        while let Some(row) = rows.next()? {
            result.push(Row::from_row(row, Some(columns.clone()))?);
        }

        Ok(Self(result, columns))
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Row> {
        self.0.iter()
    }

    pub fn column_count(&self) -> usize {
        self.1.len()
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.1.iter().map(|s| s.name.as_str()).collect()
    }

    pub fn column_name(&self, idx: usize) -> Option<&str> {
        self.1.get(idx).map(|c| c.name.as_str())
    }

    pub fn column_type(&self, idx: usize) -> std::result::Result<ValueType, rusqlite::Error> {
        if let Some(c) = self.1.get(idx) {
            return c.decl_type.ok_or_else(|| {
                rusqlite::Error::InvalidColumnType(
                    idx,
                    self.column_name(idx).unwrap_or("?").to_string(),
                    types::Type::Null,
                )
            });
        }
        return Err(rusqlite::Error::InvalidColumnType(
            idx,
            self.column_name(idx).unwrap_or("?").to_string(),
            types::Type::Null,
        ));
    }
}

#[derive(Debug)]
pub struct Row(Vec<types::Value>, Arc<Vec<Column>>);

impl Row {
    pub fn from_row(row: &rusqlite::Row, cols: Option<Arc<Vec<Column>>>) -> rusqlite::Result<Self> {
        let columns = cols.unwrap_or_else(|| Arc::new(columns(row.as_ref())));

        let count = columns.len();
        let mut values = Vec::<types::Value>::with_capacity(count);
        for idx in 0..count {
            values.push(row.get_ref(idx)?.into());
        }

        Ok(Self(values, columns))
    }

    pub fn get<T>(&self, idx: usize) -> types::FromSqlResult<T>
    where
        T: types::FromSql,
    {
        let val = self
            .0
            .get(idx)
            .ok_or_else(|| types::FromSqlError::Other("Index out of bounds".into()))?;
        T::column_result(val.into())
    }

    pub fn get_value(&self, idx: usize) -> Result<types::Value> {
        self.0
            .get(idx)
            .ok_or_else(|| Error::Other("Index out of bounds".into()))
            .cloned()
    }

    pub fn column_count(&self) -> usize {
        self.0.len()
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.1.iter().map(|s| s.name.as_str()).collect()
    }

    pub fn column_name(&self, idx: usize) -> Option<&str> {
        self.1.get(idx).map(|c| c.name.as_str())
    }
}

impl Connection {
    /// Open a new connection to a SQLite database.
    ///
    /// `Connection::open(path)` is equivalent to
    /// `Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE |
    /// OpenFlags::SQLITE_OPEN_CREATE)`.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `path` cannot be converted to a C-compatible
    /// string or if the underlying SQLite open call fails.
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_owned();
        start(move || rusqlite::Connection::open(path))
            .await
            .map_err(Error::Rusqlite)
    }

    pub async fn from_conn(conn: rusqlite::Connection) -> Result<Self> {
        start(move || Ok(conn)).await.map_err(Error::Rusqlite)
    }

    /// Open a new connection to an in-memory SQLite database.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite open call fails.
    pub async fn open_in_memory() -> Result<Self> {
        start(rusqlite::Connection::open_in_memory)
            .await
            .map_err(Error::Rusqlite)
    }

    /// Open a new connection to a SQLite database.
    ///
    /// [Database Connection](http://www.sqlite.org/c3ref/open.html) for a
    /// description of valid flag combinations.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `path` cannot be converted to a C-compatible
    /// string or if the underlying SQLite open call fails.
    pub async fn open_with_flags<P: AsRef<Path>>(path: P, flags: OpenFlags) -> Result<Self> {
        let path = path.as_ref().to_owned();
        start(move || rusqlite::Connection::open_with_flags(path, flags))
            .await
            .map_err(Error::Rusqlite)
    }

    /// Open a new connection to a SQLite database using the specific flags
    /// and vfs name.
    ///
    /// [Database Connection](http://www.sqlite.org/c3ref/open.html) for a
    /// description of valid flag combinations.
    ///
    /// # Failure
    ///
    /// Will return `Err` if either `path` or `vfs` cannot be converted to a
    /// C-compatible string or if the underlying SQLite open call fails.
    pub async fn open_with_flags_and_vfs<P: AsRef<Path>>(
        path: P,
        flags: OpenFlags,
        vfs: &str,
    ) -> Result<Self> {
        let path = path.as_ref().to_owned();
        let vfs = vfs.to_owned();
        start(move || rusqlite::Connection::open_with_flags_and_vfs(path, flags, &vfs))
            .await
            .map_err(Error::Rusqlite)
    }

    /// Open a new connection to an in-memory SQLite database.
    ///
    /// [Database Connection](http://www.sqlite.org/c3ref/open.html) for a
    /// description of valid flag combinations.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite open call fails.
    pub async fn open_in_memory_with_flags(flags: OpenFlags) -> Result<Self> {
        start(move || rusqlite::Connection::open_in_memory_with_flags(flags))
            .await
            .map_err(Error::Rusqlite)
    }

    /// Open a new connection to an in-memory SQLite database using the
    /// specific flags and vfs name.
    ///
    /// [Database Connection](http://www.sqlite.org/c3ref/open.html) for a
    /// description of valid flag combinations.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `vfs` cannot be converted to a C-compatible
    /// string or if the underlying SQLite open call fails.
    pub async fn open_in_memory_with_flags_and_vfs(flags: OpenFlags, vfs: &str) -> Result<Self> {
        let vfs = vfs.to_owned();
        start(move || rusqlite::Connection::open_in_memory_with_flags_and_vfs(flags, &vfs))
            .await
            .map_err(Error::Rusqlite)
    }

    /// Call a function in background thread and get the result
    /// asynchronously.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the database connection has been closed.
    pub async fn call<F, R>(&self, function: F) -> Result<R>
    where
        F: FnOnce(&mut rusqlite::Connection) -> Result<R> + 'static + Send,
        R: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel::<Result<R>>();

        self.sender
            .send(Message::Execute(Box::new(move |conn| {
                let value = function(conn);
                let _ = sender.send(value);
            })))
            .map_err(|_| Error::ConnectionClosed)?;

        receiver.await.map_err(|_| Error::ConnectionClosed)?
    }

    pub fn call_and_forget<F>(&self, function: F) -> Result<()>
    where
        F: FnOnce(&mut rusqlite::Connection) + 'static + Send,
    {
        self.sender
            .send(Message::Execute(Box::new(move |conn| {
                function(conn);
            })))
            .map_err(|_| Error::ConnectionClosed)?;

        return Ok(());
    }

    /// Call a function in background thread and get the result
    /// asynchronously.
    ///
    /// This method can cause a `panic` if the underlying database connection is closed.
    /// it is a more user-friendly alternative to the [`Connection::call`] method.
    /// It should be safe if the connection is never explicitly closed (using the [`Connection::close`] call).
    ///
    /// Calling this on a closed connection will cause a `panic`.
    pub async fn call_unwrap<F, R>(&self, function: F) -> R
    where
        F: FnOnce(&mut rusqlite::Connection) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (sender, receiver) = oneshot::channel::<R>();

        self.sender
            .send(Message::Execute(Box::new(move |conn| {
                let value = function(conn);
                let _ = sender.send(value);
            })))
            .expect("database connection should be open");

        receiver.await.expect(BUG_TEXT)
    }

    /// Query SQL statement.
    pub async fn query(&self, sql: &str, params: impl Params + Send + 'static) -> Result<Rows> {
        let sql = sql.to_string();
        return self
            .call(move |conn: &mut rusqlite::Connection| {
                let mut stmt = conn.prepare(&sql)?;
                params.bind(&mut stmt)?;
                let rows = stmt.raw_query();
                Ok(Rows::from_rows(rows)?)
            })
            .await;
    }

    pub async fn query_row(
        &self,
        sql: &str,
        params: impl Params + Send + 'static,
    ) -> Result<Option<Row>> {
        let sql = sql.to_string();
        return self
            .call(move |conn: &mut rusqlite::Connection| {
                let mut stmt = conn.prepare(&sql)?;
                params.bind(&mut stmt)?;
                let mut rows = stmt.raw_query();
                if let Some(row) = rows.next()? {
                    return Ok(Some(Row::from_row(row, None)?));
                }
                Ok(None)
            })
            .await;
    }

    pub async fn query_value<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        sql: &str,
        params: impl Params + Send + 'static,
    ) -> Result<Option<T>> {
        let sql = sql.to_string();
        return self
            .call(move |conn: &mut rusqlite::Connection| {
                let mut stmt = conn.prepare(&sql)?;
                params.bind(&mut stmt)?;
                let mut rows = stmt.raw_query();
                if let Some(row) = rows.next()? {
                    return Ok(Some(
                        serde_rusqlite::from_row(row).map_err(|err| Error::Other(err.into()))?,
                    ));
                }
                Ok(None)
            })
            .await;
    }

    pub async fn query_values<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        sql: &str,
        params: impl Params + Send + 'static,
    ) -> Result<Vec<T>> {
        let sql = sql.to_string();
        return self
            .call(move |conn: &mut rusqlite::Connection| {
                let mut stmt = conn.prepare(&sql)?;
                params.bind(&mut stmt)?;
                let mut rows = stmt.raw_query();

                let mut values = vec![];
                while let Some(row) = rows.next()? {
                    values.push(
                        serde_rusqlite::from_row(row).map_err(|err| Error::Other(err.into()))?,
                    );
                }
                return Ok(values);
            })
            .await;
    }

    /// Execute SQL statement.
    pub async fn execute(&self, sql: &str, params: impl Params + Send + 'static) -> Result<usize> {
        let sql = sql.to_string();
        return self
            .call(move |conn: &mut rusqlite::Connection| {
                let mut stmt = conn.prepare(&sql)?;
                params.bind(&mut stmt)?;
                Ok(stmt.raw_execute()?)
            })
            .await;
    }

    /// Batch execute SQL statements and return rows of last statement.
    pub async fn execute_batch(&self, sql: &str) -> Result<Option<Rows>> {
        let sql = sql.to_string();
        return self
            .call(move |conn: &mut rusqlite::Connection| {
                let batch = rusqlite::Batch::new(conn, &sql);

                let mut p = batch.peekable();
                while let Some(iter) = p.next() {
                    let mut stmt = iter?;

                    let mut rows = stmt.raw_query();
                    let row = rows.next()?;
                    if p.peek().is_none() {
                        if let Some(row) = row {
                            let cols: Arc<Vec<Column>> = Arc::new(columns(row.as_ref()));

                            let mut result = vec![Row::from_row(row, Some(cols.clone()))?];
                            while let Some(row) = rows.next()? {
                                result.push(Row::from_row(row, Some(cols.clone()))?);
                            }
                            return Ok(Some(Rows(result, cols)));
                        }
                        return Ok(None);
                    }
                }
                return Ok(None);
            })
            .await;
    }

    /// Close the database connection.
    ///
    /// This is functionally equivalent to the `Drop` implementation for
    /// `Connection`. It consumes the `Connection`, but on error returns it
    /// to the caller for retry purposes.
    ///
    /// If successful, any following `close` operations performed
    /// on `Connection` copies will succeed immediately.
    ///
    /// On the other hand, any calls to [`Connection::call`] will return a [`Error::ConnectionClosed`],
    /// and any calls to [`Connection::call_unwrap`] will cause a `panic`.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite close call fails.
    pub async fn close(self) -> Result<()> {
        let (sender, receiver) = oneshot::channel::<std::result::Result<(), rusqlite::Error>>();

        if let Err(crossbeam_channel::SendError(_)) = self.sender.send(Message::Close(sender)) {
            // If the channel is closed on the other side, it means the connection closed successfully
            // This is a safeguard against calling close on a `Copy` of the connection
            return Ok(());
        }

        let result = receiver.await;

        if result.is_err() {
            // If we get a RecvError at this point, it also means the channel closed in the meantime
            // we can assume the connection is closed
            return Ok(());
        }

        result.unwrap().map_err(|e| Error::Close((self, e)))
    }
}

impl Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection").finish()
    }
}

impl From<rusqlite::Connection> for Connection {
    fn from(conn: rusqlite::Connection) -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded::<Message>();
        thread::spawn(move || event_loop(conn, receiver));

        Self { sender }
    }
}

async fn start<F>(open: F) -> rusqlite::Result<Connection>
where
    F: FnOnce() -> rusqlite::Result<rusqlite::Connection> + Send + 'static,
{
    let (sender, receiver) = crossbeam_channel::unbounded::<Message>();
    let (result_sender, result_receiver) = oneshot::channel();

    thread::spawn(move || {
        let conn = match open() {
            Ok(c) => c,
            Err(e) => {
                let _ = result_sender.send(Err(e));
                return;
            }
        };

        if let Err(_e) = result_sender.send(Ok(())) {
            return;
        }

        event_loop(conn, receiver);
    });

    result_receiver
        .await
        .expect(BUG_TEXT)
        .map(|_| Connection { sender })
}

fn event_loop(mut conn: rusqlite::Connection, receiver: Receiver<Message>) {
    while let Ok(message) = receiver.recv() {
        match message {
            Message::Execute(f) => f(&mut conn),
            Message::Close(s) => {
                let result = conn.close();

                match result {
                    Ok(v) => {
                        s.send(Ok(v)).expect(BUG_TEXT);
                        break;
                    }
                    Err((c, e)) => {
                        conn = c;
                        s.send(Err(e)).expect(BUG_TEXT);
                    }
                }
            }
        }
    }
}

const BUG_TEXT: &str = "bug in tokio-rusqlite, please report";
