use rusqlite::*;

pub use rusqlite::types::Value;

pub trait Params {
    fn bind(self, stmt: &mut Statement<'_>) -> rusqlite::Result<()>;
}

impl Params for () {
    fn bind(self, _stmt: &mut Statement<'_>) -> rusqlite::Result<()> {
        Ok(())
    }
}

fn convert(v: libsql::Value) -> types::Value {
    match v {
        libsql::Value::Null => types::Value::Null,
        libsql::Value::Integer(i) => types::Value::Integer(i),
        libsql::Value::Real(i) => types::Value::Real(i),
        libsql::Value::Text(i) => types::Value::Text(i),
        libsql::Value::Blob(i) => types::Value::Blob(i),
    }
}

impl Params for libsql::params::Params {
    fn bind(self, stmt: &mut Statement<'_>) -> rusqlite::Result<()> {
        match self {
            libsql::params::Params::None => {}
            libsql::params::Params::Positional(params) => {
                for (idx, p) in params.into_iter().enumerate() {
                    stmt.raw_bind_parameter(idx + 1, convert(p))?;
                }
            }
            libsql::params::Params::Named(params) => {
                for (name, v) in params.into_iter() {
                    let Some(idx) = stmt.parameter_index(&name)? else {
                        continue;
                    };
                    stmt.raw_bind_parameter(idx, convert(v))?;
                }
            }
        };
        return Ok(());
    }
}

impl<const N: usize> Params for [std::result::Result<libsql::Value, libsql::Error>; N] {
    fn bind(self, stmt: &mut Statement<'_>) -> rusqlite::Result<()> {
        for (idx, res) in self.into_iter().enumerate() {
            let v = res.map_err(|err| rusqlite::Error::ToSqlConversionFailure(err.into()))?;
            stmt.raw_bind_parameter(idx + 1, convert(v))?;
        }
        return Ok(());
    }
}

impl<const N: usize> Params for [(&str, std::result::Result<libsql::Value, libsql::Error>); N] {
    fn bind(self, stmt: &mut Statement<'_>) -> rusqlite::Result<()> {
        for (name, res) in self {
            let v = res.map_err(|err| rusqlite::Error::ToSqlConversionFailure(err.into()))?;
            let Some(idx) = stmt.parameter_index(name)? else {
                continue;
            };
            stmt.raw_bind_parameter(idx, convert(v))?;
        }
        return Ok(());
    }
}

impl Params for Vec<(String, types::Value)> {
    fn bind(self, stmt: &mut Statement<'_>) -> rusqlite::Result<()> {
        for (name, v) in self {
            let Some(idx) = stmt.parameter_index(&name)? else {
                continue;
            };
            stmt.raw_bind_parameter(idx, v)?;
        }
        return Ok(());
    }
}

impl Params for Vec<(&str, types::Value)> {
    fn bind(self, stmt: &mut Statement<'_>) -> rusqlite::Result<()> {
        for (name, v) in self {
            let Some(idx) = stmt.parameter_index(&name)? else {
                continue;
            };
            stmt.raw_bind_parameter(idx, v)?;
        }
        return Ok(());
    }
}

impl Params for &[(&str, types::Value)] {
    fn bind(self, stmt: &mut Statement<'_>) -> rusqlite::Result<()> {
        for (name, v) in self {
            let Some(idx) = stmt.parameter_index(&name)? else {
                continue;
            };
            stmt.raw_bind_parameter(idx, v)?;
        }
        return Ok(());
    }
}

impl Params for &[(&str, &(dyn rusqlite::ToSql + Send + Sync))] {
    fn bind(self, stmt: &mut Statement<'_>) -> rusqlite::Result<()> {
        for (name, v) in self {
            let Some(idx) = stmt.parameter_index(&name)? else {
                continue;
            };
            stmt.raw_bind_parameter(idx, v)?;
        }
        return Ok(());
    }
}

impl<const N: usize> Params for [(&str, types::Value); N] {
    fn bind(self, stmt: &mut Statement<'_>) -> rusqlite::Result<()> {
        for (name, v) in self {
            let Some(idx) = stmt.parameter_index(&name)? else {
                continue;
            };
            stmt.raw_bind_parameter(idx, v)?;
        }
        return Ok(());
    }
}

impl Params for Vec<types::Value> {
    fn bind(self, stmt: &mut Statement<'_>) -> rusqlite::Result<()> {
        for (idx, p) in self.into_iter().enumerate() {
            stmt.raw_bind_parameter(idx + 1, p)?;
        }
        return Ok(());
    }
}

impl Params for &[types::Value] {
    fn bind(self, stmt: &mut Statement<'_>) -> rusqlite::Result<()> {
        for (idx, p) in self.into_iter().enumerate() {
            stmt.raw_bind_parameter(idx + 1, p)?;
        }
        return Ok(());
    }
}

impl Params for &[&(dyn rusqlite::ToSql + Send + Sync)] {
    fn bind(self, stmt: &mut Statement<'_>) -> rusqlite::Result<()> {
        for (idx, p) in self.into_iter().enumerate() {
            stmt.raw_bind_parameter(idx + 1, p)?;
        }
        return Ok(());
    }
}

impl<T, const N: usize> Params for &[T; N]
where
    T: rusqlite::ToSql + Send + Sync,
{
    fn bind(self, stmt: &mut Statement<'_>) -> rusqlite::Result<()> {
        for (idx, p) in self.into_iter().enumerate() {
            stmt.raw_bind_parameter(idx + 1, p)?;
        }
        return Ok(());
    }
}

impl<T> Params for (T,)
where
    T: rusqlite::ToSql + Send + Sync,
{
    fn bind(self, stmt: &mut Statement<'_>) -> rusqlite::Result<()> {
        return stmt.raw_bind_parameter(1, self.0);
    }
}

impl<const N: usize> Params for [types::Value; N] {
    fn bind(self, stmt: &mut Statement<'_>) -> rusqlite::Result<()> {
        for (idx, p) in self.into_iter().enumerate() {
            stmt.raw_bind_parameter(idx + 1, p)?;
        }
        return Ok(());
    }
}

// impl<const N: usize> Params for [&(dyn rusqlite::ToSql + Send + Sync); N] {
//     fn bind(self, stmt: &mut Statement<'_>) -> rusqlite::Result<()> {
//         for (idx, p) in self.into_iter().enumerate() {
//             stmt.raw_bind_parameter(idx + 1, p)?;
//         }
//         return Ok(());
//     }
// }
