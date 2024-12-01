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

impl<const N: usize> Params for [types::Value; N] {
    fn bind(self, stmt: &mut Statement<'_>) -> rusqlite::Result<()> {
        for (idx, p) in self.into_iter().enumerate() {
            stmt.raw_bind_parameter(idx + 1, p)?;
        }
        return Ok(());
    }
}
