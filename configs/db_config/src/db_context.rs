use std::{fs, path::PathBuf};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::table::TableSpec;

#[derive(Deserialize, Serialize, Debug)]
pub struct DbContext {
    table_specs: Vec<TableSpec>,
}

impl DbContext {
    pub fn from(table_specs: Vec<TableSpec>) -> Result<DbContext> {
        let db_context = DbContext { table_specs };
        Self::validate_context(&db_context)?;
        Ok(db_context)
    }

    pub fn load_from_file(context_config_path: &PathBuf) -> Result<DbContext> {
        let context_file_contents = fs::read_to_string(context_config_path)?;

        let ctx: DbContext = serde_json::from_str(&context_file_contents)?;

        Self::validate_context(&ctx)?;

        Ok(ctx)
    }

    fn validate_context(_ctx: &DbContext) -> Result<()> {
        Ok(())
    }

    pub fn get_table_specs(&self) -> &Vec<TableSpec> {
        &self.table_specs
    }
}
