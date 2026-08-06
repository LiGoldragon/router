use std::{fs, path::PathBuf};

use triad_runtime::{ComponentArgument, ComponentCommand};

use crate::{Error, RouterResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DotosCommandText {
    text: String,
}

impl DotosCommandText {
    pub fn from_command(command: ComponentCommand) -> RouterResult<Self> {
        match command.nota_argument()? {
            ComponentArgument::InlineNota(argument) => Ok(Self::new(argument.into_string())),
            ComponentArgument::NotaFile(argument) => Self::from_path(argument.into_path()),
            ComponentArgument::SignalFile(argument) => Self::from_path(argument.into_path()),
        }
    }

    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }

    pub fn from_path(path: PathBuf) -> RouterResult<Self> {
        let text = fs::read_to_string(&path).map_err(|source| Error::ReadNotaFile {
            path: path.clone(),
            source,
        })?;
        Ok(Self::new(text))
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }
}
