
use std::path::PathBuf;
use thiserror::Error;

pub type ConfigResult<T> = Result<T, ConfigError>;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("No config.yaml found at this path: {0}")]
    ConfigMissing(PathBuf),

    #[error("Config deserialization error: {0}")]
    SerializationError(#[from] serde_yaml::Error),

    #[error("Error while performing IO for the Node: {0}")]
    IoError(#[from] std::io::Error),
}


pub type NResult<T> = Result<T, NError>;

#[derive(Error, Debug)]
pub enum NError {}