use std::result::Result as StdResult;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Msg(String),
}

pub type Result<T> = StdResult<T, Error>;
