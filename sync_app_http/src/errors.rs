use anyhow::Error as AnyhowError;
use axum::{
    extract::Json,
    http::{
        header::{InvalidHeaderName, InvalidHeaderValue},
        StatusCode,
    },
    response::{IntoResponse, Response},
};
use log::error;
use postgres_query::Error as PqError;
use reqwest::Error as ReqwestError;
use serde::Serialize;
use serde_json::Error as SerdeJsonError;
use serde_yml::Error as YamlError;
use stack_string::{format_sstr, StackString};
use std::{
    fmt::{Debug, Error as FmtError},
    io::Error as IoError,
    net::AddrParseError,
    str::Utf8Error,
    string::FromUtf8Error,
};
use stdout_channel::StdoutChannelError;
use thiserror::Error;
use tokio::task::JoinError;
use url::ParseError;
use utoipa::{
    openapi::{ContentBuilder, ResponseBuilder, ResponsesBuilder},
    IntoResponses, PartialSchema, ToSchema,
};

use crate::logged_user::LOGIN_HTML;
use authorized_users::errors::AuthUsersError;

#[derive(Error, Debug)]
pub enum ServiceError {
    #[error("JoinError {0}")]
    JoinError(#[from] JoinError),
    #[error("ReqwestError {0}")]
    ReqwestError(#[from] ReqwestError),
    #[error("AuthUsersError {0}")]
    AuthUsersError(#[from] AuthUsersError),
    #[error("AddrParseError {0}")]
    AddrParseError(#[from] AddrParseError),
    #[error("Internal Server Error")]
    InternalServerError,
    #[error("BadRequest: {0}")]
    BadRequest(StackString),
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Anyhow error {0}")]
    AnyhowError(#[from] AnyhowError),
    #[error("UrlParseError {0}")]
    UrlParseError(#[from] ParseError),
    #[error("IoError {0}")]
    IoError(#[from] IoError),
    #[error("FromUtf8Error {0}")]
    FromUtf8Error(Box<FromUtf8Error>),
    #[error("Utf8Error {0}")]
    Utf8Error(#[from] Utf8Error),
    #[error("StdoutChannelError {0}")]
    StdoutChannelError(#[from] StdoutChannelError),
    #[error("PqError {0}")]
    PqError(Box<PqError>),
    #[error("FmtError {0}")]
    FmtError(#[from] FmtError),
    #[error("InvalidHeaderName {0}")]
    InvalidHeaderName(#[from] InvalidHeaderName),
    #[error("InvalidHeaderValue {0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
    #[error("SerdeJsonError {0}")]
    SerdeJsonError(#[from] SerdeJsonError),
    #[error("YamlError {0}")]
    YamlError(#[from] YamlError),
}

impl From<PqError> for ServiceError {
    fn from(value: PqError) -> Self {
        Self::PqError(value.into())
    }
}

impl From<FromUtf8Error> for ServiceError {
    fn from(value: FromUtf8Error) -> Self {
        Self::FromUtf8Error(value.into())
    }
}

#[derive(Serialize, ToSchema)]
struct ErrorMessage {
    message: StackString,
}

impl axum::response::IntoResponse for ErrorMessage {
    fn into_response(self) -> Response {
        Json(self).into_response()
    }
}

impl IntoResponse for ServiceError {
    fn into_response(self) -> Response {
        match self {
            Self::Unauthorized
            | Self::AuthUsersError(_)
            | Self::InvalidHeaderName(_)
            | Self::InvalidHeaderValue(_) => (StatusCode::OK, LOGIN_HTML).into_response(),
            Self::BadRequest(message) => {
                (StatusCode::BAD_REQUEST, ErrorMessage { message }).into_response()
            }
            e => (
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorMessage {
                    message: format_sstr!("Internal Server Error: {e}"),
                },
            )
                .into_response(),
        }
    }
}

impl IntoResponses for ServiceError {
    fn responses() -> std::collections::BTreeMap<
        String,
        utoipa::openapi::RefOr<utoipa::openapi::response::Response>,
    > {
        let error_message_content = ContentBuilder::new()
            .schema(Some(ErrorMessage::schema()))
            .build();
        ResponsesBuilder::new()
            .response(
                StatusCode::UNAUTHORIZED.as_str(),
                ResponseBuilder::new()
                    .description("Not Authorized")
                    .content(
                        "text/html",
                        ContentBuilder::new().schema(Some(String::schema())).build(),
                    ),
            )
            .response(
                StatusCode::BAD_REQUEST.as_str(),
                ResponseBuilder::new()
                    .description("Bad Request")
                    .content("application/json", error_message_content.clone()),
            )
            .response(
                StatusCode::INTERNAL_SERVER_ERROR.as_str(),
                ResponseBuilder::new()
                    .description("Internal Server Error")
                    .content("application/json", error_message_content.clone()),
            )
            .build()
            .into()
    }
}

#[cfg(test)]
mod test {
    use axum::http::header::{InvalidHeaderName, InvalidHeaderValue};
    use postgres_query::Error as PqError;
    use reqwest::Error as ReqwestError;
    use serde_json::Error as SerdeJsonError;
    use serde_yml::Error as YamlError;
    use std::{
        fmt::Error as FmtError, io::Error as IoError, net::AddrParseError, str::Utf8Error,
        string::FromUtf8Error,
    };
    use stdout_channel::StdoutChannelError;
    use tokio::{task::JoinError, time::error::Elapsed};
    use url::ParseError as UrlParseError;

    use authorized_users::errors::AuthUsersError;

    use crate::errors::ServiceError as Error;

    #[test]
    fn test_error_size() {
        println!("JoinError {}", std::mem::size_of::<JoinError>());
        println!("UrlParseError {}", std::mem::size_of::<UrlParseError>());
        println!("SerdeJsonError {}", std::mem::size_of::<SerdeJsonError>());
        println!("Elapsed {}", std::mem::size_of::<Elapsed>());
        println!("FmtError {}", std::mem::size_of::<FmtError>());
        println!("AuthUsersError {}", std::mem::size_of::<AuthUsersError>());
        println!("IoError {}", std::mem::size_of::<IoError>());
        println!("FromUtf8Error {}", std::mem::size_of::<FromUtf8Error>());
        println!("Utf8Error {}", std::mem::size_of::<Utf8Error>());
        println!(
            "StdoutChannelError {}",
            std::mem::size_of::<StdoutChannelError>()
        );
        println!("PqError {}", std::mem::size_of::<PqError>());
        println!("AddrParseError {}", std::mem::size_of::<AddrParseError>());
        println!(
            "InvalidHeaderName {}",
            std::mem::size_of::<InvalidHeaderName>()
        );
        println!(
            "InvalidHeaderValue {}",
            std::mem::size_of::<InvalidHeaderValue>()
        );
        println!("ReqwestError {}", std::mem::size_of::<ReqwestError>());
        println!("YamlError {}", std::mem::size_of::<YamlError>());

        assert_eq!(std::mem::size_of::<Error>(), 32);
    }
}
