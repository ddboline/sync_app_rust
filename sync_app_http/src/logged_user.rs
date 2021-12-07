pub use authorized_users::{
    get_random_key, get_secrets, token::Token, AuthorizedUser, AUTHORIZED_USERS, JWT_SECRET,
    KEY_LENGTH, SECRET_KEY, TRIGGER_DB_UPDATE,
};
use chrono::{DateTime, Duration, Utc};
use futures::{
    executor::block_on,
    future::{ready, Ready},
};
use log::debug;
use reqwest::{header::HeaderValue, Client};
use rweb::{filters::cookie::cookie, Filter, Rejection, Schema};
use serde::{Deserialize, Serialize};
use stack_string::StackString;
use std::{
    convert::{TryFrom, TryInto},
    env,
    env::var,
    str::FromStr,
};
use tokio::task::{spawn, JoinHandle};
use uuid::Uuid;

use sync_app_lib::{config::Config, models::AuthorizedUsers, pgpool::PgPool};

use crate::{app::AppState, errors::ServiceError as Error};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Schema)]
pub struct LoggedUser {
    #[schema(description = "Email Address")]
    pub email: StackString,
    #[schema(description = "Session UUID")]
    pub session: Uuid,
    #[schema(description = "Secret Key")]
    pub secret_key: StackString,
}

impl LoggedUser {
    pub fn verify_session_id(&self, session_id: Uuid) -> Result<(), Error> {
        if self.session == session_id {
            Ok(())
        } else {
            Err(Error::Unauthorized)
        }
    }

    pub fn filter() -> impl Filter<Extract = (Self,), Error = Rejection> + Copy {
        cookie("session-id")
            .and(cookie("jwt"))
            .and_then(|id: Uuid, user: Self| async move {
                user.verify_session_id(id)
                    .map(|_| user)
                    .map_err(rweb::reject::custom)
            })
    }

    pub async fn get_session(
        &self,
        client: &Client,
        config: &Config,
        session_key: &str,
    ) -> Result<Option<SyncSession>, anyhow::Error> {
        let url = format!("https://{}/api/session/{}", config.domain, session_key);
        let value = HeaderValue::from_str(&self.session.to_string())?;
        let key = HeaderValue::from_str(&self.secret_key)?;
        let session: Option<SyncSession> = client
            .get(url)
            .header("session", value)
            .header("secret-key", key)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        debug!("Got session {:?}", session);
        if let Some(session) = session {
            if session.created_at > (Utc::now() - Duration::minutes(10)) {
                return Ok(Some(session));
            }
        }
        Ok(None)
    }

    pub async fn set_session(
        &self,
        client: &Client,
        config: &Config,
        session_key: &str,
        session_value: SyncSession,
    ) -> Result<(), anyhow::Error> {
        let url = format!("https://{}/api/session/{}", config.domain, session_key);
        let value = HeaderValue::from_str(&self.session.to_string())?;
        let key = HeaderValue::from_str(&self.secret_key)?;
        client
            .post(url)
            .header("session", value)
            .header("secret-key", key)
            .json(&session_value)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn rm_session(
        &self,
        client: &Client,
        config: &Config,
        session_key: &str,
    ) -> Result<(), anyhow::Error> {
        let url = format!("https://{}/api/session/{}", config.domain, session_key);
        let value = HeaderValue::from_str(&self.session.to_string())?;
        let key = HeaderValue::from_str(&self.secret_key)?;
        client
            .delete(url)
            .header("session", value)
            .header("secret-key", key)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn push_session(
        self,
        key: SyncKey,
        data: AppState,
    ) -> Result<Option<Vec<StackString>>, Error> {
        if let Some(session) = self
            .get_session(&data.client, &data.config, key.to_str())
            .await
            .map_err(Into::<Error>::into)?
        {
            if let Some(result) = session.result {
                self.rm_session(&data.client, &data.config, key.to_str())
                    .await?;
                return Ok(Some(result));
            }
            debug!("session exists and is presumably running {:?}", session);
        } else {
            debug!("push job to queue {}", key.to_str());
            self.set_session(
                &data.client,
                &data.config,
                key.to_str(),
                SyncSession::default(),
            )
            .await?;
            let mesg = SyncMesg::new(self, key);
            data.queue.push((
                mesg.clone(),
                spawn({
                    let data = data.clone();
                    async move { mesg.process_mesg(&data).await.map_err(Into::into) }
                }),
            ));
        }
        Ok(None)
    }

    pub async fn clear_sessions(&self, data: AppState) -> Result<(), Error> {
        for key in SyncKey::all_keys() {
            self.set_session(
                &data.client,
                &data.config,
                key.to_str(),
                SyncSession::default(),
            )
            .await?;
        }
        Ok(())
    }
}

impl From<AuthorizedUser> for LoggedUser {
    fn from(user: AuthorizedUser) -> Self {
        Self {
            email: user.email,
            session: user.session,
            secret_key: user.secret_key,
        }
    }
}

impl TryFrom<Token> for LoggedUser {
    type Error = Error;
    fn try_from(token: Token) -> Result<Self, Self::Error> {
        let user = token.try_into()?;
        if AUTHORIZED_USERS.is_authorized(&user) {
            Ok(user.into())
        } else {
            debug!("NOT AUTHORIZED {:?}", user);
            Err(Error::Unauthorized)
        }
    }
}

impl FromStr for LoggedUser {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut buf = StackString::new();
        buf.push_str(s);
        let token: Token = buf.into();
        token.try_into()
    }
}

pub async fn fill_from_db(pool: &PgPool) -> Result<(), Error> {
    debug!("{:?}", *TRIGGER_DB_UPDATE);
    let users: Vec<_> = if TRIGGER_DB_UPDATE.check() {
        AuthorizedUsers::get_authorized_users(pool)
            .await?
            .into_iter()
            .map(|user| user.email)
            .collect()
    } else {
        AUTHORIZED_USERS.get_users()
    };
    if let Ok("true") = var("TESTENV").as_ref().map(String::as_str) {
        AUTHORIZED_USERS.merge_users(["user@test"])?;
    }
    AUTHORIZED_USERS.merge_users(users)?;
    debug!("{:?}", *AUTHORIZED_USERS);
    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SyncSession {
    pub created_at: DateTime<Utc>,
    pub result: Option<Vec<StackString>>,
}

impl Default for SyncSession {
    fn default() -> Self {
        Self {
            created_at: Utc::now(),
            result: None,
        }
    }
}

impl SyncSession {
    pub fn from_lines(lines: Vec<StackString>) -> Self {
        Self {
            created_at: Utc::now(),
            result: Some(lines),
        }
    }
}

#[derive(Clone, Copy)]
pub enum SyncKey {
    SyncGarmin,
    SyncMovie,
    SyncCalendar,
    SyncPodcast,
    SyncSecurity,
}

impl SyncKey {
    pub fn to_str(self) -> &'static str {
        match self {
            Self::SyncGarmin => "sync_garmin",
            Self::SyncMovie => "sync_movie",
            Self::SyncCalendar => "sync_calendar",
            Self::SyncPodcast => "sync_podcast",
            Self::SyncSecurity => "sync_security",
        }
    }

    pub fn all_keys() -> [Self; 5] {
        [
            Self::SyncGarmin,
            Self::SyncMovie,
            Self::SyncCalendar,
            Self::SyncPodcast,
            Self::SyncSecurity,
        ]
    }
}

#[derive(Clone)]
pub struct SyncMesg {
    pub user: LoggedUser,
    pub key: SyncKey,
}

use crate::requests::{
    CalendarSyncRequest, GarminSyncRequest, MovieSyncRequest, SyncPodcastsRequest,
    SyncSecurityRequest,
};
use log::error;

impl SyncMesg {
    pub fn new(user: LoggedUser, key: SyncKey) -> Self {
        Self { user, key }
    }

    async fn process_mesg(self, app: &AppState) -> Result<(), Error> {
        debug!(
            "start {} for {} {}",
            self.key.to_str(),
            self.user.email,
            self.user.session
        );
        let lines = match self.key {
            SyncKey::SyncGarmin => (GarminSyncRequest {}).handle(&app.locks).await,
            SyncKey::SyncMovie => (MovieSyncRequest {}).handle(&app.locks).await,
            SyncKey::SyncCalendar => (CalendarSyncRequest {}).handle(&app.locks).await,
            SyncKey::SyncPodcast => (SyncPodcastsRequest {}).handle(&app.locks).await,
            SyncKey::SyncSecurity => (SyncSecurityRequest {}).handle(&app.locks).await,
        }?;
        debug!(
            "finished {} for {} {}, {} lines",
            self.key.to_str(),
            self.user.email,
            self.user.session,
            lines.len()
        );
        let value = SyncSession::from_lines(lines);
        self.user
            .set_session(&app.client, &app.config, self.key.to_str(), value)
            .await
            .map_err(Into::into)
    }
}
