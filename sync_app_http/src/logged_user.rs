use anyhow::Error;
pub use rust_auth_server::logged_user::{LoggedUser, AUTHORIZED_USERS};

use sync_app_lib::models::AuthorizedUsers;
use sync_app_lib::pgpool::PgPool;

pub fn fill_from_db(pool: &PgPool) -> Result<(), Error> {
    let users: Vec<_> = AuthorizedUsers::get_authorized_users(&pool)?
        .into_iter()
        .map(|user| LoggedUser { email: user.email })
        .collect();

    AUTHORIZED_USERS.merge_users(&users)
}
