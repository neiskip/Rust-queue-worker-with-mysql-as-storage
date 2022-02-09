use sqlx::{mysql::MySqlPoolOptions, Pool, MySql};
use std::{time::Duration};

pub type DB = Pool<MySql>;

pub async fn connect(db_url: &str) -> Result<DB, crate::Error>{
    MySqlPoolOptions::new()
                .max_connections(100)
                .max_lifetime(Duration::from_secs(1800))
                .connect(db_url)
                .await
                .map_err(|err| crate::Error::ConnectingToDatabase(err.to_string()))
}

pub async fn migrate(db: &DB) -> Result<(), crate::Error> {
    match sqlx::migrate!().run(db).await {
        Ok(_) => Ok(()),
        Err(err) => Err(err)
    }?;

    Ok(())
}