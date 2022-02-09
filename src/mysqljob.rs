
use std::char::MAX;

use sqlx::{self, types::Json, query};
use crate::{
    db::DB,
    queue::{Job, Message, Queue}
};
use chrono;
use ulid::Ulid;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct MySqlQueue{
    db: DB,
    max_attempts: u32
}

const MAX_FAILED_ATTEMPTS: i32 = 3;

#[derive(Debug, Clone, sqlx::FromRow)]
struct MySqlJob {
    id: uuid::Uuid,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,

    scheduled_for: chrono::DateTime<chrono::Utc>,
    failed_attempts: i32,
    status: MySqlJobStatus,
    message: Json<Message>
}

#[derive(Debug, Clone, sqlx::Type, PartialEq)]
#[repr(i32)]
enum MySqlJobStatus {
    Queued = 1,
    Running = 2,
    Failed = -1
}

impl From<MySqlJob> for Job {
    fn from(item: MySqlJob) -> Self {
        Job {
            id: item.id,
            message: item.message.0
        }
    }
}

impl MySqlQueue {
    pub fn new(db: DB) -> MySqlQueue {
        let queue = MySqlQueue {
            db,
            max_attempts: 5
        };

        queue
    }
}

#[async_trait::async_trait]
impl Queue for MySqlQueue {
    async fn push(
        &self,
        job: Message,
        date: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), crate::Error> {
        let scheduled_for = date.unwrap_or(chrono::Utc::now());
        let failed_attempts: i32 = 0;
        let message = Json(job);
        let status = MySqlJobStatus::Queued;
        let now = chrono::Utc::now();
        let job_id: Uuid = Ulid::new().into();
        println!("{}", format!("Pushing job: {}", job_id));
        let query = "INSERT INTO `queue`(`id`, `created_at`, `updated_at`, `scheduled_for`, `failed_attempts`, `status`, `message`)
                    VALUES (?, ?, ?, ?, ?, ?, ?)";
        sqlx::query(query)
                .bind(job_id.to_hyphenated().to_string())
                .bind(now)
                .bind(now)
                .bind(scheduled_for)
                .bind(failed_attempts)
                .bind(status)
                .bind(message)
                .execute(&self.db)
                .await?;
        Ok(())
    }

    async fn delete(&self, job_id: Uuid) -> Result<(), crate::Error> {
        let query = "DELETE FROM queue WHERE id = ? LIMIT 1";
        
        sqlx::query(query).bind(job_id).execute(&self.db).await?;
        println!("Deleting job: {}", job_id);

        Ok(())
    }

    async fn fail(&self, job_id: Uuid) -> Result<(), crate::Error> {
        let now = chrono::Utc::now();
        let query = "UPDATE queue SET status = ?, updated_at = ?, failed_attempts = failed_attempts + 1
                        WHERE id = ?";
        sqlx::query(query)
            .bind(MySqlJobStatus::Queued)
            .bind(now)
            .bind(job_id)
            .execute(&self.db)
            .await?;
        Ok(())
    }

    async fn pull(&self, num_of_jobs: u32) -> Result<Vec<Job>, crate::Error> {
        let num_of_jobs = if num_of_jobs > 100 {
            100
        } else {
            num_of_jobs
        };
        let now: chrono::DateTime<chrono::Local> = chrono::DateTime::from(chrono::Utc::now());
        sqlx::query("
            UPDATE queue
                    SET status = ?, updated_at = ?
                    WHERE id IN (
                            SELECT id FROM `queue` WHERE `status` = ? AND `scheduled_for`<= ? AND failed_attempts < ?
                            ORDER BY `scheduled_for`
                    );
                ")
                .bind(MySqlJobStatus::Running)
                .bind(now)
                .bind(MySqlJobStatus::Queued)
                .bind(now)
                .bind(MAX_FAILED_ATTEMPTS)
                .execute(&self.db).await?;
        let query = "SELECT * FROM `queue` WHERE `status` = ? AND `scheduled_for` <= ? AND failed_attempts < ?
                        ORDER BY `scheduled_for` LIMIT ?;";
        let jobs: Vec<MySqlJob> = sqlx::query_as::<_, MySqlJob>(query)
                // .bind(MySqlJobStatus::Running)
                // .bind(now)
                // .bind(MySqlJobStatus::Queued)
                // .bind(now)
                // .bind(MAX_FAILED_ATTEMPTS)
                // .bind(num_of_jobs)
                .bind(MySqlJobStatus::Queued)
                .bind(now)
                .bind(MAX_FAILED_ATTEMPTS)
                .bind(num_of_jobs)
                .fetch_all(&self.db)
                .await.unwrap_or(Vec::new());
        Ok(jobs.into_iter().map(Into::into).collect())
    }

    async fn flush(&self) -> Result<(), crate::Error> {
        sqlx::query("DELETE FROM queue").execute(&self.db).await?;
        Ok(())
    }
}