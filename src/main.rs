
mod db;
mod error;
mod queue;
mod mysqljob;
use std::{sync::Arc, time::Duration};

pub use error::Error;

use futures::{stream, StreamExt};
use mysqljob::MySqlQueue;
use queue::{Job, Message, Queue, SendSignInEmail};

use rand::SeedableRng;
use rand::{thread_rng, Rng, rngs::StdRng};
use rand::distributions::{Alphanumeric, Standard};


const CONCURRENCY: usize = 50;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // let db_url = std::env::var("DATABASE_URL")
    //     .map_err(|_| Error::BadConfig("DATABASE URL is missing".to_string()))?;
    
    let db = db::connect("mysql://bobir:170a33a33@localhost:3306/rust_queue").await?;
    db::migrate(&db).await?;

    let queue = Arc::new(MySqlQueue::new(db.clone()));

    let worker_queue = queue.clone();
    
    let job = Message::SendSignInEmail(SendSignInEmail{
        email: "your@email.com".to_string(),
        name: "ABC DEF".to_string(),
        code: "000-000".to_string(),
    });
    // sqlx::query("INSERT INTO `queue`(`id`, `created_at`, `updated_at`, `scheduled_for`, `failed_attempts`, `status`, `message`)
    // VALUES (?, ?, ?, ?, ?, ?, ?)")
    //             .bind(uuid::Uuid::new_v4().to_hyphenated().to_string())
    //             .bind(chrono::Utc::now())
    //             .bind(chrono::Utc::now())
    //             .bind(chrono::Utc::now()+chrono::Duration::hours(1))
    //             .bind(100)
    //             .bind(1)
    //             .bind(sqlx::types::Json(&job))
    //             .execute(&db).await?;
    let _ = queue.push(job, None).await;
    
    let join_handler = tokio::spawn(async move {
        run_worker(worker_queue.clone()).await
    }).await;
    let sleep;
    if join_handler.is_ok(){
        sleep = Duration::from_secs(1);
    } else {
        sleep = Duration::from_secs(5);    
    }
    tokio::time::sleep(sleep).await;

    Ok(())
}

async fn run_worker(queue: Arc<dyn queue::Queue>){
    loop {
        let jobs: Vec<Job> = match queue.pull(CONCURRENCY as u32).await {
            Ok(jobs) => jobs,
            Err(err) => {
                println!("{}", format!("run_worker: pulling_jobs: {}", err));
                tokio::time::sleep(Duration::from_millis(500)).await;
                Vec::new()
            }
        };

        let num_of_jobs = jobs.len();

        match num_of_jobs {
            0 => {
                let rng = StdRng::from_entropy();
                let _insert = queue.push(Message::SendSignInEmail(SendSignInEmail{
                    email: rng.sample_iter(Alphanumeric).take(15).map(char::from).collect(),
                    name: StdRng::from_entropy().sample_iter(Alphanumeric).take(20).map(char::from).collect(),
                    code: (0..7).map(|_| {
                        let idx: u8 = thread_rng().gen_range(0..9);
                        idx as char
                    }).collect(),
                }), Some(chrono::DateTime::from(chrono::Utc::now()) + chrono::Duration::seconds(5))).await;
            },
            _ => { println!("{}", format!("Fetched {} jobs", num_of_jobs)); }
        }
        if num_of_jobs > 0 {
            println!("{}", format!("Fetched {} jobs", num_of_jobs));
        }
        stream::iter(jobs)
                    .for_each_concurrent(CONCURRENCY, |job| async {
                        let job_id = job.id;

                        let res = match handle_job(job).await{
                            Ok(_) => queue.delete(job_id).await,
                            Err(err) => {
                                println!("{}", format!("run_worker: handling job({}): {}", job_id, &err));
                                queue.fail(job_id).await
                            }
                        };

                        match res {
                            Ok(_) => {},
                            Err(err) => {
                                println!("{}", format!("run_worker: deleting / failed job: {}", &err));
                            }
                        }
                    }).await;
        tokio::time::sleep(Duration::from_millis(125)).await;
    }
}

async fn handle_job(job: Job) -> Result<(), crate::Error> {
    match job.message {
        message @ Message::SendSignInEmail{ .. } => {
            println!("{}", format!("Sending sign in email: {:?}", &message));
        },
        _ => {}
    };

    Ok(())
}