use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::{
    sync::Notify,
    task::{spawn, JoinHandle},
    time::{sleep, Duration},
};

#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<RateLimiterInner>,
    #[allow(dead_code)]
    rate_task: Arc<JoinHandle<()>>,
}

impl RateLimiter {
    pub fn new(max_per_unit_time: usize, unit_time_ms: usize) -> Self {
        let inner = Arc::new(RateLimiterInner::new(max_per_unit_time, unit_time_ms));
        let rate_task = Arc::new({
            let inner = inner.clone();
            spawn(async move {
                inner.check_reset().await;
            })
        });
        Self { inner, rate_task }
    }

    pub async fn acquire(&self) {
        self.inner.acquire().await;
    }
}

fn gtzero(x: usize) -> Option<usize> {
    if x > 0 {
        Some(x - 1)
    } else {
        None
    }
}

struct RateLimiterInner {
    max_per_unit_time: usize,
    unit_time_ms: usize,
    remaining: AtomicUsize,
    notify: Notify,
}

impl RateLimiterInner {
    fn new(max_per_unit_time: usize, unit_time_ms: usize) -> Self {
        Self {
            max_per_unit_time,
            unit_time_ms,
            remaining: AtomicUsize::new(max_per_unit_time),
            notify: Notify::new(),
        }
    }

    fn decrement_remaining(&self) -> Result<usize, usize> {
        self.remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, gtzero)
    }

    async fn acquire(&self) {
        loop {
            if self.decrement_remaining().is_ok() {
                return;
            }
            self.notify.notified().await;
        }
    }

    async fn check_reset(&self) {
        loop {
            self.remaining
                .fetch_max(self.max_per_unit_time, Ordering::SeqCst);
            self.notify.notify_waiters();
            sleep(Duration::from_millis(self.unit_time_ms as u64)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use chrono::Utc;
    use log::debug;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use tokio::{
        task::spawn,
        time::{sleep, Duration},
    };

    use crate::rate_limiter::RateLimiter;

    #[tokio::test]
    async fn test_rate_limiter() -> Result<(), Error> {
        env_logger::init();

        let start = Utc::now();

        let rate_limiter = RateLimiter::new(1000, 100);
        let test_count = Arc::new(AtomicUsize::new(0));

        let tasks: Vec<_> = (0..10_000)
            .map(|_| {
                let rate_limiter = rate_limiter.clone();
                let test_count = test_count.clone();
                spawn(async move {
                    rate_limiter.acquire().await;
                    test_count.fetch_add(1, Ordering::SeqCst);
                })
            })
            .collect();

        sleep(Duration::from_millis(100)).await;

        for _ in 0..5 {
            let count = test_count.load(Ordering::SeqCst);
            debug!("{}", count);
            sleep(Duration::from_millis(100)).await;
        }
        for t in tasks {
            t.await?;
        }

        let elapsed = Utc::now() - start;

        println!(
            "{} {}",
            elapsed.num_milliseconds(),
            test_count.load(Ordering::SeqCst)
        );
        assert!(elapsed.num_milliseconds() >= 950);
        assert_eq!(test_count.load(Ordering::SeqCst), 10_000);
        Ok(())
    }
}
