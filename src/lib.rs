//! `xrm` (cross runtime manager) is a managed runtime that encapsulates:
//! - a set of managed standard threads,
//! - an async [tokio](https://crates.io/crates/tokio) runtime,
//! - a non-blocking [nblock](https://crates.io/crates/nblock) runtime.
use std::{
    cell::RefCell,
    error::Error,
    fmt::Display,
    future::Future,
    io,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::{Duration, SystemTime},
};

use spinning_top::Spinlock;

pub extern crate cron;
pub extern crate nblock;
pub extern crate tokio;
// pub type CronSchedule = crate::cron::Schedule;

pub struct Builder {
    nblock: nblock::RuntimeBuilder,
    tokio: tokio::runtime::Builder,
}
impl Builder {
    fn new() -> Self {
        Self {
            nblock: nblock::Runtime::builder(),
            tokio: tokio::runtime::Builder::new_multi_thread(),
        }
    }
    pub fn nblock<'a>(&'a mut self) -> &mut nblock::RuntimeBuilder {
        &mut self.nblock
    }
    pub fn with_nblock<F: FnOnce(&mut nblock::RuntimeBuilder)>(mut self, func: F) -> Self {
        func(&mut self.nblock);
        self
    }
    pub fn tokio<'a>(&'a mut self) -> &mut tokio::runtime::Builder {
        &mut self.tokio
    }
    pub fn with_tokio<F: FnOnce(&mut tokio::runtime::Builder)>(mut self, func: F) -> Self {
        func(&mut self.tokio);
        self
    }
    pub fn build(mut self) -> Result<Runtime, io::Error> {
        let nblock = self
            .nblock
            .build()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
        let tokio = Spinlock::new(Some(self.tokio.build()?));
        let runflag = Arc::clone(nblock.runflag());
        let spawn_count = Arc::new(AtomicUsize::new(0));
        Ok(Runtime {
            context: Arc::new(RuntimeContext {
                nblock,
                tokio,
                runflag,
                spawn_count,
            }),
        })
    }
}

pub struct Runtime {
    context: Arc<RuntimeContext>,
}
impl Runtime {
    pub fn builder() -> Builder {
        Builder::new()
    }

    pub fn get() -> Arc<Runtime> {
        match Runtime::get_threadlocal() {
            None => panic!("threadlocal runtime not found"),
            Some(x) => x,
        }
    }

    pub fn get_threadlocal() -> Option<Arc<Runtime>> {
        THREADLOCAL_CONTEXT
            .with_borrow(|x| x.as_ref().map(|x| Arc::clone(&x)))
            .map(|context| Arc::new(Self { context }))
    }

    pub fn set_threadlocal(&self) {
        THREADLOCAL_CONTEXT.replace(Some(Arc::clone(&self.context)));
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        futures::executor::block_on(future)
    }

    pub fn runflag<'a>(&'a self) -> &'a Arc<AtomicBool> {
        &self.context.runflag
    }

    pub fn spawn_async<F>(&self, future: F) -> io::Result<tokio::task::JoinHandle<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let rt = self.context.tokio.lock();
        let rt = rt
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "runtime shutting down"))?;
        Ok(rt.spawn(future))
    }

    pub fn spawn_blocking<T, F>(&self, func: F) -> io::Result<tokio::task::JoinHandle<F::Output>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let rt = self.context.tokio.lock();
        let rt = rt
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "runtime shutting down"))?;
        Ok(rt.spawn_blocking(func))
    }

    pub fn spawn_nonblocking<T>(
        &self,
        task_name: &str,
        task: T,
    ) -> io::Result<nblock::JoinHandle<<T::Task as nblock::task::Task>::Output>>
    where
        T: nblock::task::IntoTask + Send + 'static,
    {
        if !self.context.runflag.load(Ordering::Relaxed) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "runtime shutting down",
            ));
        }
        Ok(self.context.nblock.spawn(task_name, task))
    }

    pub fn spawn_thread<T, F>(&self, name: String, func: F) -> io::Result<thread::JoinHandle<T>>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        if !self.context.runflag.load(Ordering::Relaxed) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "runtime shutting down",
            ));
        }
        let context = Arc::clone(&self.context);
        thread::Builder::new().name(name).spawn(move || {
            context.spawn_count.fetch_add(1, Ordering::Relaxed);
            let x = func();
            context.spawn_count.fetch_sub(1, Ordering::Relaxed);
            x
        })
    }

    // pub fn schedule<F: Future, T: Fn() -> F>(
    //     &self,
    //     task: T,
    //     schedule: Schedule,
    // ) -> io::Result<ScheduleHandle> {
    //     todo!()
    // }

    pub fn shutdown(&self, timeout: Option<Duration>) -> Result<(), TimedOutError> {
        // set shared runflag to false
        self.context.runflag.store(false, Ordering::Relaxed);

        // stop tokio
        let mut tokio = self.context.tokio.lock();
        if let Some(tokio) = tokio.take() {
            tokio.shutdown_timeout(timeout.unwrap_or(Duration::from_secs(u64::MAX)));
        }

        // wait for shutdown to complete or timeout
        self.join_timeout(
            timeout
                .map(|x| SystemTime::now() + x)
                .unwrap_or_else(|| SystemTime::UNIX_EPOCH + Duration::from_millis(u64::MAX)),
        )
    }

    pub fn join(&self) {
        self.join_timeout(SystemTime::UNIX_EPOCH + Duration::from_millis(u64::MAX))
            .ok();
    }

    fn join_timeout(&self, timeout_at: SystemTime) -> Result<(), TimedOutError> {
        while self.context.runflag.load(Ordering::Relaxed)
            || self.context.nblock.active_task_count() > 0
            || self.context.spawn_count.load(Ordering::Relaxed) > 0
            || self.context.tokio.lock().is_some()
        {
            if SystemTime::now() > timeout_at {
                return Err(TimedOutError);
            }
            thread::park_timeout(Duration::from_secs(1))
        }
        Ok(())
    }
}

struct RuntimeContext {
    nblock: nblock::Runtime,
    tokio: Spinlock<Option<tokio::runtime::Runtime>>,
    runflag: Arc<AtomicBool>,
    spawn_count: Arc<AtomicUsize>,
}

#[derive(Debug, Clone)]
pub struct TimedOutError;
impl Error for TimedOutError {}
impl Display for TimedOutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TimedOutError")
    }
}
impl From<TimedOutError> for io::Error {
    fn from(_: TimedOutError) -> Self {
        io::Error::new(io::ErrorKind::TimedOut, "TimedOutError")
    }
}

// pub enum Schedule {
//     Once(SystemTime),
//     Cron(CronSchedule),
//     Interval(Duration),
// }

pub struct ScheduleHandle {
    cancel_func: Box<dyn Fn()>,
}
impl ScheduleHandle {
    pub fn new(cancel_func: Box<dyn Fn()>) -> Self {
        Self { cancel_func }
    }
    pub fn cancel(&self) {
        (self.cancel_func)()
    }
}

thread_local! {
    static THREADLOCAL_CONTEXT: RefCell<Option<Arc<RuntimeContext>>> = RefCell::new(None);
}
