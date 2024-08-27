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

use arc_swap::ArcSwapOption;
use nblock::task::IntoTask;
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
        let pending_context = Arc::new(ArcSwapOption::<RuntimeContext>::new(None));
        let tokio_thread_start_func = {
            let pending_context = Arc::clone(&pending_context);
            move || loop {
                if let Some(pending_context) = pending_context.load_full() {
                    THREADLOCAL_CONTEXT.replace(Some(pending_context));
                    return;
                }
                thread::yield_now();
            }
        };
        let nblock_thread_start_func = {
            let pending_context = Arc::clone(&pending_context);
            move |_| loop {
                if let Some(pending_context) = pending_context.load_full() {
                    THREADLOCAL_CONTEXT.replace(Some(pending_context));
                    return;
                }
                thread::yield_now();
            }
        };
        let nblock = self
            .nblock
            .with_thread_start_hook(nblock_thread_start_func)
            .build()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
        let tokio = Spinlock::new(Some(
            self.tokio
                .on_thread_start(tokio_thread_start_func)
                .build()?,
        ));
        let runflag = Arc::clone(nblock.runflag());
        let spawn_count = Arc::new(AtomicUsize::new(0));
        let context = Arc::new(RuntimeContext {
            nblock,
            tokio,
            tokio_running: Arc::new(AtomicBool::new(true)),
            runflag,
            spawn_count,
        });
        pending_context.store(Some(Arc::clone(&context)));
        Ok(Runtime { context })
    }
}

pub struct Runtime {
    context: Arc<RuntimeContext>,
}
impl Runtime {
    /// start creating a new runtime instance
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// get the current thread's thread-local runtime, and *panic* if it is not set
    pub fn get() -> Arc<Runtime> {
        match Runtime::get_threadlocal() {
            None => panic!("threadlocal runtime not found"),
            Some(x) => x,
        }
    }

    /// get the current thread's thread-local runtime, which will be automatically set for all threads managed by this runtime
    pub fn get_threadlocal() -> Option<Arc<Runtime>> {
        THREADLOCAL_CONTEXT
            .with_borrow(|x| x.as_ref().map(|x| Arc::clone(&x)))
            .map(|context| Arc::new(Self { context }))
    }

    /// set this instance of the runtime to the thread-local runtime
    pub fn set_threadlocal(&self) {
        THREADLOCAL_CONTEXT.replace(Some(Arc::clone(&self.context)));
    }

    /// helper function to synchronously block on the given future
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        futures::executor::block_on(future)
    }

    /// get the underlying runflag used to determine if the runtime should keep running
    pub fn runflag<'a>(&'a self) -> &'a Arc<AtomicBool> {
        &self.context.runflag
    }

    /// spawn an async task on the [`tokio`] thread pool
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

    /// spawn a blocking task on the [`tokio`] thread pool
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

    /// spawn a nonblocking [`nblock`] task
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
        Ok(self.context.nblock.spawn(
            task_name,
            ContextSettingTask {
                task,
                context: Arc::clone(&self.context),
            },
        ))
    }

    /// spawn a traditional [`std::thread`]
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
            THREADLOCAL_CONTEXT.replace(Some(Arc::clone(&context)));
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

    /// set the runflag to false, waiting for all threads to gracefully exit before returning
    pub fn shutdown(&self, timeout: Option<Duration>) -> Result<(), TimedOutError> {
        // set shared runflag to false
        self.context.runflag.store(false, Ordering::Relaxed);

        // stop tokio
        let tokio = self.context.tokio.lock().take();
        if let Some(tokio) = tokio {
            match timeout {
                None | Some(Duration::ZERO) => tokio.shutdown_background(),
                Some(timeout) => tokio.shutdown_timeout(timeout),
            }
            self.context.tokio_running.store(false, Ordering::Relaxed);
        }

        // wait for shutdown to complete or timeout
        self.join_timeout(
            timeout
                .map(|x| SystemTime::now() + x)
                .unwrap_or_else(|| SystemTime::UNIX_EPOCH + Duration::from_millis(u64::MAX)),
        )
    }

    /// join this instance of runtime indefinitely, waiting for it to shutdown
    pub fn join(&self) {
        self.join_timeout(SystemTime::UNIX_EPOCH + Duration::from_millis(u64::MAX))
            .ok();
    }

    /// join this instance of runtime until it shuts down or the given timeout is elapsed
    fn join_timeout(&self, timeout_at: SystemTime) -> Result<(), TimedOutError> {
        while self.context.runflag.load(Ordering::Relaxed)
            || self.context.nblock.active_task_count() > 0
            || self.context.spawn_count.load(Ordering::Relaxed) > 0
            || self.context.tokio_running.load(Ordering::Relaxed)
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

struct ContextSettingTask<T: IntoTask> {
    task: T,
    context: Arc<RuntimeContext>,
}
impl<T: IntoTask> IntoTask for ContextSettingTask<T> {
    type Task = T::Task;
    fn into_task(self) -> Self::Task {
        THREADLOCAL_CONTEXT.replace(Some(self.context));
        self.task.into_task()
    }
}

struct RuntimeContext {
    nblock: nblock::Runtime,
    tokio: Spinlock<Option<tokio::runtime::Runtime>>,
    tokio_running: Arc<AtomicBool>,
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

#[cfg(test)]
mod test {
    use nblock::{idle::Backoff, selector::RoundRobinSelector};

    use super::*;
    #[test]
    fn shutdown_no_timeout() {
        let runtime = Runtime::builder()
            .with_nblock(|builder| {
                builder.set_thread_selector(
                    RoundRobinSelector::builder()
                        .with_thread_ids(vec![0, 1])
                        .with_idle(Backoff::default())
                        .build()
                        .unwrap(),
                );
            })
            .with_tokio(|builder| {
                builder.enable_all().worker_threads(4);
            })
            .build()
            .unwrap();

        runtime.shutdown(None).unwrap();
    }
}
