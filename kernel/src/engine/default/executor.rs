// TODO: (#1112) remove panics in this file
#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]
#![allow(clippy::panic)]

//! The default engine uses Async IO to read files, but the kernel APIs are all
//! synchronous. Therefore, we need an executor to run the async IO on in the
//! background.
//!
//! A generic trait [TaskExecutor] can be implemented with your preferred async
//! runtime. Behind the `tokio` feature flag, we provide a both a single-threaded
//! and multi-threaded executor based on Tokio.
use futures::{future::BoxFuture, Future};

use crate::DeltaResult;

/// An executor that can be used to run async tasks. This is used by IO functions
/// within the `DefaultEngine`.
///
/// This must be capable of running within an async context and running futures
/// on another thread. This could be a multi-threaded runtime, like Tokio's or
/// could be a single-threaded runtime on a background thread.
pub trait TaskExecutor: Send + Sync + 'static {
    /// Block on the given future, returning its output.
    ///
    /// This should NOT panic if called within an async context. Thus it can't
    /// be implemented by `tokio::runtime::Runtime::block_on`.
    fn block_on<T>(&self, task: T) -> T::Output
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static;

    /// Run the future in the background.
    fn spawn<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static;

    fn spawn_blocking<T, R>(&self, task: T) -> BoxFuture<'_, DeltaResult<R>>
    where
        T: FnOnce() -> R + Send + 'static,
        R: Send + 'static;
}

#[cfg(any(feature = "tokio", test))]
pub mod tokio {
    use super::TaskExecutor;
    use futures::TryFutureExt;
    use futures::{future::BoxFuture, Future};
    use std::sync::mpsc::channel;
    use tokio::runtime::RuntimeFlavor;

    use crate::DeltaResult;

    /// A [`TaskExecutor`] that uses the tokio single-threaded runtime in a
    /// background thread to service tasks.
    #[derive(Debug)]
    pub struct TokioBackgroundExecutor {
        sender: tokio::sync::mpsc::Sender<BoxFuture<'static, ()>>,
        _thread: std::thread::JoinHandle<()>,
    }

    impl Default for TokioBackgroundExecutor {
        fn default() -> Self {
            Self::new()
        }
    }

    impl TokioBackgroundExecutor {
        pub fn new() -> Self {
            let (sender, mut receiver) = tokio::sync::mpsc::channel::<BoxFuture<'_, ()>>(50);
            let thread = std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    while let Some(task) = receiver.recv().await {
                        tokio::task::spawn(task);
                    }
                });
            });
            Self {
                sender,
                _thread: thread,
            }
        }
    }

    impl TokioBackgroundExecutor {
        fn send_future(&self, fut: BoxFuture<'static, ()>) {
            // We cannot call `blocking_send()` because that calls `block_on`
            // internally and panics if called within an async context. 🤦
            let mut fut = Some(fut);
            loop {
                match self.sender.try_send(fut.take().unwrap()) {
                    Ok(()) => break,
                    Err(tokio::sync::mpsc::error::TrySendError::Full(original)) => {
                        std::thread::yield_now();
                        fut.replace(original);
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        panic!("TokioBackgroundExecutor channel closed")
                    }
                };
            }
        }
    }

    impl TaskExecutor for TokioBackgroundExecutor {
        fn block_on<T>(&self, task: T) -> T::Output
        where
            T: Future + Send + 'static,
            T::Output: Send + 'static,
        {
            // We cannot call `tokio::runtime::Runtime::block_on` here because
            // it panics if called within an async context. So instead we spawn
            // the future on the runtime and send the result back using a channel.
            let (sender, receiver) = channel::<T::Output>();

            let fut = Box::pin(async move {
                let task_output = task.await;
                tokio::task::spawn_blocking(move || {
                    sender.send(task_output).ok();
                })
                .await
                .unwrap();
            });

            self.send_future(fut);

            receiver
                .recv()
                .expect("TokioBackgroundExecutor has crashed")
        }

        fn spawn<F>(&self, task: F)
        where
            F: Future<Output = ()> + Send + 'static,
        {
            self.send_future(Box::pin(task));
        }

        fn spawn_blocking<T, R>(&self, task: T) -> BoxFuture<'_, DeltaResult<R>>
        where
            T: FnOnce() -> R + Send + 'static,
            R: Send + 'static,
        {
            Box::pin(tokio::task::spawn_blocking(task).map_err(crate::Error::join_failure))
        }
    }

    /// A [`TaskExecutor`] that uses the tokio multi-threaded runtime. You can
    /// create one based on a handle to an existing runtime, so it can share
    /// the runtime with other parts of your application.
    #[derive(Debug)]
    pub struct TokioMultiThreadExecutor {
        handle: tokio::runtime::Handle,
    }

    impl TokioMultiThreadExecutor {
        pub fn new(handle: tokio::runtime::Handle) -> Self {
            assert_eq!(
                handle.runtime_flavor(),
                RuntimeFlavor::MultiThread,
                "TokioExecutor must be created with a multi-threaded runtime"
            );
            Self { handle }
        }
    }

    impl TaskExecutor for TokioMultiThreadExecutor {
        // `block_on` uses `block_in_place`; If concurrent `block_on` calls exceed Tokio's `max_blocking_threads`, this can deadlock
        // See:
        // https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.max_blocking_threads
        fn block_on<T>(&self, task: T) -> T::Output
        where
            T: Future + Send + 'static,
            T::Output: Send + 'static,
        {
            // We cannot call `tokio::runtime::Runtime::block_on` here because
            // it panics if called within an async context. So instead we spawn
            // the future on the runtime and send the result back using a channel.
            let (sender, receiver) = channel::<T::Output>();

            let fut = Box::pin(async move {
                let task_output = task.await;
                tokio::task::spawn_blocking(move || {
                    sender.send(task_output).ok();
                })
                .await
                .unwrap();
            });

            // We throw away the handle, but it should continue on.
            self.handle.spawn(fut);

            // Use block_in_place to tell Tokio we're about to block - this allows
            // the runtime to move tasks off this worker's local queue so they can
            // be stolen by other workers.
            tokio::task::block_in_place(|| {
                receiver
                    .recv()
                    .expect("TokioMultiThreadExecutor has crashed")
            })
        }

        fn spawn<F>(&self, task: F)
        where
            F: Future<Output = ()> + Send + 'static,
        {
            self.handle.spawn(task);
        }

        fn spawn_blocking<T, R>(&self, task: T) -> BoxFuture<'_, DeltaResult<R>>
        where
            T: FnOnce() -> R + Send + 'static,
            R: Send + 'static,
        {
            Box::pin(tokio::task::spawn_blocking(task).map_err(crate::Error::join_failure))
        }
    }

    #[cfg(test)]
    mod test {
        use super::*;

        async fn test_executor(executor: impl TaskExecutor) {
            // Can run a task
            let task = async {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                2 + 2
            };
            let result = executor.block_on(task);
            assert_eq!(result, 4);

            // Can spawn a task
            let (sender, receiver) = channel::<i32>();
            executor.spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                sender.send(2 + 2).unwrap();
            });
            let result = receiver.recv().unwrap();
            assert_eq!(result, 4);
        }

        #[tokio::test]
        async fn test_tokio_background_executor() {
            let executor = TokioBackgroundExecutor::new();
            test_executor(executor).await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
        async fn test_tokio_multi_thread_executor() {
            let executor = TokioMultiThreadExecutor::new(tokio::runtime::Handle::current());
            test_executor(executor).await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_nested_block_on_does_not_deadlock() {
            use std::sync::Arc;
            use std::time::Duration;

            let executor = Arc::new(TokioMultiThreadExecutor::new(
                tokio::runtime::Handle::current(),
            ));
            let executor_clone = executor.clone();

            let (tx, rx) = channel::<i32>();

            let handle = std::thread::spawn(move || {
                // Outer block_on
                let result = executor.block_on(async move {
                    // Inner block_on
                    let inner_result = executor_clone.block_on(async {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        42
                    });
                    inner_result + 1
                });
                tx.send(result).ok();
            });

            // Wait with timeout - if this times out, we have a deadlock
            let timeout = Duration::from_secs(5);
            let result = rx
                .recv_timeout(timeout)
                .expect("Timeout - likely deadlock in TokioMultiThreadExecutor::block_on");
            assert_eq!(result, 43);
            handle.join().expect("thread panicked");
        }
    }
}
