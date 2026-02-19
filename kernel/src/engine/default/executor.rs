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
    /// The type of guard returned for `enter`
    type Guard<'a>
    where
        Self: 'a;

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

    /// Enter the runtime context of this executor.
    fn enter(&self) -> Self::Guard<'_>;
}

#[cfg(any(feature = "tokio", test))]
pub mod tokio {
    use super::TaskExecutor;
    use futures::TryFutureExt;
    use futures::{future::BoxFuture, Future};
    use std::sync::mpsc::channel;
    use tokio::runtime::{EnterGuard, Handle, RuntimeFlavor};

    use crate::{DeltaResult, Error};

    /// A [`TaskExecutor`] that uses the tokio single-threaded runtime in a
    /// background thread to service tasks.
    #[derive(Debug)]
    pub struct TokioBackgroundExecutor {
        sender: tokio::sync::mpsc::Sender<BoxFuture<'static, ()>>,
        handle: Handle,
        _thread: std::thread::JoinHandle<()>,
    }

    impl Default for TokioBackgroundExecutor {
        fn default() -> Self {
            Self::new()
        }
    }

    impl TokioBackgroundExecutor {
        pub fn new() -> Self {
            let (handle_sender, handle_receiver) = std::sync::mpsc::channel::<Handle>();
            let (sender, mut receiver) = tokio::sync::mpsc::channel::<BoxFuture<'_, ()>>(50);
            let thread = std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let handle = rt.handle().clone();
                handle_sender.send(handle).unwrap();
                rt.block_on(async move {
                    while let Some(task) = receiver.recv().await {
                        tokio::task::spawn(task);
                    }
                });
            });
            let handle = handle_receiver.recv().unwrap();
            Self {
                sender,
                handle,
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
        type Guard<'a> = EnterGuard<'a>;

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
            Box::pin(tokio::task::spawn_blocking(task).map_err(Error::join_failure))
        }

        fn enter(&self) -> EnterGuard<'_> {
            self.handle.enter()
        }
    }

    /// A [`TaskExecutor`] that uses the tokio multi-threaded runtime.
    ///
    /// You can create one based on a handle to an existing runtime (to share
    /// the runtime with other parts of your application), or create one that
    /// owns its own runtime.
    #[derive(Debug)]
    pub struct TokioMultiThreadExecutor {
        handle: tokio::runtime::Handle,
        /// If Some, this executor owns the runtime and will keep it alive.
        /// If None, the executor borrows an external runtime via `handle`.
        _runtime: Option<tokio::runtime::Runtime>,
    }

    impl TokioMultiThreadExecutor {
        /// Create a new executor that uses an existing runtime's handle.
        pub fn new(handle: tokio::runtime::Handle) -> Self {
            assert_eq!(
                handle.runtime_flavor(),
                RuntimeFlavor::MultiThread,
                "TokioExecutor must be created with a multi-threaded runtime"
            );
            Self {
                handle,
                _runtime: None,
            }
        }

        /// Create a new executor that owns its own multi-threaded Tokio runtime.
        ///
        /// # Parameters
        /// - `worker_threads`: Number of worker threads. If `None`, uses Tokio's default.
        ///   See [`tokio::runtime::Builder::worker_threads`].
        /// - `max_blocking_threads`: Maximum number of threads for blocking operations.
        ///   If `None`, uses Tokio's default. See [`tokio::runtime::Builder::max_blocking_threads`].
        ///
        /// # Errors
        /// Returns an error if the runtime cannot be created.
        pub fn new_owned_runtime(
            worker_threads: Option<usize>,
            max_blocking_threads: Option<usize>,
        ) -> DeltaResult<Self> {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            builder.enable_all();

            if let Some(threads) = worker_threads {
                builder.worker_threads(threads);
            }
            if let Some(max_blocking) = max_blocking_threads {
                builder.max_blocking_threads(max_blocking);
            }

            let runtime = builder
                .build()
                .map_err(|e| Error::generic(format!("Failed to create Tokio runtime: {e}")))?;

            let handle = runtime.handle().clone();
            Ok(Self {
                handle,
                _runtime: Some(runtime),
            })
        }
    }

    impl TaskExecutor for TokioMultiThreadExecutor {
        type Guard<'a> = EnterGuard<'a>;

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

            let recv = || {
                receiver
                    .recv()
                    .expect("TokioMultiThreadExecutor has crashed")
            };

            if tokio::runtime::Handle::try_current().is_ok() {
                // Use block_in_place to tell Tokio we're about to block - this allows
                // the runtime to move tasks off this worker's local queue so they can
                // be stolen by other workers. Only use block_in_place if we're inside
                // a Tokio runtime.
                tokio::task::block_in_place(recv)
            } else {
                // If we're not inside a Tokio runtime, we can't use block_in_place,
                // so we just block on the receiver.
                recv()
            }
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
            Box::pin(tokio::task::spawn_blocking(task).map_err(Error::join_failure))
        }

        fn enter(&self) -> EnterGuard<'_> {
            self.handle.enter()
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

        #[test]
        fn test_tokio_multi_thread_executor_owned_runtime() {
            let executor = TokioMultiThreadExecutor::new_owned_runtime(None, None)
                .expect("Failed to create executor");

            // Test block_on works
            let result = executor.block_on(async {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                2 + 2
            });
            assert_eq!(result, 4, "block_on should return the correct result");

            // Test spawn works
            let (sender, receiver) = channel::<i32>();
            executor.spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                sender.send(2 + 2).unwrap();
            });
            let result = receiver.recv().expect("spawn task should send result");
            assert_eq!(result, 4, "spawned task should compute correct result");
        }

        #[test]
        fn test_owned_runtime_small_pool_nested_block_on_deadlocks() {
            use std::sync::Arc;
            use std::time::Duration;

            // Create a small pool
            let executor = Arc::new(
                TokioMultiThreadExecutor::new_owned_runtime(Some(1), Some(1))
                    .expect("Failed to create executor"),
            );
            let e1 = executor.clone();
            let e2 = executor.clone();
            let e3 = executor.clone();

            let (tx, rx) = channel::<i32>();

            // Spawn a thread to do deeply nested block_on calls
            std::thread::spawn(move || {
                let result = executor.block_on(async move {
                    e1.block_on(async move {
                        e2.block_on(async move {
                            e3.block_on(async {
                                tokio::time::sleep(Duration::from_millis(1)).await;
                                42
                            })
                        })
                    })
                });
                tx.send(result).ok();
            });

            // With 1 worker thread, 1 blocking thread and 4 nested block_on calls, this should deadlock
            let timeout = Duration::from_millis(500);
            let result = rx.recv_timeout(timeout);

            // Test passes if we got a timeout (deadlock occurred as expected)
            // Test fails if we got a result (no deadlock - unexpected)
            assert!(
                result.is_err(),
                "Expected deadlock with 1 worker thread, 1 blocking thread and 4 nested block_on calls",
            );
        }

        #[test]
        fn test_block_on_works_outside_tokio_runtime() {
            let executor = TokioMultiThreadExecutor::new_owned_runtime(None, None)
                .expect("Failed to create executor");

            // Verify we're not inside a Tokio runtime
            assert!(
                tokio::runtime::Handle::try_current().is_err(),
                "Test must run outside of a Tokio runtime"
            );

            // block_on should work even though we're not inside a Tokio runtime
            let result = executor.block_on(async {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                42
            });
            assert_eq!(result, 42);
        }

        #[rstest::rstest]
        #[case::multithreaded(
            TokioMultiThreadExecutor::new_owned_runtime(None, None).expect("Couldn't create multithreaded executor")
        )]
        #[case::background(TokioBackgroundExecutor::new())]
        fn can_enter_a_runtime<T: TaskExecutor>(#[case] executor: T) {
            // Verify we're not inside a Tokio runtime
            assert!(
                tokio::runtime::Handle::try_current().is_err(),
                "Test must run outside of a Tokio runtime"
            );

            let guard = executor.enter();

            assert!(
                tokio::runtime::Handle::try_current().is_ok(),
                "Should have entered runtime"
            );

            drop(guard);

            assert!(
                tokio::runtime::Handle::try_current().is_err(),
                "Should have exited runtime"
            );
        }
    }
}
