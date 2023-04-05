//! A graceful shutdown handler that allows all parts of an application to trigger a shutdown.
//!
//! # Why?
//!
//! An application I was maintaining was in charge of 3 different services.
//! * A RabbitMQ processing service
//! * A gRPC Server
//! * An HTTP metrics server.
//!
//! Our RabbitMQ node was restarted, so our connections dropped and our service went into shutdown mode.
//! However, due to a bug in our application layer, we didn't acknowledge the failure immediately and
//! continued handling the gRPC and HTTP traffic. Thankfully our alerts triggered that the queue was backing up
//! and we manually restarted the application without any real impact.
//!
//! Understandably, I wanted a way to not have this happen ever again. We fixed the bug in the application, and then
//! tackled the root cause: **Other services were oblivious that a shutdown happened**.
//!
//! Using this library, we've enforced that all service libraries take in a `ShutdownHandler` instance and use it to gracefully
//! shutdown. If any of them are about to crash, they will immediately raise a shutdown signal. The other services
//! will then see that signal, finish whatever work they had started, then shutdown.
//!
//! # Example
//!
//! ```
//! use std::pin::pin;
//! use std::sync::Arc;
//! use shutdown_handler::{ShutdownHandler, SignalOrComplete};
//!
//! # #[tokio::main] async fn main() {
//! // Create the shutdown handler
//! let shutdown = Arc::new(ShutdownHandler::new());
//!
//! // Shutdown on SIGTERM
//! shutdown.spawn_sigterm_handler().unwrap();
//!
//! // Spawn a few service workers
//! let mut workers = tokio::task::JoinSet::new();
//! for port in 0..4 {
//!     workers.spawn(service(Arc::clone(&shutdown), port));
//! }
//!
//! // await all workers and collect the errors
//! let mut errors = vec![];
//! while let Some(result) = workers.join_next().await {
//!     // unwrap any JoinErrors that happen if the tokio task panicked
//!     let result = result.unwrap();
//!
//!     // did our service error?
//!     if let Err(e) = result {
//!         errors.push(e);
//!     }
//! }
//!
//! assert_eq!(errors, ["port closed"]);
//! # }
//!
//! // Define our services to loop on work and shutdown gracefully
//!
//! async fn service(shutdown: Arc<ShutdownHandler>, port: u16) -> Result<(), &'static str> {
//!     // a work loop that handles events
//!     for request in 0.. {
//!         let handle = pin!(handle_request(port, request));
//!
//!         match shutdown.wait_for_signal_or_future(handle).await {
//!             // We finished handling the request without any interuptions. Continue
//!             SignalOrComplete::Completed(Ok(_)) => {}
//!
//!             // There was an error handling the request, let's shutdown
//!             SignalOrComplete::Completed(Err(e)) => {
//!                 shutdown.shutdown();
//!                 return Err(e);
//!             }
//!
//!             // There was a shutdown signal raised while handling this request
//!             SignalOrComplete::ShutdownSignal(handle) => {
//!                 // We will finish handling the request but then exit
//!                 return handle.await;
//!             }
//!         }
//!     }
//!     Ok(())
//! }
//!
//! async fn handle_request(port: u16, request: usize) -> Result<(), &'static str> {
//!     // simulate some work being done
//!     tokio::time::sleep(std::time::Duration::from_millis(10)).await;
//!     
//!     // simulate an error
//!     if port == 3 && request > 12 {
//!         Err("port closed")
//!     } else {
//!         Ok(())
//!     }
//! }
//! ```

use futures_util::{
    future::{select, Either},
    Future,
};
use pin_project_lite::pin_project;
use std::sync::{atomic::AtomicBool, Arc};
use tokio::{
    signal::unix::{signal, SignalKind},
    sync::{futures::Notified, Notify},
};

/// A graceful shutdown handler that allows all parts of an application to trigger a shutdown.
///
/// # Example
/// ```
/// use std::pin::pin;
/// use std::sync::Arc;
/// use shutdown_handler::{ShutdownHandler, SignalOrComplete};
///
/// # #[tokio::main] async fn main() {
/// // Create the shutdown handler
/// let shutdown = Arc::new(ShutdownHandler::new());
///
/// // Shutdown on SIGTERM
/// shutdown.spawn_sigterm_handler().unwrap();
///
/// // Spawn a few service workers
/// let mut workers = tokio::task::JoinSet::new();
/// for port in 0..4 {
///     workers.spawn(service(Arc::clone(&shutdown), port));
/// }
///
/// // await all workers and collect the errors
/// let mut errors = vec![];
/// while let Some(result) = workers.join_next().await {
///     // unwrap any JoinErrors that happen if the tokio task panicked
///     let result = result.unwrap();
///
///     // did our service error?
///     if let Err(e) = result {
///         errors.push(e);
///     }
/// }
///
/// assert_eq!(errors, ["port closed"]);
/// # }
///
/// // Define our services to loop on work and shutdown gracefully
///
/// async fn service(shutdown: Arc<ShutdownHandler>, port: u16) -> Result<(), &'static str> {
///     // a work loop that handles events
///     for request in 0.. {
///         let handle = pin!(handle_request(port, request));
///
///         match shutdown.wait_for_signal_or_future(handle).await {
///             // We finished handling the request without any interuptions. Continue
///             SignalOrComplete::Completed(Ok(_)) => {}
///
///             // There was an error handling the request, let's shutdown
///             SignalOrComplete::Completed(Err(e)) => {
///                 shutdown.shutdown();
///                 return Err(e);
///             }
///
///             // There was a shutdown signal raised while handling this request
///             SignalOrComplete::ShutdownSignal(handle) => {
///                 // We will finish handling the request but then exit
///                 return handle.await;
///             }
///         }
///     }
///     Ok(())
/// }
///
/// async fn handle_request(port: u16, request: usize) -> Result<(), &'static str> {
///     // simulate some work being done
///     tokio::time::sleep(std::time::Duration::from_millis(10)).await;
///     
///     // simulate an error
///     if port == 3 && request > 12 {
///         Err("port closed")
///     } else {
///         Ok(())
///     }
/// }
/// ```
#[derive(Debug, Default)]
pub struct ShutdownHandler {
    notifier: Notify,
    shutdown: AtomicBool,
}

impl ShutdownHandler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `ShutdownHandler` and registers the sigterm handler
    pub fn sigterm() -> std::io::Result<Arc<Self>> {
        let this = Arc::new(Self::new());
        this.spawn_sigterm_handler()?;
        Ok(this)
    }

    /// Registers the signal event `SIGTERM` to trigger an application shutdown
    pub fn spawn_sigterm_handler(self: &Arc<Self>) -> std::io::Result<()> {
        self.spawn_signal_handler(SignalKind::terminate())
    }

    /// Registers a signal event to trigger an application shutdown
    pub fn spawn_signal_handler(self: &Arc<Self>, signal_kind: SignalKind) -> std::io::Result<()> {
        let mut signal = signal(signal_kind)?;

        let shutdown = self.clone();
        tokio::spawn(async move {
            signal.recv().await;
            shutdown.shutdown();
        });
        Ok(())
    }

    /// Sends the shutdown signal to all the current and future waiters
    pub fn shutdown(&self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Release);
        self.notifier.notify_waiters();
    }

    /// Returns a future that waits for the shutdown signal.
    ///
    /// You can use this like an async function.
    pub fn wait_for_signal(&self) -> ShutdownSignal<'_> {
        ShutdownSignal {
            shutdown: &self.shutdown,
            notified: self.notifier.notified(),
        }
    }

    /// This method will try to complete the given future, but will give up if the shutdown signal is raised.
    /// The unfinished future is returned in case it is not cancel safe and you need to complete it
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::pin::pin;
    /// use shutdown_handler::{ShutdownHandler, SignalOrComplete};
    ///
    /// # #[tokio::main] async fn main() {
    /// async fn important_work() -> i32 {
    ///     tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    ///     42
    /// }
    ///
    /// let shutdown = Arc::new(ShutdownHandler::new());
    ///
    /// // another part of the application signals a shutdown
    /// let shutdown2 = Arc::clone(&shutdown);
    /// let handle = tokio::spawn(async move {
    ///     tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    ///     shutdown2.shutdown();
    /// });
    ///
    /// let work = pin!(important_work());
    ///
    /// match shutdown.wait_for_signal_or_future(work).await {
    ///     SignalOrComplete::Completed(res) => println!("important work completed without interuption: {res}"),
    ///     SignalOrComplete::ShutdownSignal(work) => {
    ///         println!("shutdown signal recieved");
    ///         let res = work.await;
    ///         println!("important work completed: {res}");
    ///     },
    /// }
    /// # }
    /// ```
    pub async fn wait_for_signal_or_future<F: Future + Unpin>(&self, f: F) -> SignalOrComplete<F> {
        let handle = self.wait_for_signal();
        tokio::pin!(handle);
        match select(handle, f).await {
            Either::Left((_signal, f)) => SignalOrComplete::ShutdownSignal(f),
            Either::Right((res, _)) => SignalOrComplete::Completed(res),
        }
    }
}

#[derive(Debug)]
/// Reports whether a future managed to complete without interuption, or if there was a shutdown signal
pub enum SignalOrComplete<F: Future> {
    ShutdownSignal(F),
    Completed(F::Output),
}

pin_project!(
    /// A Future that waits for a shutdown signal. Returned by [`ShutdownHandler::shutdown`]
    pub struct ShutdownSignal<'a> {
        shutdown: &'a AtomicBool,
        #[pin]
        notified: Notified<'a>,
    }
);

impl std::future::Future for ShutdownSignal<'_> {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        if this.shutdown.load(std::sync::atomic::Ordering::Acquire) {
            std::task::Poll::Ready(())
        } else {
            this.notified.poll(cx)
        }
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};

    use nix::sys::signal::{raise, Signal};
    use tokio::{signal::unix::SignalKind, sync::oneshot, time::timeout};

    use crate::ShutdownHandler;

    #[tokio::test]
    async fn shutdown_sigterm() {
        let shutdown = Arc::new(ShutdownHandler::new());
        shutdown.spawn_sigterm_handler().unwrap();

        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            shutdown.wait_for_signal().await;
            tx.send(true).unwrap();
        });

        raise(Signal::SIGTERM).unwrap();

        assert!(
            (timeout(Duration::from_secs(1), rx).await).is_ok(),
            "Shutdown handler took longer than 1 second!"
        );
    }

    #[tokio::test]
    async fn shutdown_custom_signal() {
        let shutdown = Arc::new(ShutdownHandler::new());
        shutdown.spawn_signal_handler(SignalKind::hangup()).unwrap();

        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            shutdown.wait_for_signal().await;
            tx.send(true).unwrap();
        });

        raise(Signal::SIGHUP).unwrap();

        assert!(
            (timeout(Duration::from_secs(1), rx).await).is_ok(),
            "Shutdown handler took longer than 1 second!"
        );
    }

    #[tokio::test]
    async fn shutdown() {
        let shutdown = Arc::new(ShutdownHandler::new());

        let (tx, rx) = oneshot::channel();
        let channel_shutdown = shutdown.clone();
        tokio::spawn(async move {
            channel_shutdown.wait_for_signal().await;
            tx.send(true).unwrap();
        });

        tokio::spawn(async move {
            shutdown.shutdown();
        });

        assert!(
            (timeout(Duration::from_secs(1), rx).await).is_ok(),
            "Shutdown handler took longer than 1 second!"
        );
    }

    #[tokio::test]
    async fn no_notification() {
        let shutdown = Arc::new(ShutdownHandler::new());

        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            shutdown.wait_for_signal().await;
            tx.send(true).unwrap();
        });

        assert!(
            (timeout(Duration::from_secs(1), rx).await).is_err(),
            "Shutdown handler ran without a signal!"
        );
    }
}
