use pin_project_lite::pin_project;
use std::sync::{atomic::AtomicBool, Arc};
use tokio::{
    signal::unix::{signal, SignalKind},
    sync::{futures::Notified, Notify},
};

/// A shutdown handler that allows all parts of an application to trigger a shutdown.
///
/// # Example
/// ```ignore
/// // Create the shutdown handler
/// let shutdown = Arc::new(ShutdownHandler::new());
///
/// let actix_server = /* create an actix web server */
///
/// // if the shutdown signal fires, tell actix web to shutdown
/// let actix_handle = self.actix_server.handle();
/// let actix_shutdown = Arc::clone(&shutdown);
/// tokio::spawn(async move {
///     actix_shutdown.wait_for_signal().await;
///     actix_handle.stop(true).await
/// });
///
/// // Wait for actix to close naturally
/// let res = self.actix_server.await.context("HTTP Server Error");
///
/// // if actix web is shutdown, tell the rest of the application to shutdown
/// shutdown.shutdown();
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

    /// Returns a future that waits for the shutdown signal. You can use
    /// this like an async function
    pub fn wait_for_signal(&self) -> ShutdownSignal<'_> {
        ShutdownSignal {
            shutdown: &self.shutdown,
            notified: self.notifier.notified(),
        }
    }
}

pin_project!(
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

        unsafe { libc::raise(signal_hook::SIGTERM) };

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

        unsafe { libc::raise(signal_hook::SIGHUP) };

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
