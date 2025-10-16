pub struct ShutdownTx(pub tokio::sync::oneshot::Sender<()>);
pub struct ShutdownRx(pub tokio::sync::oneshot::Receiver<()>);

pub fn shutdown_pair() -> (ShutdownTx, ShutdownRx) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    (ShutdownTx(tx), ShutdownRx(rx))
}

impl ShutdownRx {
    pub async fn wait(&mut self) {
        let _ = (&mut self.0).await;
    }
    pub fn try_now(&mut self) -> bool {
        self.0.try_recv().is_ok()
    }
    pub fn is_closed(&self) -> bool {
        self.0.is_terminated()
    }
}

impl From<tokio::sync::oneshot::Receiver<()>> for ShutdownRx {
    fn from(rx: tokio::sync::oneshot::Receiver<()>) -> Self {
        ShutdownRx(rx)
    }
}
