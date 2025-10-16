use crossbeam_channel as cb;

pub struct ShutdownTx(pub cb::Sender<()>);
pub struct ShutdownRx(pub cb::Receiver<()>);

pub fn shutdown_pair() -> (ShutdownTx, ShutdownRx) {
    let (tx, rx) = cb::bounded(1);
    (ShutdownTx(tx), ShutdownRx(rx))
}

impl ShutdownRx {
    pub fn wait(&mut self) {
        let _ = self.0.recv();
    }
    pub fn try_now(&mut self) -> bool {
        self.0.try_recv().is_ok()
    }
    pub fn is_closed(&self) -> bool {
        self.0.is_empty()
    }
}

impl ShutdownTx {
    pub fn send(self) {
        let _ = self.0.send(());
    }
}

impl From<crossbeam_channel::Receiver<()>> for ShutdownRx {
    fn from(rx: crossbeam_channel::Receiver<()>) -> Self {
        ShutdownRx(rx)
    }
}
