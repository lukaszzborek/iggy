use crate::binary::{command::ServerCommand, sender::SenderKind};
use bytes::{BufMut, BytesMut};
use iggy::error::IggyError;

pub async fn receive_and_validate(
    sender: &mut SenderKind,
    code: u32,
    length: u32,
) -> Result<ServerCommand, IggyError> {
    let mut buffer = BytesMut::with_capacity(length as usize);

    unsafe {
        buffer.set_len(length as usize);
    }
    tracing::error!("length: {length}");
    // buffer.put_bytes(0, length as usize);
    tracing::error!("after put");
    if length > 0 {
        tracing::error!("reading");
        sender.read(&mut buffer).await?;
        tracing::error!("after read");
    }

    let command = ServerCommand::from_code_and_payload(code, buffer.freeze())?;
    command.validate()?;
    Ok(command)
}
