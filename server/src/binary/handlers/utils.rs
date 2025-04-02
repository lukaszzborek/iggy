use crate::binary::{command::ServerCommand, sender::SenderKind};
use bytes::BytesMut;
use iggy::error::IggyError;

pub async fn receive_and_validate(
    sender: &mut SenderKind,
    code: u32,
    length: u32,
) -> Result<ServerCommand, IggyError> {
    let mut buffer = BytesMut::with_capacity(length as usize);
    if length > 0 {
        unsafe {
            buffer.set_len(length as usize);
        }
        sender.read(&mut buffer).await?;
    }
    let command = ServerCommand::from_code_and_payload(code, buffer.freeze())?;
    command.validate()?;
    Ok(command)
}
