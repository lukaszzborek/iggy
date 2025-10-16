// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::error::IggyError;
use bytes::{BufMut, Bytes, BytesMut};
use std::str::from_utf8;

/// Encodes username and password into the wire format for LOGIN_USER command.
pub fn encode_login_auth(
    username: &str,
    password: &str,
    version: &Option<String>,
    context: &Option<String>,
) -> Bytes {
    let mut bytes = BytesMut::with_capacity(
        2 + username.len()
            + password.len()
            + 8
            + version.as_ref().map(|v| v.len()).unwrap_or(0)
            + context.as_ref().map(|c| c.len()).unwrap_or(0),
    );

    #[allow(clippy::cast_possible_truncation)]
    bytes.put_u8(username.len() as u8);
    bytes.put_slice(username.as_bytes());

    #[allow(clippy::cast_possible_truncation)]
    bytes.put_u8(password.len() as u8);
    bytes.put_slice(password.as_bytes());

    match version {
        Some(v) => {
            bytes.put_u32_le(v.len() as u32);
            bytes.put_slice(v.as_bytes());
        }
        None => {
            bytes.put_u32_le(0);
        }
    }

    match context {
        Some(c) => {
            bytes.put_u32_le(c.len() as u32);
            bytes.put_slice(c.as_bytes());
        }
        None => {
            bytes.put_u32_le(0);
        }
    }

    bytes.freeze()
}

/// Simplified version: encodes username and password without version/context.
#[inline]
pub fn encode_login_auth_simple(username: &str, password: &str) -> Bytes {
    encode_login_auth(username, password, &None, &None)
}

/// Decodes login authentication payload from wire format.
pub fn decode_login_auth(
    bytes: Bytes,
) -> Result<(String, String, Option<String>, Option<String>), IggyError> {
    if bytes.len() < 4 {
        return Err(IggyError::InvalidCommand);
    }

    let mut cursor = bytes.as_ref();

    // Decode username
    let username_length = cursor[0];
    if cursor.len() < 1 + username_length as usize {
        return Err(IggyError::InvalidCommand);
    }
    let username = from_utf8(&cursor[1..=(username_length as usize)])
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();
    if username.len() != username_length as usize {
        return Err(IggyError::InvalidCommand);
    }
    cursor = &cursor[1 + username_length as usize..];

    // Decode password
    if cursor.is_empty() {
        return Err(IggyError::InvalidCommand);
    }
    let password_length = cursor[0];
    if cursor.len() < 1 + password_length as usize {
        return Err(IggyError::InvalidCommand);
    }
    let password = from_utf8(&cursor[1..1 + password_length as usize])
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();
    if password.len() != password_length as usize {
        return Err(IggyError::InvalidCommand);
    }
    cursor = &cursor[1 + password_length as usize..];

    // Decode version
    if cursor.len() < 4 {
        return Err(IggyError::InvalidCommand);
    }
    let version_length = u32::from_le_bytes(
        cursor[0..4]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    cursor = &cursor[4..];

    let version = if version_length > 0 {
        if cursor.len() < version_length as usize {
            return Err(IggyError::InvalidCommand);
        }
        let v = from_utf8(&cursor[..version_length as usize])
            .map_err(|_| IggyError::InvalidUtf8)?
            .to_string();
        cursor = &cursor[version_length as usize..];
        Some(v)
    } else {
        None
    };

    // Decode context
    if cursor.len() < 4 {
        return Err(IggyError::InvalidCommand);
    }
    let context_length = u32::from_le_bytes(
        cursor[0..4]
            .try_into()
            .map_err(|_| IggyError::InvalidNumberEncoding)?,
    );
    cursor = &cursor[4..];

    let context = if context_length > 0 {
        if cursor.len() < context_length as usize {
            return Err(IggyError::InvalidCommand);
        }
        let c = from_utf8(&cursor[..context_length as usize])
            .map_err(|_| IggyError::InvalidUtf8)?
            .to_string();
        Some(c)
    } else {
        None
    };

    Ok((username, password, version, context))
}

/// Encodes personal access token (PAT) into wire format for LOGIN_WITH_PERSONAL_ACCESS_TOKEN command.
pub fn encode_pat_auth(token: &str) -> Bytes {
    let mut buf = BytesMut::new();
    #[allow(clippy::cast_possible_truncation)]
    buf.put_u8(token.len() as u8);
    buf.put_slice(token.as_bytes());
    buf.freeze()
}

/// Decodes personal access token payload from wire format.
pub fn decode_pat_auth(bytes: Bytes) -> Result<String, IggyError> {
    if bytes.is_empty() {
        return Err(IggyError::InvalidCommand);
    }

    let token_length = bytes[0];
    if bytes.len() < 1 + token_length as usize {
        return Err(IggyError::InvalidCommand);
    }

    let token = from_utf8(&bytes[1..1 + token_length as usize])
        .map_err(|_| IggyError::InvalidUtf8)?
        .to_string();

    if token.len() != token_length as usize {
        return Err(IggyError::InvalidCommand);
    }

    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_login_auth_simple() {
        let username = "testuser";
        let password = "secretpass";

        let encoded = encode_login_auth_simple(username, password);
        let (decoded_user, decoded_pass, version, context) = decode_login_auth(encoded).unwrap();

        assert_eq!(decoded_user, username);
        assert_eq!(decoded_pass, password);
        assert_eq!(version, None);
        assert_eq!(context, None);
    }

    #[test]
    fn roundtrip_login_auth_with_version_and_context() {
        let username = "user123";
        let password = "pass456";
        let version = Some("1.2.3".to_string());
        let context = Some("test-context".to_string());

        let encoded = encode_login_auth(username, password, &version, &context);
        let (decoded_user, decoded_pass, decoded_version, decoded_context) =
            decode_login_auth(encoded).unwrap();

        assert_eq!(decoded_user, username);
        assert_eq!(decoded_pass, password);
        assert_eq!(decoded_version, version);
        assert_eq!(decoded_context, context);
    }

    #[test]
    fn roundtrip_login_auth_with_only_version() {
        let username = "user";
        let password = "pass";
        let version = Some("2.0.0".to_string());

        let encoded = encode_login_auth(username, password, &version, &None);
        let (decoded_user, decoded_pass, decoded_version, decoded_context) =
            decode_login_auth(encoded).unwrap();

        assert_eq!(decoded_user, username);
        assert_eq!(decoded_pass, password);
        assert_eq!(decoded_version, version);
        assert_eq!(decoded_context, None);
    }

    #[test]
    fn roundtrip_pat_auth() {
        let token = "pat_abc123def456";

        let encoded = encode_pat_auth(token);
        let decoded = decode_pat_auth(encoded).unwrap();

        assert_eq!(decoded, token);
    }

    #[test]
    fn golden_login_auth_format() {
        // Verify exact bytes for a known input to ensure protocol doesn't drift
        let username = "user";
        let password = "pass";

        let encoded = encode_login_auth_simple(username, password);
        let expected = vec![
            0x04, // username length
            b'u', b's', b'e', b'r', 0x04, // password length
            b'p', b'a', b's', b's', 0x00, 0x00, 0x00, 0x00, // version length (0)
            0x00, 0x00, 0x00, 0x00, // context length (0)
        ];

        assert_eq!(encoded.to_vec(), expected);
    }

    #[test]
    fn golden_pat_auth_format() {
        let token = "token123";

        let encoded = encode_pat_auth(token);
        let expected = vec![
            0x08, // token length
            b't', b'o', b'k', b'e', b'n', b'1', b'2', b'3',
        ];

        assert_eq!(encoded.to_vec(), expected);
    }

    #[test]
    fn decode_login_auth_truncated_payload() {
        let truncated = Bytes::from_static(&[0x04]); // Only username length, no actual data
        let result = decode_login_auth(truncated);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), IggyError::InvalidCommand));
    }

    #[test]
    fn decode_login_auth_invalid_utf8() {
        let mut bytes = BytesMut::new();
        bytes.put_u8(2);
        bytes.put_slice(&[0xFF, 0xFE]); // Invalid UTF-8
        bytes.put_u8(4);
        bytes.put_slice(b"pass");
        bytes.put_u32_le(0);
        bytes.put_u32_le(0);

        let result = decode_login_auth(bytes.freeze());
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), IggyError::InvalidUtf8));
    }

    #[test]
    fn decode_pat_auth_empty_payload() {
        let empty = Bytes::new();
        let result = decode_pat_auth(empty);
        assert!(result.is_err());
    }

    #[test]
    fn decode_pat_auth_truncated() {
        let truncated = Bytes::from_static(&[0x08]); // Length 8, but no data
        let result = decode_pat_auth(truncated);
        assert!(result.is_err());
    }

    #[test]
    fn login_auth_empty_version_and_context() {
        let username = "u";
        let password = "p";
        let version = Some(String::new());
        let context = Some(String::new());

        let encoded = encode_login_auth(username, password, &version, &context);
        let (u, p, v, c) = decode_login_auth(encoded).unwrap();

        assert_eq!(u, username);
        assert_eq!(p, password);
        // Empty strings are encoded as length 0, so decoded back as None
        // This is expected behavior - length 0 means "not present"
        assert_eq!(v, None);
        assert_eq!(c, None);
    }

    #[test]
    fn login_auth_special_characters() {
        let username = "user@example.com";
        let password = "p@$$w0rd!";

        let encoded = encode_login_auth_simple(username, password);
        let (u, p, _, _) = decode_login_auth(encoded).unwrap();

        assert_eq!(u, username);
        assert_eq!(p, password);
    }

    #[test]
    fn login_auth_unicode_characters() {
        let username = "пользователь";
        let password = "пароль";

        let encoded = encode_login_auth_simple(username, password);
        let (u, p, _, _) = decode_login_auth(encoded).unwrap();

        assert_eq!(u, username);
        assert_eq!(p, password);
    }

    #[test]
    fn pat_auth_with_special_characters() {
        let token = "pat_z9a8b7c6d5e4f3g2h1i0_special";

        let encoded = encode_pat_auth(token);
        let decoded = decode_pat_auth(encoded).unwrap();

        assert_eq!(decoded, token);
    }

    #[test]
    fn compatibility_login_auth_simple_matches_full_with_none() {
        let username = "test";
        let password = "pass";

        let simple = encode_login_auth_simple(username, password);
        let full = encode_login_auth(username, password, &None, &None);

        assert_eq!(simple, full);
    }
}
