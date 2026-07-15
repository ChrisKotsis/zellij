use crate::consts::WEBSERVER_SOCKET_PATH;
use crate::errors::prelude::*;
use crate::input::config::Config;
use interprocess::local_socket::LocalSocketStream;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, BufWriter, Read, Write};
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub fn shutdown_all_webserver_instances() -> Result<()> {
    let entries = fs::read_dir(&*WEBSERVER_SOCKET_PATH)?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if let Some(file_name) = path.file_name() {
            if let Some(_file_name_str) = file_name.to_str() {
                let metadata = entry.metadata()?;
                let file_type = metadata.file_type();

                if file_type.is_socket() {
                    match create_webserver_sender(path.to_str().unwrap_or("")) {
                        Ok(mut sender) => {
                            let _ = send_webserver_instruction(
                                &mut sender,
                                InstructionForWebServer::ShutdownWebServer,
                            );
                        },
                        Err(_) => {
                            // no-op
                        },
                    }
                }
            }
        }
    }
    Ok(())
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InstructionForWebServer {
    ShutdownWebServer,
    ConfigWrittenToDisk(Config),
    // LOCAL PATCH (isahc removal, 2026-07-14): version/status query over this
    // IPC bus, replacing the session server's isahc HTTP poll (upstream did
    // the same after 0.43.1). Appended last so the wire encoding of the
    // pre-existing variants is unchanged across binary versions.
    QueryVersion,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VersionInfo {
    pub version: String,
    pub ip: String,
    pub port: u16,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WebServerResponse {
    Version(VersionInfo),
}

pub fn discover_webserver_sockets() -> Result<Vec<PathBuf>> {
    let mut sockets = Vec::new();
    if !WEBSERVER_SOCKET_PATH.exists() {
        return Ok(sockets);
    }
    for entry in fs::read_dir(&*WEBSERVER_SOCKET_PATH)? {
        let entry = entry?;
        if entry.metadata()?.file_type().is_socket() {
            sockets.push(entry.path());
        }
    }
    Ok(sockets)
}

/// Ask a web server instance for its version and bound address over its IPC
/// socket. The listener protocol is one instruction per connection, delimited
/// by EOF — so the write side is half-closed after sending, then the response
/// is read until the server closes its end.
pub fn query_webserver_version(socket_path: &Path, timeout: Duration) -> Result<VersionInfo> {
    let mut stream = std::os::unix::net::UnixStream::connect(socket_path)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    rmp_serde::encode::write(&mut stream, &InstructionForWebServer::QueryVersion)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf)?;
    let response: WebServerResponse =
        rmp_serde::from_slice(&buf).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let WebServerResponse::Version(info) = response;
    Ok(info)
}

pub fn create_webserver_sender(path: &str) -> Result<BufWriter<LocalSocketStream>> {
    let stream = LocalSocketStream::connect(path)?;
    Ok(BufWriter::new(stream))
}

pub fn send_webserver_instruction(
    sender: &mut BufWriter<LocalSocketStream>,
    instruction: InstructionForWebServer,
) -> Result<()> {
    rmp_serde::encode::write(sender, &instruction)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    sender.flush()?;
    Ok(())
}

#[cfg(test)]
mod web_server_query_tests {
    use super::*;
    use std::io::{Read as _, Write as _};
    use std::os::unix::net::UnixListener;

    // Mirrors the web server's ipc_listener protocol: EOF-delimited rmp
    // instruction in, rmp response out on the same stream, then close.
    #[test]
    fn query_version_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("ipctest");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).unwrap();
            let instruction: InstructionForWebServer = rmp_serde::from_slice(&buf).unwrap();
            assert!(matches!(instruction, InstructionForWebServer::QueryVersion));
            let response = WebServerResponse::Version(VersionInfo {
                version: "0.43.1".to_owned(),
                ip: "0.0.0.0".to_owned(),
                port: 8082,
            });
            let bytes = rmp_serde::to_vec(&response).unwrap();
            stream.write_all(&bytes).unwrap();
        });
        let info = query_webserver_version(&socket_path, Duration::from_millis(2000)).unwrap();
        server.join().unwrap();
        assert_eq!(info.version, "0.43.1");
        assert_eq!(info.ip, "0.0.0.0");
        assert_eq!(info.port, 8082);
    }
}
