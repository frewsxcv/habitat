// Copyright (c) 2017 Chef Software Inc. and/or applicable contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering, ATOMIC_BOOL_INIT};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::thread;

use ipc_channel::ipc::{IpcOneShotServer, IpcReceiver, IpcSender};
use protobuf;
use protocol;
use zmq;

use error::{Error, Result};
use client::read_msg;

pub const MGR_INPROC: &'static str = "inproc://launcher-mgr.broker";
pub static IS_STOPPING: AtomicBool = ATOMIC_BOOL_INIT;

type IpcServer = IpcOneShotServer<Vec<u8>>;

pub struct LauncherMgr {
    cli_sock: zmq::Socket,
    tx: IpcSender<Vec<u8>>,
    rx: IpcReceiver<Vec<u8>>,
}

impl LauncherMgr {
    pub fn boot(context: &mut zmq::Context, pipe: String) -> Result<()> {
        let tx = IpcSender::connect(pipe).map_err(Error::Connect)?;
        let (ipc_srv, pipe) = IpcServer::new().map_err(Error::BadPipe)?;
        let mut cmd = protocol::Register::new();
        cmd.set_pipe(pipe);
        send(&tx, &cmd)?;
        let (rx, raw) = ipc_srv.accept().map_err(|_| Error::AcceptConn)?;
        read_msg::<protocol::NetOk>(&raw)?;
        let cli_sock = context.socket(zmq::ROUTER)?;
        cli_sock.set_router_mandatory(true)?;
        let mgr = LauncherMgr {
            cli_sock: cli_sock,
            tx: tx,
            rx: rx,
        };
        let (tx, rx) = sync_channel(0);
        thread::Builder::new()
            .name("launcher-mgr".to_string())
            .spawn(move || mgr.run(tx))
            .expect("couldn't start launcher-mgr thread");
        rx.recv().unwrap();
        Ok(())
    }

    pub fn is_stopping() -> bool {
        IS_STOPPING.load(Ordering::SeqCst)
    }

    fn run(&self, rz: SyncSender<()>) {
        self.cli_sock.bind(MGR_INPROC).expect(
            "launcher-mgr failed to inproc bind",
        );
        let mut snd_buf = zmq::Message::new().unwrap();
        let mut msg_buf = zmq::Message::new().unwrap();
        rz.send(()).unwrap();
        loop {
            match try_recv::<protocol::Shutdown>(&self.rx) {
                Ok(Some(_)) |
                Err(Error::IPCIO(_)) => break,
                Ok(None) => (),
                Err(_) => break,
            }
            match self.cli_sock.poll(zmq::POLLIN, 1_000) {
                Ok(count) if count < 0 => unreachable!("zmq::poll, returned a negative count"),
                Ok(count) if count == 0 => continue,
                Ok(_) => (),
                Err(_) => break,
            }
            if self.recv_request(&mut snd_buf, &mut msg_buf).is_err() {
                break;
            }
            if self.send_request(&msg_buf).is_err() {
                break;
            }
            let response = match self.rx.recv() {
                Ok(bytes) => bytes,
                Err(_) => break,
            };
            if self.send_response(&snd_buf, &response).is_err() {
                break;
            }
        }
        IS_STOPPING.store(true, Ordering::SeqCst);
    }

    fn recv_request(&self, snd_buf: &mut zmq::Message, msg_buf: &mut zmq::Message) -> Result<()> {
        self.cli_sock.recv(snd_buf, 0)?;
        self.cli_sock.recv(msg_buf, 0)?;
        self.cli_sock.recv(msg_buf, 0).map_err(Error::Socket)
    }

    fn send_request(&self, msg_buf: &zmq::Message) -> Result<()> {
        self.tx.send((*msg_buf).to_vec()).map_err(Error::Send)
    }

    fn send_response(&self, snd_buf: &zmq::Message, msg: &[u8]) -> Result<()> {
        self.cli_sock.send(&snd_buf, zmq::SNDMORE)?;
        self.cli_sock.send(&[], zmq::SNDMORE)?;
        self.cli_sock.send(msg, 0).map_err(Error::Socket)
    }
}

/// Send a command to a Launcher
fn send<T>(tx: &IpcSender<Vec<u8>>, message: &T) -> Result<()>
where
    T: protobuf::MessageStatic,
{
    let txn = protocol::NetTxn::build(message).map_err(Error::Serialize)?;
    let bytes = txn.to_bytes().map_err(Error::Serialize)?;
    tx.send(bytes).map_err(Error::Send)?;
    Ok(())
}

/// Receive and read protocol message from an IpcReceiver
fn try_recv<T>(rx: &IpcReceiver<Vec<u8>>) -> Result<Option<T>>
where
    T: protobuf::MessageStatic,
{
    match rx.try_recv().map_err(|err| Error::from(*err)) {
        Ok(bytes) => {
            let msg = read_msg::<T>(&bytes)?;
            Ok(Some(msg))
        }
        Err(Error::IPCIO(io::ErrorKind::WouldBlock)) => Ok(None),
        Err(err) => Err(err),
    }
}
