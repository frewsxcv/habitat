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

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::Ordering;

use core::os::process::Pid;
use protobuf;
use protocol;
use zmq;

use error::{Error, Result};
use manager::{IS_STOPPING, MGR_INPROC};

type Env = HashMap<String, String>;

pub struct LauncherCli {
    msg_buf: zmq::Message,
    sock: zmq::Socket,
}

impl LauncherCli {
    pub fn connect(context: &mut zmq::Context) -> Result<Self> {
        let sock = context.socket(zmq::REQ)?;
        sock.connect(MGR_INPROC)?;
        Ok(LauncherCli {
            msg_buf: zmq::Message::new()?,
            sock: sock,
        })
    }

    /// Restart a running process with the same arguments
    pub fn restart(&mut self, pid: Pid) -> Result<Pid> {
        let mut msg = protocol::Restart::new();
        msg.set_pid(pid.into());
        self.send(&msg)?;
        let reply = self.wait_recv::<protocol::SpawnOk>()?;
        Ok(reply.get_pid() as Pid)
    }

    /// Send a process spawn command to the connected Launcher
    pub fn spawn<I, B, U, G, P>(
        &mut self,
        id: I,
        bin: B,
        user: U,
        group: G,
        password: Option<P>,
        env: Env,
    ) -> Result<Pid>
    where
        I: ToString,
        B: AsRef<Path>,
        U: ToString,
        G: ToString,
        P: ToString,
    {
        let mut msg = protocol::Spawn::new();
        msg.set_binary(bin.as_ref().to_path_buf().to_string_lossy().into_owned());
        msg.set_svc_user(user.to_string());
        msg.set_svc_group(group.to_string());
        if let Some(password) = password {
            msg.set_svc_password(password.to_string());
        }
        msg.set_env(env);
        msg.set_id(id.to_string());
        self.send(&msg)?;
        let reply = self.wait_recv::<protocol::SpawnOk>()?;
        Ok(reply.get_pid() as Pid)
    }

    pub fn terminate(&mut self, pid: Pid) -> Result<i32> {
        let mut msg = protocol::Terminate::new();
        msg.set_pid(pid.into());
        self.send(&msg)?;
        let reply = self.wait_recv::<protocol::TerminateOk>()?;
        Ok(reply.get_exit_code())
    }

    /// Send a command to a Launcher
    fn send<T>(&self, message: &T) -> Result<()>
    where
        T: protobuf::MessageStatic,
    {
        let txn = protocol::NetTxn::build(message).map_err(Error::Serialize)?;
        let bytes = txn.to_bytes().map_err(Error::Serialize)?;
        self.sock.send(&bytes, 0).map_err(Error::Socket)?;
        Ok(())
    }

    /// Receive and read protocol message from an IpcReceiver
    fn wait_recv<T>(&mut self) -> Result<T>
    where
        T: protobuf::MessageStatic,
    {
        self.sock.recv(&mut self.msg_buf, 0)?;
        read_msg(&*self.msg_buf)
    }
}

/// Read a launcher protocol message from a byte array
pub fn read_msg<T>(bytes: &[u8]) -> Result<T>
where
    T: protobuf::MessageStatic,
{
    let txn = protocol::NetTxn::from_bytes(bytes).map_err(
        Error::Deserialize,
    )?;
    if txn.message_id() == "NetErr" {
        let err = txn.decode::<protocol::NetErr>().map_err(Error::Deserialize)?;
        return Err(Error::Protocol(err));
    }
    if txn.message_id() == "Shutdown" {
        IS_STOPPING.store(true, Ordering::SeqCst);
        txn.decode::<protocol::Shutdown>().map_err(
            Error::Deserialize,
        )?;
        return Err(Error::Shutdown);
    }
    let msg = txn.decode::<T>().map_err(Error::Deserialize)?;
    Ok(msg)
}
