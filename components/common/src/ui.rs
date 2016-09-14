// Copyright (c) 2016 Chef Software Inc. and/or applicable contributors
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

use std::fmt;
use std::io::{self, Read, Write};

use ansi_term::Colour;
use term::{Terminal, TerminfoTerminal};
use term::terminfo::TermInfo;

use error::Result;
use self::tty::StdStream;

pub enum Status {
    Applying,
    Cached,
    Creating,
    Downloading,
    Encrypting,
    Installed,
    Missing,
    Signing,
    Signed,
    Uploaded,
    Uploading,
    Using,
    Verified,
    Custom(char, String),
}

impl Status {
    pub fn parts(&self) -> (char, String, Colour) {
        match *self {
            Status::Applying => ('↑', "Applying".into(), Colour::Green),
            Status::Cached => ('☑', "Cached".into(), Colour::Green),
            Status::Creating => ('Ω', "Creating".into(), Colour::Green),
            Status::Downloading => ('↓', "Downloading".into(), Colour::Green),
            Status::Encrypting => ('☛', "Encypting".into(), Colour::Green),
            Status::Installed => ('✓', "Installed".into(), Colour::Green),
            Status::Missing => ('∵', "Missing".into(), Colour::Cyan),
            Status::Signed => ('✓', "Signed".into(), Colour::Cyan),
            Status::Signing => ('☛', "Signing".into(), Colour::Cyan),
            Status::Uploaded => ('✓', "Uploaded".into(), Colour::Green),
            Status::Uploading => ('↑', "Uploading".into(), Colour::Green),
            Status::Using => ('→', "Using".into(), Colour::Green),
            Status::Verified => ('✓', "Verified".into(), Colour::Green),
            Status::Custom(c, ref s) => (c, s.to_string(), Colour::Green),
        }
    }
}

pub struct UI {
    shell: Shell,
}

impl UI {
    pub fn default_with(coloring: Coloring) -> Self {
        UI { shell: Shell::default_with(coloring) }
    }

    pub fn begin<T: ToString>(&mut self, message: T) -> Result<()> {
        Self::write_heading(&mut self.shell.out, Colour::Yellow, '»', message)
    }

    pub fn end<T: ToString>(&mut self, message: T) -> Result<()> {
        Self::write_heading(&mut self.shell.out, Colour::Blue, '★', message)
    }

    pub fn status<T: fmt::Display>(&mut self, status: Status, message: T) -> Result<()> {
        let ref mut stream = self.shell.out;
        let (symbol, status_str, color) = status.parts();
        match stream.is_colored() {
            true => {
                try!(write!(stream,
                            "{} {}\n",
                            color.bold().paint(format!("{} {}", symbol, status_str)),
                            message.to_string()))
            }
            false => {
                try!(write!(stream,
                            "{} {} {}\n",
                            symbol,
                            status_str,
                            message.to_string()))
            }
        }
        try!(stream.flush());
        Ok(())
    }

    fn write_heading<T: ToString>(stream: &mut OutputStream,
                                  color: Colour,
                                  symbol: char,
                                  message: T)
                                  -> Result<()> {
        match stream.is_colored() {
            true => {
                try!(write!(stream,
                            "{}\n",
                            color.bold().paint(format!("{} {}", symbol, message.to_string()))))
            }
            false => try!(write!(stream, "{} {}\n", symbol, message.to_string())),
        }
        try!(stream.flush());
        Ok(())
    }
}

impl Default for UI {
    fn default() -> Self {
        UI::default_with(Coloring::Auto)
    }
}

pub struct Shell {
    input: InputStream,
    out: OutputStream,
    err: OutputStream,
}

impl Shell {
    pub fn new(input: InputStream, out: OutputStream, err: OutputStream) -> Self {
        Shell {
            input: input,
            out: out,
            err: err,
        }
    }

    pub fn default_with(coloring: Coloring) -> Self {
        let stdin = InputStream::from_stdin();
        debug!("InputStream(stdin): {{ is_a_terminal(): {} }}",
               stdin.is_a_terminal());
        let stdout = OutputStream::from_stdout(coloring);
        debug!("OutputStream(stdout): {{ is_colored(): {}, supports_color(): {}, \
                is_a_terminal(): {} }}",
               stdout.is_colored(),
               stdout.supports_color(),
               stdout.is_a_terminal());
        let stderr = OutputStream::from_stderr(coloring);
        debug!("OutputStream(stderr): {{ is_colored(): {}, supports_color(): {}, \
                is_a_terminal(): {} }}",
               stderr.is_colored(),
               stderr.supports_color(),
               stderr.is_a_terminal());
        Shell::new(stdin, stdout, stderr)
    }

    pub fn input(&mut self) -> &mut InputStream {
        &mut self.input
    }

    pub fn out(&mut self) -> &mut OutputStream {
        &mut self.out
    }

    pub fn err(&mut self) -> &mut OutputStream {
        &mut self.err
    }
}

impl Default for Shell {
    fn default() -> Self {
        Shell::default_with(Coloring::Auto)
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum Coloring {
    Auto,
    Always,
    Never,
}

pub struct InputStream {
    inner: Box<Read + Send>,
    isatty: bool,
}

impl InputStream {
    pub fn new(inner: Box<Read + Send>, isatty: bool) -> Self {
        InputStream {
            inner: inner,
            isatty: isatty,
        }
    }

    pub fn from_stdin() -> Self {
        Self::new(Box::new(io::stdin()), tty::isatty(StdStream::Stdin))
    }

    pub fn is_a_terminal(&self) -> bool {
        self.isatty
    }
}

impl Read for InputStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

pub struct OutputStream {
    inner: WriteStream,
    coloring: Coloring,
    isatty: bool,
}

impl OutputStream {
    pub fn new(inner: WriteStream, coloring: Coloring, isatty: bool) -> Self {
        OutputStream {
            inner: inner,
            coloring: coloring,
            isatty: isatty,
        }
    }

    pub fn from_stdout(coloring: Coloring) -> Self {
        Self::new(WriteStream::create(|| Box::new(io::stdout())),
                  coloring,
                  tty::isatty(StdStream::Stdout))
    }

    pub fn from_stderr(coloring: Coloring) -> Self {
        Self::new(WriteStream::create(|| Box::new(io::stderr())),
                  coloring,
                  tty::isatty(StdStream::Stderr))
    }

    pub fn supports_color(&self) -> bool {
        match self.inner {
            WriteStream::Color(_) => true,
            WriteStream::NoColor(_) => false,

        }
    }

    pub fn is_colored(&self) -> bool {
        self.supports_color() &&
        ((self.isatty && Coloring::Auto == self.coloring) || Coloring::Always == self.coloring)
    }

    pub fn is_a_terminal(&self) -> bool {
        self.isatty
    }
}

impl Write for OutputStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.inner {
            WriteStream::Color(ref mut io) => io.write(buf),
            WriteStream::NoColor(ref mut io) => io.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.inner {
            WriteStream::Color(ref mut io) => io.flush(),
            WriteStream::NoColor(ref mut io) => io.flush(),
        }
    }
}

pub enum WriteStream {
    NoColor(Box<Write + Send>),
    Color(Box<Terminal<Output = Box<Write + Send>> + Send>),
}

impl WriteStream {
    // Implementation heavily inspired and based on the Cargo `shell.rs` implementation. Source:
    // https://github.com/rust-lang/cargo/blob/d05ba53afec82308edcfeb778446010bf18e71ae/src/cargo/core/shell.rs

    pub fn create<T: FnMut() -> Box<Write + Send>>(mut writable_fn: T) -> Self {
        match Self::get_term(writable_fn()) {
            Ok(t) => t,
            Err(_) => WriteStream::NoColor(writable_fn()),
        }
    }

    #[cfg(any(windows))]
    fn get_term(writeable: Box<Write + Send>) -> Result<Self> {
        // Check if the creation of a console will succeed
        if ::term::WinConsole::new(vec![0u8; 0]).is_ok() {
            let term = try!(::term::WinConsole::new(writeable));
            if !term.supports_color() {
                Ok(WriteStream::NoColor(Box::new(term)))
            } else {
                Ok(WriteStream::Color(Box::new(term)))
            }
        } else {
            // If we fail to get a windows console, we try to get a `TermInfo` one
            Ok(Shell::get_terminfo_term(out))
        }
    }

    #[cfg(any(unix))]
    fn get_term(writeable: Box<Write + Send>) -> Result<Self> {
        Ok(Self::get_terminfo_term(writeable))
    }

    fn get_terminfo_term(writeable: Box<Write + Send>) -> Self {
        // Use `TermInfo::from_env()` and `TerminfoTerminal::supports_color()` to determine if
        // creation of a TerminfoTerminal is possible regardless of the tty status. --color options
        // are parsed after Shell creation so always try to create a terminal that supports color
        // output. Fall back to a no-color terminal regardless of whether or not a tty is present
        // and if color output is not possible.
        match TermInfo::from_env() {
            Ok(info) => {
                let term = TerminfoTerminal::new_with_terminfo(writeable, info);
                if !term.supports_color() {
                    WriteStream::NoColor(term.into_inner())
                } else {
                    WriteStream::Color(Box::new(term))
                }
            }
            Err(_) => WriteStream::NoColor(writeable),
        }
    }
}

mod tty {
    pub enum StdStream {
        Stdin,
        Stdout,
        Stderr,
    }

    #[cfg(unix)]
    pub fn isatty(output: StdStream) -> bool {
        extern crate libc;

        let fd = match output {
            StdStream::Stdin => libc::STDIN_FILENO,
            StdStream::Stdout => libc::STDOUT_FILENO,
            StdStream::Stderr => libc::STDERR_FILENO,
        };

        unsafe { libc::isatty(fd) != 0 }
    }
    #[cfg(windows)]
    pub fn isatty(output: StdStream) -> bool {
        extern crate kernel32;
        extern crate winapi;

        let handle = match output {
            StdStream::Stdin => winapi::winbase::STD_INPUT_HANDLE,
            StdStream::Stdout => winapi::winbase::STD_OUTPUT_HANDLE,
            StdStream::Stderr => winapi::winbase::STD_ERROR_HANDLE,
        };

        unsafe {
            let handle = kernel32::GetStdHandle(handle);
            let mut out = 0;
            kernel32::GetConsoleMode(handle, &mut out) != 0
        }
    }
}