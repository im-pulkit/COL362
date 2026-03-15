use std::{
    io::{Error, Write},
    os::{fd::RawFd, raw::c_void},
};

pub struct WriteFdWrapper {
    raw_fd: RawFd,
}

impl WriteFdWrapper {
    pub fn new(raw_fd: RawFd) -> impl Write {
        Self { raw_fd }
    }
}

impl Write for WriteFdWrapper {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let result = unsafe { libc::write(self.raw_fd, buf.as_ptr() as *const c_void, buf.len()) };

        if result < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(result as usize)
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
