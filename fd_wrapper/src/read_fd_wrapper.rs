use std::{
    io::{Error, Read},
    os::{fd::RawFd, raw::c_void},
};

pub struct ReadFdWrapper {
    raw_fd: RawFd,
}

impl ReadFdWrapper {
    pub fn new(raw_fd: RawFd) -> impl Read {
        Self { raw_fd }
    }
}

impl Read for ReadFdWrapper {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let result = unsafe { libc::read(self.raw_fd, buf.as_mut_ptr() as *mut c_void, buf.len()) };

        if result < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(result as usize)
        }
    }
}
