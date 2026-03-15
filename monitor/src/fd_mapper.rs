use std::{cmp::max, os::fd::RawFd};

use libc::fcntl;

pub struct FdMapping {
    source: RawFd,
    target: RawFd,
    close_on_exec: bool,
}

impl FdMapping {
    pub fn new(source: RawFd, target: RawFd, close_on_exec: bool) -> FdMapping {
        Self {
            source,
            target,
            close_on_exec,
        }
    }
}

pub fn remap_fds(mappings: &[FdMapping]) {
    let remap_base = mappings
        .iter()
        .map(|mapping| max(mapping.source, mapping.target))
        .max()
        .unwrap_or(0)
        + 1;
    for (index, mapping) in mappings.iter().enumerate() {
        unsafe {
            libc::dup2(mapping.source, remap_base + (index as i32));
            libc::close(mapping.source);
        }
    }

    for (index, mapping) in mappings.iter().enumerate() {
        unsafe {
            libc::dup2(remap_base + (index as i32), mapping.target);
            libc::close(remap_base + (index as i32));
            let current_flags = fcntl(mapping.target, libc::F_GETFD);

            let new_flags = if mapping.close_on_exec {
                current_flags | libc::FD_CLOEXEC
            } else {
                current_flags & !libc::FD_CLOEXEC
            };

            fcntl(mapping.target, libc::F_SETFD, new_flags);
        }
    }
}
