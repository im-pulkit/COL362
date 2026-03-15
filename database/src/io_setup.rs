use std::{
    io::{Read, Write},
    os::fd::RawFd,
};

use fd_wrapper::{ReadFdWrapper, WriteFdWrapper};

const DISK_INPUT_FD: RawFd = 3;
const DISK_OUPUT_FD: RawFd = 4;

const MONITOR_INPUT_FD: RawFd = 5;
const MONITOR_OUPUT_FD: RawFd = 6;

pub fn setup_disk_io() -> (impl Read, impl Write) {
    let disk_in = ReadFdWrapper::new(DISK_INPUT_FD);
    let disk_out = WriteFdWrapper::new(DISK_OUPUT_FD);

    (disk_in, disk_out)
}

pub fn setup_monitor_io() -> (impl Read, impl Write) {
    let monitor_in = ReadFdWrapper::new(MONITOR_INPUT_FD);
    let monitor_out = WriteFdWrapper::new(MONITOR_OUPUT_FD);

    (monitor_in, monitor_out)
}
