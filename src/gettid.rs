
// fixed gettid - not sure why it does not cast to usize for windows

//! A crate to help with fetching thread ids across multiple platforms.

#[cfg(any(target_os = "linux", target_os = "android", rustdoc))]
mod imp {
    pub fn gettid() -> usize {
        unsafe { libc::syscall(libc::SYS_gettid) as usize }
    }
}

#[cfg(target_os = "macos")]
mod imp {
    #[link(name = "pthread")]
    extern "C" {
        fn pthread_threadid_np(thread: libc::pthread_t, thread_id: *mut libc::uint64_t) -> libc::c_int;
    }

    pub fn gettid() -> u64 {
        let mut result = 0;
        unsafe {let _ = pthread_threadid_np(0, &mut result); }
        result
    }
}

#[cfg(target_os = "windows")]
mod imp {
    pub fn gettid() -> usize {
        unsafe { winapi::um::processthreadsapi::GetCurrentThreadId() as usize }
    }
}

pub use imp::*;
