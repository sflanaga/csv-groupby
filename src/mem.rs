use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

#[cfg(target_os = "linux")]
use jemalloc_ctl::{epoch, stats};
#[cfg(target_os = "linux")]
use jemallocator::Jemalloc;

#[derive(Debug)]
pub struct CounterTlsToAtomicUsize;
#[derive(Debug)]
pub struct CounterAtomicUsize;
#[derive(Debug)]
pub struct CounterUsize;

static ALLOCATED_TRACKER: AtomicUsize = AtomicUsize::new(0);

static mut ALLOCATED_NUM: usize = 0;

use std::cell::RefCell;

pub struct AllocTrackChunk {
    pub count: u32,
    pub sum: usize,
}

struct MemSettings {
    update_count: u32,
    update_bytes: usize,
}

static mut MEM_SETTINGS: MemSettings = MemSettings {
    update_bytes: 256 * 1024,
    update_count: 10,
};

thread_local! {
    pub static ALLOCS_LOCAL: RefCell<AllocTrackChunk> = RefCell::new(AllocTrackChunk{ count: 0, sum: 0});
    pub static DEALLOCS_LOCAL: RefCell<AllocTrackChunk> = RefCell::new(AllocTrackChunk{ count: 0, sum: 0});
}
pub trait GetAlloc {
    fn get_alloc(&self) -> usize;
}

impl GetAlloc for CounterTlsToAtomicUsize {
    fn get_alloc(&self) -> usize {
        ALLOCATED_TRACKER.load(Relaxed)
    }
}
impl GetAlloc for CounterAtomicUsize {
    fn get_alloc(&self) -> usize {
        ALLOCATED_TRACKER.load(Relaxed)
    }
}
impl GetAlloc for CounterUsize {
    fn get_alloc(&self) -> usize {
        unsafe { ALLOCATED_NUM }
    }
}

impl GetAlloc for System {
    fn get_alloc(&self) -> usize {
        0
    }
}


#[cfg(target_os = "linux")]
impl GetAlloc for Jemalloc {
    fn get_alloc(&self) -> usize {
        epoch::advance().unwrap();
        stats::active::read().unwrap()
    }
}


pub fn set_alloc_settings(update_bytes: usize, update_count: u32) {
    unsafe {
        MEM_SETTINGS.update_count = update_count;
        MEM_SETTINGS.update_bytes = update_bytes;
    }
}

fn my_track_alloc(size: usize) {
    ALLOCATED_TRACKER.fetch_add(size, Relaxed);
}

fn my_track_dealloc(size: usize) {
    ALLOCATED_TRACKER.fetch_sub(size, Relaxed);
}

unsafe impl GlobalAlloc for CounterTlsToAtomicUsize {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc(layout);

        if !ret.is_null() {
            let toadd = ALLOCS_LOCAL.with(|f| {
                let mut x = f.borrow_mut();
                x.count += 1;
                let memsize = layout.size();
                x.sum += memsize;
                let mut toret = 0;
                if x.sum > MEM_SETTINGS.update_bytes || x.count > MEM_SETTINGS.update_count {
                    toret = x.sum;
                    x.count = 0;
                    x.sum = 0;
                }
                toret
            });
            if toadd > 0 {
                //DIRECT_NUM += toadd;
                my_track_alloc(toadd);
            }
        }
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);

        let toadd = DEALLOCS_LOCAL.with(|f| {
            let mut x = f.borrow_mut();
            x.count += 1;
            let memsize = layout.size();
            //if memsize > 11446744073704130855 { panic!("funky DE-alloc size?!!!{}", memsize); }
            x.sum += memsize;
            let mut toret = 0;
            if x.sum > MEM_SETTINGS.update_bytes || MEM_SETTINGS.update_count > 10 {
                toret = x.sum;
                //ALLOCATED_TRACKER.fetch_sub(x.sum, Relaxed);
                x.count = 0;
                x.sum = 0;
            }
            toret
        });
        if toadd > 0 {
            // DIRECT_NUM -= toadd;
            my_track_dealloc(toadd);
        }
    }
}

unsafe impl GlobalAlloc for CounterAtomicUsize {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc(layout);
        if !ret.is_null() {
            my_track_alloc(layout.size());
        }
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        my_track_dealloc(layout.size());
    }
}

// essentially wrong but here to test performance vs others
unsafe impl GlobalAlloc for CounterUsize {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc(layout);
        if !ret.is_null() {
            ALLOCATED_NUM += layout.size();
        }
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        ALLOCATED_NUM -= layout.size();
    }
}
