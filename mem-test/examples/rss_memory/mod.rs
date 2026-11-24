//! Diagnostic snapshots of this process's own Darwin VM regions and malloc zones.

use std::collections::BTreeMap;
use std::mem::MaybeUninit;
use std::path::Path;
use std::sync::atomic::Ordering;

use serde::Serialize;

// Matches vm_region_extended_info_data_t in mach/vm_region.h. This flavor uses only 32-bit
// words, with the short/char members filling a single word, and has no platform-sized pointers.
#[repr(C)]
#[derive(Default)]
struct RegionInfo {
    protection: i32,
    user_tag: u32,
    pages_resident: u32,
    pages_shared_now_private: u32,
    pages_swapped_out: u32,
    pages_dirtied: u32,
    ref_count: u32,
    shadow_depth: u16,
    external_pager: u8,
    share_mode: u8,
    pages_reusable: u32,
}

unsafe extern "C" {
    fn mach_vm_region(
        task: u32,
        address: *mut u64,
        size: *mut u64,
        flavor: i32,
        info: *mut i32,
        count: *mut u32,
        object: *mut u32,
    ) -> i32;
    fn mach_port_deallocate(task: u32, port: u32) -> i32;
}

#[derive(Default, Serialize)]
struct RegionUsage {
    regions: usize,
    virtual_bytes: u64,
    resident_bytes: u64,
    reusable_bytes: u64,
}

pub(super) fn capture(directory: &Path, stage: &str) -> std::io::Result<()> {
    let requested_live = super::LIVE.load(Ordering::Relaxed);
    let rss = super::current_rss();
    let mut statistics = MaybeUninit::<libc::malloc_statistics_t>::zeroed();
    // SAFETY: A null zone aggregates the process's zones into a correctly sized output record.
    unsafe { libc::malloc_zone_statistics(std::ptr::null_mut(), statistics.as_mut_ptr()) };
    let statistics = unsafe { statistics.assume_init() };
    let mut rusage = MaybeUninit::<libc::rusage_info_v4>::zeroed();
    // SAFETY: The selected flavor matches the output buffer. Only this process is inspected.
    let status = unsafe {
        libc::proc_pid_rusage(
            libc::getpid(),
            libc::RUSAGE_INFO_V4,
            rusage.as_mut_ptr().cast::<libc::rusage_info_t>(),
        )
    };
    let rusage = (status == 0).then(|| unsafe { rusage.assume_init() });
    let (regions, walk_status) = regions();
    let value = serde_json::json!({
        "stage": stage, "rust_requested_live": requested_live, "rss_bytes": rss,
        "physical_footprint_bytes": rusage.as_ref().map(|r| r.ri_phys_footprint),
        "physical_footprint_lifetime_peak": rusage.as_ref().map(|r| r.ri_lifetime_max_phys_footprint),
        "malloc_blocks_in_use": statistics.blocks_in_use,
        "malloc_size_in_use": statistics.size_in_use,
        "malloc_max_size_in_use": statistics.max_size_in_use,
        "malloc_size_allocated": statistics.size_allocated,
        "vm_region_walk_status": walk_status,
        "regions_by_vm_tag": regions,
        "scope": "Self VM region counters and malloc zone statistics. Tag 30 is stack; malloc tags include 1, 2, 3, 7, 8, 9, 11. Region resident sums can include shared aliases. Reserved virtual bytes are not resident bytes. Observations are adjacent, not atomic.",
    });
    serde_json::to_writer_pretty(
        std::fs::File::create(directory.join(format!("{stage}.json")))?,
        &value,
    )
    .map_err(std::io::Error::other)
}

#[allow(deprecated)]
fn regions() -> (BTreeMap<u32, RegionUsage>, i32) {
    let mut regions = BTreeMap::<u32, RegionUsage>::new();
    let mut address = 0u64;
    let task = unsafe { libc::mach_task_self() };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
    loop {
        let mut size = 0u64;
        let mut count = (size_of::<RegionInfo>() / size_of::<u32>()) as u32;
        let mut info = RegionInfo::default();
        let mut object = 0u32;
        // SAFETY: The extended-info flavor (13) matches RegionInfo's SDK-defined layout and
        // word count. Outputs are aligned, initialized, and used only after a successful query.
        let status = unsafe {
            mach_vm_region(
                task,
                &mut address,
                &mut size,
                13,
                (&mut info as *mut RegionInfo).cast(),
                &mut count,
                &mut object,
            )
        };
        if status != 0 {
            return (regions, status);
        }
        if object != 0 {
            // SAFETY: Release the send right returned by mach_vm_region for this task.
            unsafe { mach_port_deallocate(task, object) };
        }
        let usage = regions.entry(info.user_tag).or_default();
        usage.regions += 1;
        usage.virtual_bytes += size;
        usage.resident_bytes += u64::from(info.pages_resident) * page_size;
        usage.reusable_bytes += u64::from(info.pages_reusable) * page_size;
        let Some(next) = address.checked_add(size).filter(|&next| next > address) else {
            return (regions, 1);
        };
        address = next;
    }
}
