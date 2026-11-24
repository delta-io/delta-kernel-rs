#!/usr/bin/env python3
"""Attribute live Rust allocation bytes at DHAT's single global heap peak."""
import collections
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    profile = json.load(source)

groups = collections.Counter()
sites = []
for point in profile["pps"]:
    live = point["gb"]
    if not live:
        continue
    frames = [profile["ftbl"][index] for index in point["fs"]]
    stack = "\n".join(frames)
    if "KernelAllocator" in stack:
        category = "kernel_allocator"
    elif "arrow_json::writer::Writer" in stack:
        category = "arrow_json_writer"
    elif "TypedGetData<alloc::string::String>" in stack or "actions::Metadata" in stack:
        category = "metadata_strings"
    elif "arrow_" in stack and any(item in stack for item in ["MutableBuffer", "BufferBuilder", "ArrayBuilder", "TapeDecoder"]):
        category = "arrow_buffers_and_parser"
    elif any(item in stack for item in ["arrow_array::", "arrow_data::", "arrow_schema::", "arrow_json::"]):
        category = "arrow_objects_schemas_and_decoder_state"
    elif "Vec<delta_kernel::path::ParsedLogPath>" in stack or "url::" in stack:
        category = "log_descriptors_and_urls"
    elif any(item in stack for item in ["delta_kernel::schema::", "indexmap::"]):
        category = "schema_structures"
    else:
        category = "other"
    groups[category] += live
    relevant = [frame.split(": ", 1)[-1].split(" (")[0] for frame in frames
                if any(name in frame for name in ["delta_kernel::", "arrow_", "serde_json::", "url::"])]
    sites.append({"bytes": live, "category": category, "frames": relevant[:3]})

total = sum(groups.values())
print(json.dumps({
    "global_peak_requested_rust_bytes": total,
    "kernel_live_at_global_peak": groups["kernel_allocator"],
    "kernel_fraction_at_global_peak": groups["kernel_allocator"] / total if total else 0,
    "groups": dict(groups.most_common()),
    "largest_sites": sorted(sites, key=lambda site: site["bytes"], reverse=True)[:20],
    "scope": "Allocations tracked after profiler start; excludes direct libc/C++, RSS, fragmentation and stacks. Stack grouping is diagnostic, not enforcement.",
}, indent=2))
