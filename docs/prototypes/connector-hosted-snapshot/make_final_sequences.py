from html import escape
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parent


class Sequence:
    def __init__(self, title, subtitle):
        self.parts = []
        self.y = 150
        self.title = title
        self.subtitle = subtitle

    def text(self, x, y, lines, size=18, anchor="middle"):
        for i, line in enumerate(lines):
            self.parts.append(
                f'<text x="{x}" y="{y + i * 24}" text-anchor="{anchor}" '
                f'font-family="DejaVu Sans" font-size="{size}" fill="#17324d">'
                f'{escape(line)}</text>')

    def arrow(self, label, direction="right", detail=None):
        lines = [label] + ([detail] if detail else [])
        self.y += 24 * len(lines)
        self.text(600, self.y - 24 * (len(lines) - 1) - 10, lines, 17)
        x1, x2 = (175, 1025) if direction == "right" else (1025, 175)
        dash = ' stroke-dasharray="7 5"' if direction == "return" else ""
        self.parts.append(
            f'<path d="M{x1},{self.y + 6} H{x2}" fill="none" stroke="#334e68" '
            f'stroke-width="2" marker-end="url(#arrow)"{dash}/>')
        self.y += 48

    def note(self, side, lines, warn=False):
        x, width = ((35, 505) if side == "left" else
                    (660, 505) if side == "right" else (80, 1040))
        height = 24 * len(lines) + 26
        fill = "#fff3d8" if warn else "#eaf2fb" if side == "left" else "#e5f3ec"
        self.parts.append(
            f'<rect x="{x}" y="{self.y}" width="{width}" height="{height}" rx="8" '
            f'fill="{fill}" stroke="#90a5b5"/>')
        self.text(x + width / 2, self.y + 27, lines, 17)
        self.y += height + 25

    def save(self, name):
        height = self.y + 25
        head = (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="{height}" '
            f'viewBox="0 0 1200 {height}"><defs><marker id="arrow" markerWidth="10" '
            f'markerHeight="8" refX="9" refY="4" orient="auto"><path '
            f'd="M0,0 L10,4 L0,8" fill="#334e68"/></marker></defs>'
            f'<rect width="1200" height="{height}" fill="white"/>')
        title = (
            f'<text x="600" y="35" text-anchor="middle" font-family="DejaVu Sans" '
            f'font-size="26" fill="#17324d">{escape(self.title)}</text>'
            f'<text x="600" y="66" text-anchor="middle" font-family="DejaVu Sans" '
            f'font-size="16" fill="#52606d">{escape(self.subtitle)}</text>')
        lifelines = ""
        for x, label, color in [
                (175, "Java connector / engine", "#eaf2fb"),
                (1025, "Rust kernel", "#e5f3ec")]:
            lifelines += (
                f'<path d="M{x},126 V{height - 20}" stroke="#a7b7c5" '
                f'stroke-dasharray="6 6"/><rect x="{x - 160}" y="85" width="320" '
                f'height="44" rx="7" fill="{color}" stroke="#718a9f"/>'
                f'<text x="{x}" y="113" text-anchor="middle" '
                f'font-family="DejaVu Sans" font-size="19">{label}</text>')
        svg = ROOT / f"{name}.svg"
        svg.write_text(head + title + lifelines + "".join(self.parts) + "</svg>")
        subprocess.run([
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-i", str(svg),
            "-frames:v", "1", "-y", str(ROOT / f"{name}.png")], check=True)


creation = Sequence(
    "Snapshot construction and validated handoff",
    "Final p100 candidate; Java left, Rust right; calls grouped for readability")
creation.arrow("1. Load source snapshot with the construction engine")
creation.note("right", [
    "Read log state and materialize one native Snapshot.",
    "Validate protocol, metadata, logical and physical schemas."])
creation.arrow("Return source snapshot handle", "return")
creation.arrow("2. Read immutable components used to build SnapshotHint")
creation.arrow("Return version, metadata and protocol", "return")
creation.note("left", [
    "Create immutable SnapshotHint and retain its identity.",
    "Close source snapshot before building the hinted snapshot."])
creation.note("left", [
    "3. Pack the full hint once into a confined JNR scope.",
    "The setter copies it into native builder-owned state."])
creation.arrow("snapshot_builder_set_snapshot_hint(builder, exactHint)")
creation.arrow("snapshot_builder_build(builder)")
creation.note("right", [
    "Build and validate the hinted native Snapshot.",
    "Record the exact Java hint identity on JnrSnapshot."])
creation.arrow("Return hinted snapshot handle", "return")
creation.note("left", [
    "4. externalize(hinted, exactHint, generation)",
    "Identity match proves this is the state used by the builder."])
creation.arrow("snapshot_externalize_validated_core(snapshot, generation)")
creation.note("right", [
    "Retain root, version, freshness and generation.",
    "Retain a compact validated metadata-scan token.",
    "No second hint pack or full component comparison."])
creation.arrow("Return SnapshotCore handle", "return")
creation.arrow("5. Close hinted snapshot and construction engine")
creation.note("both", [
    "Between calls: Java retains SnapshotHint + plan executor + core handle.",
    "Rust retains SnapshotCore only: 741,192 B for 1,107 snapshots.",
    "Construction engine, source snapshot and hinted snapshot are released."])
creation.note("both", [
    "Fallback: a different hint object takes the full comparison path.",
    "Generation, version and freshness are rechecked on later calls.",
    "An unchanged generation cannot protect against connector mutation."], True)
creation.save("final-snapshot-creation-sequence")


planning = Sequence(
    "Default metadata scan planning",
    "Final p100 candidate; operation-specific log-state borrow; Java left, Rust right")
planning.note("both", [
    "Entry: Java owns immutable SnapshotHint and plan executor.",
    "Rust owns only SnapshotCore and its validated metadata-scan token."])
planning.note("left", [
    "1. Create a read-only plan engine for this scan call.",
    "Open a confined JNR scope for temporary native buffers."])
planning.note("left", [
    "2. Pack FfiSnapshotLogState: version, freshness,",
    "log paths and checkpoint hint only.",
    "No table schema, metadata, protocol or CRC payload."])
planning.arrow(
    "3. snapshot_core_declarative_metadata_plan_from_log_state(",
    detail="core, logState, generation, planEngine)")
planning.note("right", [
    "Check generation, version and freshness.",
    "Borrow log state for this FFI call only.",
    "Use the handoff validation token."])
planning.note("right", [
    "4. Decode paths and checkpoint hint.",
    "Create a scoped LogSegment.",
    "No table schema, TableConfiguration or StateInfo."])
planning.arrow("5. Engine callbacks for checkpoint shape and I/O", "left")
planning.note("left", [
    "Java plan executor performs checkpoint I/O.",
    "Checkpoint shape work remains operation scoped."])
planning.arrow("Return checkpoint inputs to Rust")
planning.note("right", [
    "6. Build MetadataScanPlan.",
    "Serialize the Operation protobuf.",
    "Drop scoped log, checkpoint and plan inputs.",
    "Keep result bytes until Java finishes decoding."])
planning.arrow("Return KernelOwnedBytes (pointer + length)", "return")
planning.note("left", [
    "7. Decode protobuf directly from native memory.",
    "Free result bytes; close JNR scope and plan engine."])
planning.arrow("jnr_free_kernel_bytes(pointer, length)")
planning.note("right", [
    "Free serialized result buffer.",
    "SnapshotCore remains; no Rust Scan or engine is retained."])
planning.note("both", [
    "Per fleet scan: 1,107 plans, 5,535 downcalls and 54.5 MB of path UTF-8.",
    "Schema bytes copied during scan: zero. Cleanup returns Rust and JNR to baseline."])
planning.note("both", [
    "Fallback for a core without a validation token uses the full hint path.",
    "Explicit schema getters remain separate and return owned Rust values."], True)
planning.save("final-scan-planning-sequence")
