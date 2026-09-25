from pathlib import Path
from html import escape
import subprocess
ROOT=Path(__file__).resolve().parent
class Sequence:
 def __init__(self,title,subtitle):
  self.parts=[];self.y=150;self.title=title;self.subtitle=subtitle
 def text(self,x,y,lines,size=18,anchor='middle'):
  for i,line in enumerate(lines):self.parts.append(f'<text x="{x}" y="{y+i*24}" text-anchor="{anchor}" font-family="DejaVu Sans" font-size="{size}" fill="#17324d">{escape(line)}</text>')
 def arrow(self,label,direction='right',detail=None):
  lines=[label]+([detail] if detail else []);self.y+=24*len(lines)
  self.text(600,self.y-24*(len(lines)-1)-10,lines,17)
  x1,x2=(175,1025) if direction=='right' else (1025,175)
  dash=' stroke-dasharray="7 5"' if direction=='return' else ''
  self.parts.append(f'<path d="M{x1},{self.y+6} H{x2}" fill="none" stroke="#334e68" stroke-width="2" marker-end="url(#arrow)"{dash}/>')
  self.y+=48
 def note(self,side,lines,warn=False):
  x,w=(35,505) if side=='left' else (660,505) if side=='right' else (80,1040)
  h=24*len(lines)+26;fill='#fff3d8' if warn else '#eaf2fb' if side=='left' else '#e5f3ec'
  self.parts.append(f'<rect x="{x}" y="{self.y}" width="{w}" height="{h}" rx="8" fill="{fill}" stroke="#90a5b5"/>')
  self.text(x+w/2,self.y+27,lines,17);self.y+=h+25
 def save(self,name):
  height=self.y+25
  head=f'<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="{height}" viewBox="0 0 1200 {height}"><defs><marker id="arrow" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto"><path d="M0,0 L10,4 L0,8" fill="#334e68"/></marker></defs><rect width="1200" height="{height}" fill="white"/>'
  title=f'<text x="600" y="35" text-anchor="middle" font-family="DejaVu Sans" font-size="26" fill="#17324d">{escape(self.title)}</text><text x="600" y="66" text-anchor="middle" font-family="DejaVu Sans" font-size="16" fill="#52606d">{escape(self.subtitle)}</text>'
  lifelines=''
  for x,label,color in [(175,'Java connector / engine','#eaf2fb'),(1025,'Rust kernel','#e5f3ec')]:
   lifelines+=f'<path d="M{x},126 V{height-20}" stroke="#a7b7c5" stroke-dasharray="6 6"/><rect x="{x-160}" y="85" width="320" height="44" rx="7" fill="{color}" stroke="#718a9f"/><text x="{x}" y="113" text-anchor="middle" font-family="DejaVu Sans" font-size="19">{label}</text>'
  p=ROOT/(name+'.svg');p.write_text(head+title+lifelines+''.join(self.parts)+'</svg>')
  subprocess.run(['ffmpeg','-hide_banner','-loglevel','error','-i',str(p),'-frames:v','1','-y',str(ROOT/(name+'.png'))],check=True)

s=Sequence('Snapshot creation and validated handoff','Current p100 candidate: actual harness lifecycle; calls are grouped for readability')
s.arrow('1. TableManager.loadSnapshot(table)')
s.note('right',['Load log state through engine callbacks.','Build source Snapshot + TableConfiguration;','parse and validate logical/physical schemas.'])
s.arrow('Return source snapshot handle','return')
s.arrow('2. Read version, metadata and protocol from source')
s.arrow('Return components to Java','return')
s.note('left',['Harness lists fixture log paths in Java.','Construct immutable SnapshotHint.','Source native snapshot remains alive.'])
s.note('left',['3. Pack full hint into scoped JNR buffers.','Encode strings as UTF-8 and copy to native memory.'])
s.arrow('snapshot_builder_set_snapshot_hint(builder, hint)')
s.note('right',['Copy hint into native builder-owned state.','JNR input buffers can be released after setter.'])
s.arrow('snapshot_builder_build(builder)')
s.note('right',['Build and validate the hinted Snapshot.','Source and hinted native snapshots overlap.'])
s.arrow('Return hinted snapshot handle','return')
s.note('left',['4. Pack full hint again for handoff.','Pass caller generation (version in this harness).'])
s.arrow('snapshot_externalize_core(hinted, hint, generation, engine)')
s.note('right',['Compare full connector state with hinted snapshot.','Check nonempty schema and scan feature support.','Validate explicit metadata-column scan rules.','Create small core + optional scan-validation token.'])
s.arrow('Return core handle; handoff input buffers can be freed','return')
s.arrow('5. externalize() closes hinted snapshot: free_snapshot')
s.arrow('6. Harness exits source scope: free_snapshot')
s.note('both',['Between calls: Java retains immutable hint, generation, engine and core handle.','Rust retains core identity + validation token and engine/context allocations.','Full native snapshots are released; initial load/handoff still materializes them.'])
s.note('both',['Failure: a handoff mismatch returns an error without consuming the snapshot.','A scan-invalid snapshot may still externalize for getters; its core has no scan token.','The generation must remain immutable; unchanged-ID mutations are not detected.'],True)
s.save('narrow-snapshot-creation-sequence')

s=Sequence('Default metadata scan planning','Current narrow candidate: one scoped hint borrow; Java left, Rust right')
s.note('both',['Entry: Java owns immutable SnapshotHint + generation; Rust owns the small core.','This path supports JSON stats, string partition values and no predicate.'])
s.note('left',['1. scanPlan() opens a confined JNR scope.','Convert paths to kernel URLs; encode/copy strings.','Pack full hint, including unused schema JSON.'])
s.arrow('2. snapshot_core_declarative_metadata_plan(core, hint, generation, engine)')
s.note('right',['Check core generation, version and freshness.','Borrow SnapshotState for this FFI call only.','Use the handoff validation token; check identity.'])
s.note('right',['3. Decode log paths and checkpoint hint.','Create scoped LogSegment.','No metadata/protocol/table-schema getter;','no TableConfiguration or StateInfo construction.'])
s.arrow('4. Engine callback: discover checkpoint shape, when needed','left')
s.note('left',['Java plan executor handles checkpoint I/O.','Read the checkpoint schema required for shape.','This is distinct from materializing the table schema.'])
s.arrow('Return checkpoint-shape inputs to Rust')
s.note('right',['5. Build MetadataScanPlan from narrow inputs.','Construct plan IR and serialize Operation protobuf.','Drop scoped log / shape / plan inputs;','keep the owned result buffer until explicitly freed.'])
s.arrow('Return optional KernelOwnedBytes (pointer + length)','return')
s.note('left',['6. Wrap bytes with a direct ByteBuffer.','Parse protobuf into the Java Plan.','Decode while the native result remains valid.'])
s.arrow('7. finally: jnr_free_kernel_bytes(pointer, length)')
s.note('right',['Free serialized result buffer.','Core and validation token remain alive.'])
s.note('left',['8. Close JNR scope; free hint input buffers.','Return Java Plan; retain immutable hint.','Benchmark calls getSerializedSize() inside timer.'])
s.note('both',['Failure paths: identity mismatch returns an error before planning.','If the core has no scan-validation token, use the existing fallible planner;','that fallback can materialize schemas and preserves scan-validation errors.'],True)
s.note('both',['No persistent Rust Scan is created by this valid default path.','Explicit schema getters are separate operations and still return an owned schema.'])
s.save('narrow-scan-planning-sequence')
