//! Reader for Iceberg's Mumbling bitmap format (version 1).
//!
//! A Mumbling bitmap is a Roaring-derived compressed bitmap for bounded-size use cases such as
//! deletion vectors. All integers are little-endian. The buffer has three concatenated sections:
//!
//! 1. **Header** (6 bytes): a `0x01` version byte, a 3-byte cardinality (total set bits), and a
//!    2-byte container count.
//! 2. **Descriptor array**: one descriptor byte per container, compressed with the patched
//!    frame-of-reference (PFOR) scheme (see [`decode_pfor`]). A container's key is implicit -- it
//!    is the index into this array. Bit 5 of a descriptor selects the container type (0 = sparse, 1
//!    = dense); for a sparse container the low 5 bits are the set count. The top two bits are
//!    reserved and this reader rejects them when set.
//! 3. **Containers**: concatenated in container-index order. A sparse container is a sorted list of
//!    set positions (one byte each); a dense container is a 32-byte, MSB-first bitset.
//!
//! A bitmap position splits into a 16-bit container index (`pos >> 8`) and an 8-bit in-container
//! value (`pos & 0xFF`), so positions span 24 bits.
//!
//! [`MumblingBitmap`] is the owned, read-only decoded form: it answers membership queries and
//! iterates set positions without materializing a `Vec<bool>`. [`read_bitmap`] is a convenience
//! wrapper that decodes straight to a selection vector of a caller-provided length. Encoding and
//! mutation are out of scope.

use bytes::{Buf, Bytes};
use itertools::Either;

use crate::{DeltaResult, Error};

/// Only format version this decoder supports.
const FORMAT_VERSION: u8 = 0x01;

/// Byte length of the fixed header (version + cardinality + container count).
const HEADER_LEN: usize = 6;

/// Positions covered by a single container (one 256-bit region of the bitmap).
const POSITIONS_PER_CONTAINER: usize = 256;

/// Number of values in a full PFOR chunk (the last chunk is the remainder).
const PFOR_CHUNK: usize = 256;

/// Byte length of a dense container's bitset (256 bits).
const DENSE_BYTES: usize = 32;

/// Descriptor bit that marks a container as dense rather than sparse.
const DENSE_TYPE_FLAG: u8 = 0x20;

/// Descriptor bits holding a sparse container's set count (0..=31).
const SPARSE_LEN_MASK: u8 = 0x1F;

/// Descriptor's top two bits, reserved for future use; a v1 reader rejects them when set.
const RESERVED_DESCRIPTOR_BITS: u8 = 0xC0;

/// An owned, decoded Mumbling bitmap (version 1).
///
/// Holds the containers as zero-copy views into the source buffer, so decoding does not copy
/// container payloads. Supports membership queries ([`contains`](Self::contains)) and iteration
/// over set positions ([`iter`](Self::iter)) without materializing a selection vector.
#[derive(Debug)]
pub(crate) struct MumblingBitmap {
    /// Total number of set bits (from the header, validated against the containers).
    cardinality: usize,
    /// One entry per container, indexed by container key; empty containers included.
    containers: Vec<Container>,
}

impl MumblingBitmap {
    /// Decode a Mumbling bitmap (version 1) from `bytes`.
    ///
    /// # Errors
    /// Returns an error if the buffer is shorter than the 6-byte header, the version byte is not
    /// `0x01`, a PFOR chunk or container body extends past the end of the buffer, the PFOR bit
    /// widths are inconsistent, an exception offset is out of range, or the number of set bits
    /// does not match the header's cardinality.
    pub(crate) fn deserialize(mut bytes: Bytes) -> DeltaResult<Self> {
        let mut header = take(&mut bytes, HEADER_LEN)?;
        let version = header.get_u8();
        if version != FORMAT_VERSION {
            return Err(Error::generic(format!(
                "Mumbling bitmap has unsupported version {version}, expected {FORMAT_VERSION}"
            )));
        }
        // Cardinality is a 3-byte little-endian integer; assemble it from its low 3 bytes.
        let cardinality = header.get_u8() as usize
            | (header.get_u8() as usize) << 8
            | (header.get_u8() as usize) << 16;
        let container_count = header.get_u16_le() as usize;

        let descriptors = decode_pfor(&mut bytes, container_count)?;
        let mut containers = Vec::with_capacity(container_count);
        let mut set = 0usize;
        for &descriptor in &descriptors {
            let container = Container::read(&mut bytes, descriptor)?;
            set += container.count();
            containers.push(container);
        }

        if set != cardinality {
            return Err(Error::generic(format!(
                "Mumbling bitmap set {set} bits but header cardinality is {cardinality}"
            )));
        }
        Ok(Self {
            cardinality,
            containers,
        })
    }

    /// Whether position `pos` is set. Positions beyond the bitmap's containers are unset.
    pub(crate) fn contains(&self, pos: u32) -> bool {
        let container = (pos >> 8) as usize;
        self.containers
            .get(container)
            .is_some_and(|c| c.contains(pos as u8))
    }

    /// The number of set bits.
    pub(crate) fn cardinality(&self) -> usize {
        self.cardinality
    }

    /// Whether the bitmap has no set bits.
    pub(crate) fn is_empty(&self) -> bool {
        self.cardinality == 0
    }

    /// The set positions, in ascending order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.containers
            .iter()
            .enumerate()
            .flat_map(|(container, c)| {
                let base = (container as u32) << 8; // container index is the high 16 bits
                c.set_values().map(move |v| base + v as u32)
            })
    }

    /// Materialize a selection vector of length `len` where element `p` is `true` if and only if
    /// position `p` is set.
    ///
    /// `len` is typically the row count of the data being filtered (the selection vector must not
    /// exceed it); it must be greater than the highest set position.
    ///
    /// # Errors
    /// Returns an error if any set position is `>= len`.
    pub(crate) fn to_selection_vec(&self, len: usize) -> DeltaResult<Vec<bool>> {
        let mut out = vec![false; len];
        for pos in self.iter() {
            let slot = out.get_mut(pos as usize).ok_or_else(|| {
                Error::generic(format!(
                    "Mumbling bitmap position {pos} exceeds selection vector length {len}"
                ))
            })?;
            *slot = true;
        }
        Ok(out)
    }
}

/// Decode a Mumbling bitmap (version 1) directly into a selection vector of length `len`.
///
/// Convenience wrapper over [`MumblingBitmap::deserialize`] and
/// [`MumblingBitmap::to_selection_vec`]; see those for the layout and errors.
pub(crate) fn read_bitmap(bytes: Bytes, len: usize) -> DeltaResult<Vec<bool>> {
    MumblingBitmap::deserialize(bytes)?.to_selection_vec(len)
}

// === Helpers ===

/// A single container, holding a zero-copy view of its body in the source buffer.
#[derive(Debug)]
enum Container {
    /// Sparse: up to 31 set in-container values (0..=255), sorted ascending, one byte each.
    Sparse(Bytes),
    /// Dense: a `DENSE_BYTES`-byte MSB-first bitset.
    Dense(Bytes),
}

impl Container {
    /// Read the container described by `descriptor` from `bytes`.
    fn read(bytes: &mut Bytes, descriptor: u8) -> DeltaResult<Self> {
        if descriptor & RESERVED_DESCRIPTOR_BITS != 0 {
            return Err(Error::generic(format!(
                "Mumbling container descriptor {descriptor:#04x} sets reserved bits"
            )));
        }
        if descriptor & DENSE_TYPE_FLAG != 0 {
            return Ok(Container::Dense(take(bytes, DENSE_BYTES)?));
        }
        let length = (descriptor & SPARSE_LEN_MASK) as usize;
        let values = take(bytes, length)?;
        // Sparse values must be strictly ascending. This guarantees uniqueness -- so `count` cannot
        // over-count and let a malformed body slip past the cardinality check -- and upholds the
        // ascending-order contract of `MumblingBitmap::iter`.
        if values.windows(2).any(|w| w[0] >= w[1]) {
            return Err(Error::generic(
                "Mumbling sparse container values are not strictly ascending",
            ));
        }
        Ok(Container::Sparse(values))
    }

    /// The number of set bits in this container.
    fn count(&self) -> usize {
        match self {
            Container::Sparse(values) => values.len(),
            Container::Dense(block) => block.iter().map(|b| b.count_ones() as usize).sum(),
        }
    }

    /// Whether in-container `value` is set.
    fn contains(&self, value: u8) -> bool {
        match self {
            Container::Sparse(values) => values.contains(&value),
            Container::Dense(block) => dense_bit_set(block, value),
        }
    }

    /// The set in-container values, ascending.
    fn set_values(&self) -> impl Iterator<Item = u8> + '_ {
        match self {
            Container::Sparse(values) => Either::Left(values.iter().copied()),
            Container::Dense(block) => {
                Either::Right((0..=u8::MAX).filter(move |&v| dense_bit_set(block, v)))
            }
        }
    }
}

/// Whether `value` is set in a dense container's MSB-first bitset: position 0 is the most
/// significant bit of byte 0, so `value` is bit `7 - (value & 7)` of byte `value / 8`.
fn dense_bit_set(block: &[u8], value: u8) -> bool {
    block[value as usize / 8] & (1 << (7 - (value % 8))) != 0
}

/// Decode `count` PFOR-encoded byte values (the descriptor array) from `bytes`.
///
/// Values are grouped into 256-value chunks; the final chunk holds the remainder. Each chunk
/// stores its own frame-of-reference minimum, primary bit width, and patched exceptions. See the
/// module docs and the Mumbling spec Appendix A for the wire layout.
fn decode_pfor(bytes: &mut Bytes, count: usize) -> DeltaResult<Vec<u8>> {
    let mut values = Vec::with_capacity(count);
    while values.len() < count {
        let chunk_len = (count - values.len()).min(PFOR_CHUNK);

        let mut header = take(bytes, 3)?;
        let widths = header.get_u8();
        let b1 = (widths & 0x0F) as u32; // primary bit width, low nibble
        let b2 = (widths >> 4) as u32; // exception bit width, high nibble
        let exception_count = header.get_u8() as usize;
        let min = header.get_u8();
        if b1 + b2 > 8 {
            return Err(Error::generic(format!(
                "Mumbling PFOR chunk has invalid bit widths b1={b1} b2={b2} (sum exceeds 8)"
            )));
        }

        let primary = take(bytes, bytes_for_bits(chunk_len, b1))?;
        let mut chunk = unpack_msb(primary.as_ref(), chunk_len, b1);

        let offsets = take(bytes, exception_count)?;
        let exceptions = take(bytes, bytes_for_bits(exception_count, b2))?;
        let exception_bits = unpack_msb(exceptions.as_ref(), exception_count, b2);
        let mut patched = [false; PFOR_CHUNK];
        for (&offset, &high) in offsets.as_ref().iter().zip(&exception_bits) {
            let offset = offset as usize;
            if offset >= chunk_len {
                return Err(Error::generic(format!(
                    "Mumbling PFOR exception offset {offset} out of range for chunk length {chunk_len}"
                )));
            }
            if patched[offset] {
                return Err(Error::generic(format!(
                    "Mumbling PFOR chunk has a duplicate exception offset {offset}"
                )));
            }
            patched[offset] = true;
            chunk[offset] |= high << b1; // exception supplies the high bits above b1
        }

        for value in chunk {
            values.push((value as u8).wrapping_add(min));
        }
    }
    Ok(values)
}

/// Unpack `count` values of `width` bits each from `src`, most significant bit first.
///
/// A `width` of 0 yields `count` zeros. Callers must ensure `src` holds at least
/// `bytes_for_bits(count, width)` bytes.
fn unpack_msb(src: &[u8], count: usize, width: u32) -> Vec<u16> {
    if width == 0 {
        return vec![0; count];
    }
    let mut values = Vec::with_capacity(count);
    let mut bit = 0usize;
    for _ in 0..count {
        let mut value = 0u16;
        for _ in 0..width {
            let byte = src[bit / 8];
            let set = (byte >> (7 - (bit % 8))) & 1;
            value = (value << 1) | set as u16;
            bit += 1;
        }
        values.push(value);
    }
    values
}

/// Bytes needed to hold `count` values of `width` bits each, rounded up.
fn bytes_for_bits(count: usize, width: u32) -> usize {
    (count * width as usize).div_ceil(8)
}

/// Split the first `n` bytes off `bytes` (zero-copy), erroring if fewer remain.
fn take(bytes: &mut Bytes, n: usize) -> DeltaResult<Bytes> {
    if bytes.remaining() < n {
        return Err(Error::generic(format!(
            "Mumbling bitmap is truncated: need {n} bytes but only {} remain",
            bytes.remaining()
        )));
    }
    Ok(bytes.split_to(n))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A container body for building test buffers.
    enum Body {
        /// Sparse container: the sorted set values (the descriptor stores the count).
        Sparse(Vec<u8>),
        /// Dense container: the raw 32-byte MSB-first bitset (descriptor `0x20`).
        Dense([u8; DENSE_BYTES]),
    }

    /// Encode `values` as a PFOR byte array using the trivial `b1 = 8` scheme (one raw byte per
    /// value, no exceptions), which the spec permits. Lets full-bitmap tests build descriptor
    /// arrays without a real PFOR encoder.
    fn encode_pfor_raw(values: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        for chunk in values.chunks(PFOR_CHUNK) {
            out.extend_from_slice(&[0x08, 0x00, 0x00]); // b1=8, b2=0, e=0, m=0
            out.extend_from_slice(chunk);
        }
        out
    }

    /// Build a full Mumbling bitmap buffer from `(container_index, body)` entries. Unlisted
    /// containers get an empty sparse descriptor. `cardinality` overrides the computed value (for
    /// the mismatch test).
    fn encode_bitmap(
        container_count: usize,
        entries: &[(usize, Body)],
        cardinality: Option<u32>,
    ) -> Bytes {
        let mut descriptors = vec![0u8; container_count];
        let mut bodies: Vec<Vec<u8>> = vec![Vec::new(); container_count];
        let mut set_bits = 0u32;
        for (idx, body) in entries {
            match body {
                Body::Sparse(values) => {
                    descriptors[*idx] = values.len() as u8; // type bit 0, low bits = count
                    bodies[*idx] = values.clone();
                    set_bits += values.len() as u32;
                }
                Body::Dense(block) => {
                    descriptors[*idx] = DENSE_TYPE_FLAG;
                    bodies[*idx] = block.to_vec();
                    set_bits += block.iter().map(|b| b.count_ones()).sum::<u32>();
                }
            }
        }

        let cardinality = cardinality.unwrap_or(set_bits);
        let mut buf = vec![FORMAT_VERSION];
        buf.extend_from_slice(&cardinality.to_le_bytes()[..3]);
        buf.extend_from_slice(&(container_count as u16).to_le_bytes());
        buf.extend_from_slice(&encode_pfor_raw(&descriptors));
        for body in bodies {
            buf.extend_from_slice(&body);
        }
        Bytes::from(buf)
    }

    /// Build a dense 32-byte bitset from a set of in-container values (MSB-first).
    fn dense_bytes(values: &[u8]) -> [u8; DENSE_BYTES] {
        let mut block = [0u8; DENSE_BYTES];
        for &v in values {
            block[v as usize / 8] |= 1 << (7 - (v % 8));
        }
        block
    }

    /// The set positions of a decoded bitmap, ascending.
    fn set_positions(bitmap: &[bool]) -> Vec<usize> {
        bitmap
            .iter()
            .enumerate()
            .filter_map(|(i, &b)| b.then_some(i))
            .collect()
    }

    // === PFOR decoder: the spec's Appendix A worked examples ===

    #[rstest::rstest]
    #[case::all_zero(&[0x00, 0x00, 0x00], 256, vec![0; 256])]
    #[case::all_five(&[0x00, 0x00, 0x05], 51, vec![5; 51])]
    #[case::two_exceptions(
        &[0x80, 0x02, 0x00, 0x04, 0x07, 0xFF, 0xFE], 8, vec![0, 0, 0, 0, 0xFF, 0, 0, 0xFE]
    )]
    #[case::two_bit(&[0x02, 0x00, 0x06, 0x18], 3, vec![6, 7, 8])]
    #[case::one_exception(&[0x32, 0x01, 0x06, 0x09, 0x01, 0xE0], 4, vec![6, 34, 8, 7])]
    fn decode_pfor_matches_spec_examples(
        #[case] encoded: &[u8],
        #[case] count: usize,
        #[case] expected: Vec<u8>,
    ) {
        let decoded = decode_pfor(&mut Bytes::copy_from_slice(encoded), count).unwrap();
        assert_eq!(decoded, expected);
    }

    #[rstest::rstest]
    #[case::width_two(&[0xE6, 0xC0], 5, 2, vec![3, 2, 1, 2, 3])]
    #[case::width_zero(&[], 4, 0, vec![0, 0, 0, 0])]
    fn unpack_msb_reads_bits_most_significant_first(
        #[case] src: &[u8],
        #[case] count: usize,
        #[case] width: u32,
        #[case] expected: Vec<u16>,
    ) {
        assert_eq!(unpack_msb(src, count, width), expected);
    }

    #[test]
    fn decode_pfor_spans_multiple_chunks() {
        let values: Vec<u8> = (0..300).map(|i| (i % 256) as u8).collect();
        let encoded = encode_pfor_raw(&values);
        let decoded = decode_pfor(&mut Bytes::from(encoded), values.len()).unwrap();
        assert_eq!(decoded, values);
    }

    #[rstest::rstest]
    #[case::truncated_header(&[0x08, 0x00], 1)]
    #[case::truncated_primary(&[0x08, 0x00, 0x00], 4)] // b1=8 needs 4 body bytes, none follow
    #[case::truncated_exceptions(&[0x00, 0x01, 0x00], 4)] // 1 exception offset promised, none follow
    #[case::invalid_bit_widths(&[0x88, 0x00, 0x00], 4)] // b1=8, b2=8 -> sum > 8
    // b1=0, b2=8, e=2, m=0, offsets [1, 1] (duplicate), values [0, 0]
    #[case::duplicate_exception_offset(&[0x80, 0x02, 0x00, 0x01, 0x01, 0x00, 0x00], 3)]
    fn decode_pfor_rejects_malformed_chunks(#[case] encoded: &[u8], #[case] count: usize) {
        assert!(decode_pfor(&mut Bytes::copy_from_slice(encoded), count).is_err());
    }

    #[test]
    fn decode_pfor_rejects_out_of_range_exception_offset() {
        // b1=0, b2=8, e=1, m=0, offset=200, value=0xFF -- offset exceeds the 3-value chunk.
        let encoded = [0x80, 0x01, 0x00, 200, 0xFF];
        assert!(decode_pfor(&mut Bytes::copy_from_slice(&encoded), 3).is_err());
    }

    // === Full bitmap decode ===

    #[test]
    fn empty_bitmap_has_no_containers() {
        let bitmap = read_bitmap(encode_bitmap(0, &[], None), 0).unwrap();
        assert!(bitmap.is_empty());
    }

    #[test]
    fn sparse_container_sets_expected_positions() {
        let bitmap = read_bitmap(
            encode_bitmap(1, &[(0, Body::Sparse(vec![3, 5, 255]))], None),
            256,
        )
        .unwrap();
        assert_eq!(bitmap.len(), 256);
        assert_eq!(set_positions(&bitmap), vec![3, 5, 255]);
    }

    #[test]
    fn high_container_index_maps_to_24_bit_position() {
        // Container 0x123 (291), value 0x45 -> position 0x12345. Exercises the u16 container index
        // (a u8 container, as in the old gist-based reader, could not address this).
        let bitmap = read_bitmap(
            encode_bitmap(292, &[(0x123, Body::Sparse(vec![0x45]))], None),
            292 * 256,
        )
        .unwrap();
        assert_eq!(bitmap.len(), 292 * 256);
        assert_eq!(set_positions(&bitmap), vec![0x12345]);
    }

    #[test]
    fn dense_container_respects_msb_first_bit_order() {
        let values = [0u8, 7, 8, 255];
        let bitmap = read_bitmap(
            encode_bitmap(1, &[(0, Body::Dense(dense_bytes(&values)))], None),
            256,
        )
        .unwrap();
        let expected: Vec<usize> = values.iter().map(|&v| v as usize).collect();
        assert_eq!(set_positions(&bitmap), expected);
    }

    #[test]
    fn dense_byte0_msb_is_position_zero() {
        let mut block = [0u8; DENSE_BYTES];
        block[0] = 0x80; // only the most significant bit of byte 0
        let bitmap = read_bitmap(encode_bitmap(1, &[(0, Body::Dense(block))], None), 256).unwrap();
        assert!(bitmap[0]);
        assert!(!bitmap[1]);
    }

    #[test]
    fn full_dense_container_sets_all_256_positions() {
        let bitmap = read_bitmap(
            encode_bitmap(1, &[(0, Body::Dense([0xFF; DENSE_BYTES]))], None),
            256,
        )
        .unwrap();
        assert_eq!(set_positions(&bitmap), (0..256).collect::<Vec<_>>());
    }

    #[test]
    fn sparse_container_at_max_length() {
        let values: Vec<u8> = (0..31).collect(); // 31 values, the largest sparse container
        let bitmap = read_bitmap(
            encode_bitmap(1, &[(0, Body::Sparse(values.clone()))], None),
            256,
        )
        .unwrap();
        let expected: Vec<usize> = values.iter().map(|&v| v as usize).collect();
        assert_eq!(set_positions(&bitmap), expected);
    }

    #[test]
    fn mixed_sparse_dense_and_empty_containers() {
        let entries = vec![
            (0, Body::Sparse(vec![1, 2])),
            // container 1 empty
            (2, Body::Dense(dense_bytes(&[0, 100, 255]))),
        ];
        let bitmap = read_bitmap(encode_bitmap(3, &entries, None), 3 * 256).unwrap();
        let mut expected = vec![1usize, 2];
        expected.extend([0usize, 100, 255].iter().map(|&v| 512 + v));
        expected.sort_unstable();
        assert_eq!(set_positions(&bitmap), expected);
    }

    #[test]
    fn round_trip_across_many_containers() {
        use std::collections::BTreeMap;

        let positions = [0usize, 255, 256, 0x12345, 40000, 0x1FFFF];
        let container_count = (positions.iter().max().unwrap() / 256) + 1;
        let mut by_container: BTreeMap<usize, Vec<u8>> = BTreeMap::new();
        for &p in &positions {
            by_container
                .entry(p >> 8)
                .or_default()
                .push((p & 0xFF) as u8);
        }
        let entries: Vec<(usize, Body)> = by_container
            .into_iter()
            .map(|(container, mut values)| {
                values.sort_unstable();
                (container, Body::Sparse(values))
            })
            .collect();

        let bitmap = read_bitmap(
            encode_bitmap(container_count, &entries, None),
            container_count * 256,
        )
        .unwrap();
        let mut expected = positions.to_vec();
        expected.sort_unstable();
        assert_eq!(set_positions(&bitmap), expected);
    }

    #[test]
    fn decodes_compressed_descriptor_chunk() {
        // Hand-build a compressed PFOR descriptor chunk for 3 containers where container 1 is dense
        // (0x20) and the others are empty sparse (0x00): b1=0 (all normalized values 0), one
        // exception at offset 1 supplying 0x20 via 6 exception bits (b2=6), m=0.
        let mut buf = vec![FORMAT_VERSION];
        buf.extend_from_slice(&8u32.to_le_bytes()[..3]); // cardinality: 8 bits set in the container
        buf.extend_from_slice(&3u16.to_le_bytes()); // container count
        buf.extend_from_slice(&[0x60, 0x01, 0x00]); // PFOR header: b1=0, b2=6, e=1, m=0
        buf.push(0x01); // exception offset: container 1
        buf.push(0x20 << 2); // 6-bit value 0x20 packed MSB-first into one byte -> 0x80
        buf.extend_from_slice(&dense_bytes(&[1, 2, 3, 4, 5, 6, 7, 8])); // container 1's body

        let bitmap = read_bitmap(Bytes::from(buf), 3 * 256).unwrap();
        assert_eq!(bitmap.len(), 3 * 256);
        assert_eq!(
            set_positions(&bitmap),
            vec![257, 258, 259, 260, 261, 262, 263, 264]
        );
    }

    #[test]
    fn position_beyond_selection_vec_len_errors() {
        // Position 0x12345 with a selection vector too short to hold it.
        let bytes = encode_bitmap(292, &[(0x123, Body::Sparse(vec![0x45]))], None);
        let err = read_bitmap(bytes, 1000).unwrap_err();
        assert!(
            err.to_string().contains("exceeds selection vector length"),
            "{err:?}"
        );
    }

    #[test]
    fn cardinality_mismatch_errors() {
        let entries = [(0, Body::Sparse(vec![1, 2, 3]))];
        // Correct cardinality (3) decodes.
        assert!(read_bitmap(encode_bitmap(1, &entries, Some(3)), 256).is_ok());
        // Wrong cardinality is rejected.
        let err = read_bitmap(encode_bitmap(1, &entries, Some(99)), 256).unwrap_err();
        assert!(err.to_string().contains("cardinality"), "{err:?}");
    }

    #[rstest::rstest]
    #[case::short_header(Bytes::from(vec![0x01, 0x00, 0x00]), "truncated")]
    #[case::bad_version(encode_bad_version(), "version")]
    #[case::truncated_container(encode_truncated_container(), "truncated")]
    #[case::reserved_descriptor_bits(encode_reserved_descriptor(), "reserved")]
    #[case::sparse_not_ascending(
        encode_bitmap(1, &[(0, Body::Sparse(vec![5, 3]))], None), "ascending"
    )]
    #[case::sparse_duplicate_values(
        encode_bitmap(1, &[(0, Body::Sparse(vec![5, 5]))], None), "ascending"
    )]
    fn rejects_malformed_bitmap(#[case] bytes: Bytes, #[case] needle: &str) {
        let err = read_bitmap(bytes, 256).unwrap_err();
        assert!(err.to_string().contains(needle), "{err:?}");
    }

    /// A bitmap with a single container whose descriptor sets a reserved (high) bit.
    fn encode_reserved_descriptor() -> Bytes {
        let mut buf = vec![FORMAT_VERSION];
        buf.extend_from_slice(&0u32.to_le_bytes()[..3]); // cardinality
        buf.extend_from_slice(&1u16.to_le_bytes()); // one container
        buf.extend_from_slice(&encode_pfor_raw(&[0x40])); // reserved bit 6 set (sparse, length 0)
        Bytes::from(buf) // length-0 sparse body: no bytes follow
    }

    /// A header whose version byte is not `0x01`.
    fn encode_bad_version() -> Bytes {
        let mut bytes = encode_bitmap(0, &[], None).to_vec();
        bytes[0] = 0x02;
        Bytes::from(bytes)
    }

    /// A bitmap whose descriptor promises a 3-value sparse container but has no container body.
    fn encode_truncated_container() -> Bytes {
        let mut buf = vec![FORMAT_VERSION];
        buf.extend_from_slice(&3u32.to_le_bytes()[..3]); // cardinality
        buf.extend_from_slice(&1u16.to_le_bytes()); // one container
        buf.extend_from_slice(&encode_pfor_raw(&[3])); // descriptor: sparse of 3
        Bytes::from(buf) // no container body follows
    }

    // === MumblingBitmap owned type ===

    #[test]
    fn contains_queries_sparse_dense_and_high_containers() {
        let entries = vec![
            (0, Body::Sparse(vec![3, 5, 255])),
            (2, Body::Dense(dense_bytes(&[0, 100, 255]))),
            (0x123, Body::Sparse(vec![0x45])), // -> position 0x12345
        ];
        let bitmap = MumblingBitmap::deserialize(encode_bitmap(292, &entries, None)).unwrap();

        // Present positions across a sparse container, a dense container, and a high container.
        for pos in [3u32, 5, 255, 512, 612, 767, 0x12345] {
            assert!(bitmap.contains(pos), "expected {pos:#x} to be set");
        }
        // Absent positions, including one in an unset in-range container.
        for pos in [0u32, 4, 256, 511, 613, 0x12344, 0x12346] {
            assert!(!bitmap.contains(pos), "expected {pos:#x} to be unset");
        }
        // A position in a container index beyond the bitmap is unset, not a panic.
        assert!(!bitmap.contains(0xFFFFF));
    }

    #[test]
    fn cardinality_and_is_empty() {
        let populated = MumblingBitmap::deserialize(encode_bitmap(
            1,
            &[(0, Body::Sparse(vec![1, 2, 3]))],
            None,
        ))
        .unwrap();
        assert_eq!(populated.cardinality(), 3);
        assert!(!populated.is_empty());

        let empty = MumblingBitmap::deserialize(encode_bitmap(0, &[], None)).unwrap();
        assert_eq!(empty.cardinality(), 0);
        assert!(empty.is_empty());
    }

    #[test]
    fn iter_yields_ascending_positions_matching_selection_vec() {
        let entries = vec![
            (0, Body::Sparse(vec![3, 5, 255])),
            (2, Body::Dense(dense_bytes(&[0, 100, 255]))),
            (0x123, Body::Sparse(vec![0x45])),
        ];
        let bitmap = MumblingBitmap::deserialize(encode_bitmap(292, &entries, None)).unwrap();

        let from_iter: Vec<usize> = bitmap.iter().map(|p| p as usize).collect();
        assert!(
            from_iter.windows(2).all(|w| w[0] < w[1]),
            "iter must be ascending"
        );
        assert_eq!(from_iter.len(), bitmap.cardinality());
        assert_eq!(
            from_iter,
            set_positions(&bitmap.to_selection_vec(292 * 256).unwrap())
        );
    }

    #[test]
    fn to_selection_vec_matches_read_bitmap() {
        let bytes = encode_bitmap(
            3,
            &[
                (0, Body::Sparse(vec![1, 2])),
                (2, Body::Dense(dense_bytes(&[0, 100, 255]))),
            ],
            None,
        );
        let via_type = MumblingBitmap::deserialize(bytes.clone())
            .unwrap()
            .to_selection_vec(3 * 256)
            .unwrap();
        assert_eq!(via_type, read_bitmap(bytes, 3 * 256).unwrap());
    }

    #[test]
    fn deserialize_surfaces_errors() {
        let err = MumblingBitmap::deserialize(encode_bad_version()).unwrap_err();
        assert!(err.to_string().contains("version"), "{err:?}");

        let err = MumblingBitmap::deserialize(encode_bitmap(
            1,
            &[(0, Body::Sparse(vec![1, 2, 3]))],
            Some(99),
        ))
        .unwrap_err();
        assert!(err.to_string().contains("cardinality"), "{err:?}");
    }
}
