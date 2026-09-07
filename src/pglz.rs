//! pglz decompression. Used for both compressed full-page images and for
//! TOAST values stored with the default `pglz` compression.
//!
//! Format: a control byte whose bits (LSB first) select literal vs match for
//! the next 8 items. A match is two bytes -- low nibble of the first is
//! length-3, high nibble plus the second byte is the back-offset -- with a
//! third byte extending the length when it reaches 18.

use anyhow::{bail, Result};

pub fn decompress(src: &[u8], expected_len: usize) -> Result<Vec<u8>> {
    let mut out: Vec<u8> = Vec::with_capacity(expected_len);
    let mut sp = 0usize;

    while sp < src.len() && out.len() < expected_len {
        let ctrl = src[sp];
        sp += 1;
        for bit in 0..8 {
            if sp >= src.len() || out.len() >= expected_len {
                break;
            }
            if ctrl & (1 << bit) != 0 {
                if sp + 1 >= src.len() {
                    bail!("pglz: truncated match tag at byte {sp}");
                }
                let mut len = (src[sp] & 0x0f) as usize + 3;
                let off = (((src[sp] & 0xf0) as usize) << 4) | src[sp + 1] as usize;
                sp += 2;
                if len == 18 {
                    if sp >= src.len() {
                        bail!("pglz: truncated length extension at byte {sp}");
                    }
                    len += src[sp] as usize;
                    sp += 1;
                }
                if off == 0 || off > out.len() {
                    bail!("pglz: back-reference {off} beyond {} bytes produced", out.len());
                }
                // Must copy byte at a time: matches routinely overlap the
                // output cursor to encode runs.
                for _ in 0..len {
                    if out.len() >= expected_len {
                        break;
                    }
                    let b = out[out.len() - off];
                    out.push(b);
                }
            } else {
                out.push(src[sp]);
                sp += 1;
            }
        }
    }

    if out.len() != expected_len {
        bail!("pglz: produced {} bytes, expected {expected_len}", out.len());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals_only() {
        // ctrl=0 -> eight literal bytes, then ctrl=0 -> two more.
        let mut src = vec![0u8];
        src.extend_from_slice(b"abcdefgh");
        src.push(0u8);
        src.extend_from_slice(b"ij");
        assert_eq!(decompress(&src, 10).unwrap(), b"abcdefghij");
    }

    #[test]
    fn overlapping_match_encodes_a_run() {
        // Literal 'a', then a match of length 4 at offset 1 -> "aaaaa".
        // ctrl bit0=0 (literal), bit1=1 (match).
        let src = vec![0b0000_0010, b'a', 0x01, 0x01];
        assert_eq!(decompress(&src, 5).unwrap(), b"aaaaa");
    }

    #[test]
    fn rejects_backref_past_start() {
        let src = vec![0b0000_0001, 0x01, 0x05];
        assert!(decompress(&src, 8).is_err());
    }
}
