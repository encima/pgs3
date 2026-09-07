//! jsonb -> text.
//!
//! `jsonb_send` does not emit the binary tree: it emits a version byte followed
//! by the *text* rendering, so producing binary output for a jsonb column means
//! implementing `jsonb_out`. That in turn means `numeric_out`, because jsonb
//! stores its numbers as numerics.
//!
//! On-disk layout: a 4-byte container header, then one JEntry per element (two
//! per pair for objects, keys first), then the values. A JEntry normally holds
//! the element's *length*, but every 32nd holds an *offset* instead, so an
//! element's position is found by summing back to the nearest offset entry.
//! Numerics and nested containers are 4-byte aligned, and their JEntry length
//! includes the padding that precedes them.

use crate::numeric;
use anyhow::{bail, Result};

const JB_CMASK: u32 = 0x0FFF_FFFF;
const JB_FSCALAR: u32 = 0x1000_0000;
const JB_FOBJECT: u32 = 0x2000_0000;

const JENTRY_OFFLENMASK: u32 = 0x0FFF_FFFF;
const JENTRY_TYPEMASK: u32 = 0x7000_0000;
const JENTRY_HAS_OFF: u32 = 0x8000_0000;

const JENTRY_ISSTRING: u32 = 0x0000_0000;
const JENTRY_ISNUMERIC: u32 = 0x1000_0000;
const JENTRY_ISBOOL_FALSE: u32 = 0x2000_0000;
const JENTRY_ISBOOL_TRUE: u32 = 0x3000_0000;
const JENTRY_ISNULL: u32 = 0x4000_0000;
const JENTRY_ISCONTAINER: u32 = 0x5000_0000;

fn align4(n: usize) -> usize {
    (n + 3) & !3
}

fn u32_at(data: &[u8], at: usize) -> Result<u32> {
    if at + 4 > data.len() {
        bail!("jsonb truncated: wanted 4 bytes at {at}, value is {} bytes", data.len());
    }
    Ok(u32::from_le_bytes(data[at..at + 4].try_into().unwrap()))
}

pub fn to_text(data: &[u8]) -> Result<String> {
    let mut out = String::new();
    render_container(data, 0, &mut out)?;
    Ok(out)
}

fn render_container(data: &[u8], co: usize, out: &mut String) -> Result<()> {
    let header = u32_at(data, co)?;
    let count = (header & JB_CMASK) as usize;
    let is_object = header & JB_FOBJECT != 0;
    let is_scalar = header & JB_FSCALAR != 0;
    let nentries = if is_object { count * 2 } else { count };

    let children = co + 4;
    let values = children + 4 * nentries;
    let child = |i: usize| u32_at(data, children + 4 * i);

    // An element's offset is the sum of the lengths behind it, back to the
    // nearest entry that stores an offset outright.
    let offset_of = |i: usize| -> Result<usize> {
        let mut off = 0u32;
        for j in (0..i).rev() {
            let c = child(j)?;
            off += c & JENTRY_OFFLENMASK;
            if c & JENTRY_HAS_OFF != 0 {
                break;
            }
        }
        Ok(off as usize)
    };
    let len_of = |i: usize| -> Result<usize> {
        let c = child(i)?;
        let raw = (c & JENTRY_OFFLENMASK) as usize;
        Ok(if i == 0 {
            raw
        } else if c & JENTRY_HAS_OFF != 0 {
            raw.saturating_sub(offset_of(i)?)
        } else {
            raw
        })
    };

    let render_entry = |i: usize, out: &mut String| -> Result<()> {
        let c = child(i)?;
        let start = values + offset_of(i)?;
        let end = start + len_of(i)?;
        if end > data.len() {
            bail!("jsonb entry {i} runs to {end}, past the {}-byte value", data.len());
        }
        match c & JENTRY_TYPEMASK {
            JENTRY_ISNULL => out.push_str("null"),
            JENTRY_ISBOOL_TRUE => out.push_str("true"),
            JENTRY_ISBOOL_FALSE => out.push_str("false"),
            JENTRY_ISSTRING => escape_json(&data[start..end], out),
            JENTRY_ISNUMERIC => {
                // The stored length covers the alignment padding too.
                let at = align4(start);
                let n = numeric::parse(varlena_payload(&data[at..end])?)?;
                out.push_str(&numeric::to_text(&n));
            }
            JENTRY_ISCONTAINER => render_container(data, align4(start), out)?,
            other => bail!("unknown jsonb entry type {other:#010x}"),
        }
        Ok(())
    };

    if is_scalar {
        // A bare scalar is stored as a one-element array; print it unwrapped.
        return render_entry(0, out);
    }
    if is_object {
        out.push('{');
        for i in 0..count {
            if i > 0 {
                out.push_str(", ");
            }
            render_entry(i, out)?; // key
            out.push_str(": ");
            render_entry(count + i, out)?; // value
        }
        out.push('}');
    } else {
        out.push('[');
        for i in 0..count {
            if i > 0 {
                out.push_str(", ");
            }
            render_entry(i, out)?;
        }
        out.push(']');
    }
    Ok(())
}

/// Strip the varlena header from a numeric embedded in a jsonb value. Such a
/// numeric is stored whole, header included, and is never compressed or
/// external.
fn varlena_payload(p: &[u8]) -> Result<&[u8]> {
    if p.is_empty() {
        bail!("jsonb numeric is empty");
    }
    let (start, total) = if p[0] & 0x01 == 0x01 {
        (1usize, (p[0] >> 1) as usize & 0x7F)
    } else {
        if p.len() < 4 {
            bail!("jsonb numeric has a truncated varlena header");
        }
        let w = u32::from_le_bytes(p[0..4].try_into().unwrap());
        (4usize, ((w >> 2) & 0x3FFF_FFFF) as usize)
    };
    if total > p.len() || total < start {
        bail!("jsonb numeric claims {total} bytes but only {} are present", p.len());
    }
    Ok(&p[start..total])
}

/// Matches `escape_json`: named escapes for the usual controls, `\uXXXX` for
/// anything else below space, everything else verbatim.
fn escape_json(bytes: &[u8], out: &mut String) {
    out.push('"');
    for ch in String::from_utf8_lossy(bytes).chars() {
        match ch {
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A numeric as jsonb stores it: 4-byte varlena header, then the packed form.
    fn jnumeric(short_header: u16, digits: &[i16]) -> Vec<u8> {
        let mut n = short_header.to_le_bytes().to_vec();
        for d in digits {
            n.extend_from_slice(&d.to_le_bytes());
        }
        let mut v = (((n.len() + 4) as u32) << 2).to_le_bytes().to_vec();
        v.extend_from_slice(&n);
        v
    }

    /// `{"a": 1}` laid out by hand: header, two JEntries, then "a", padding,
    /// and the numeric.
    #[test]
    fn object_with_a_numeric_value() {
        let num = jnumeric(0x8000, &[1]); // weight 0, dscale 0, digit 1
        let mut v = (JB_FOBJECT | 1).to_le_bytes().to_vec();
        v.extend_from_slice(&(JENTRY_ISSTRING | 1).to_le_bytes()); // key "a", len 1
        let pad = 3usize; // "a" ends at 1, the numeric aligns to 4
        v.extend_from_slice(&(JENTRY_ISNUMERIC | (pad + num.len()) as u32).to_le_bytes());
        v.push(b'a');
        v.extend_from_slice(&[0, 0, 0]);
        v.extend_from_slice(&num);
        assert_eq!(to_text(&v).unwrap(), "{\"a\": 1}");
    }

    #[test]
    fn array_of_scalars() {
        let mut v = 3u32.to_le_bytes().to_vec(); // array, three elements
        v.extend_from_slice(&JENTRY_ISNULL.to_le_bytes());
        v.extend_from_slice(&JENTRY_ISBOOL_TRUE.to_le_bytes());
        v.extend_from_slice(&(JENTRY_ISSTRING | 2).to_le_bytes());
        v.extend_from_slice(b"hi");
        assert_eq!(to_text(&v).unwrap(), "[null, true, \"hi\"]");
    }

    #[test]
    fn strings_are_escaped_like_escape_json() {
        let mut s = String::new();
        escape_json(b"a\"b\\c", &mut s);
        assert_eq!(s, "\"a\\\"b\\\\c\"");

        let mut s = String::new();
        escape_json(&[b'x', 0x0a, 0x09, 0x01], &mut s);
        assert_eq!(s, "\"x\\n\\t\\u0001\"");
    }
}
