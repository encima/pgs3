//! Postgres `numeric`: on-disk unpacking, plus the two output forms.
//!
//! On disk a numeric is a packed 2- or 4-byte header followed by base-10000
//! digits. `numeric_send` unpacks that into four big-endian int16 fields, and
//! `numeric_out` renders it as decimal text. jsonb needs the text form, because
//! jsonb stores its numbers as numerics and `jsonb_send` emits jsonb *text*.

use anyhow::{bail, Result};

const NUMERIC_SIGN_MASK: u16 = 0xC000;
pub const NUMERIC_POS: u16 = 0x0000;
pub const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_SHORT: u16 = 0x8000;
const NUMERIC_SPECIAL: u16 = 0xC000;
const NUMERIC_SHORT_SIGN_MASK: u16 = 0x2000;
const NUMERIC_SHORT_DSCALE_MASK: u16 = 0x1F80;
const NUMERIC_SHORT_DSCALE_SHIFT: u16 = 7;
const NUMERIC_SHORT_WEIGHT_SIGN_MASK: u16 = 0x0040;
const NUMERIC_SHORT_WEIGHT_MASK: u16 = 0x003F;
const NUMERIC_EXT_SIGN_MASK: u16 = 0xF000;
pub const NUMERIC_NAN: u16 = 0xC000;
pub const NUMERIC_PINF: u16 = 0xD000;
pub const NUMERIC_NINF: u16 = 0xF000;

/// Digits are base 10000, four decimal places each.
const DEC_DIGITS: usize = 4;

#[derive(Debug, PartialEq)]
pub struct Numeric {
    pub sign: u16,
    /// Position of `digits[0]`, in units of 10000, relative to the point.
    pub weight: i16,
    /// Decimal places to display.
    pub dscale: u16,
    pub digits: Vec<i16>,
}


pub fn parse(b: &[u8]) -> Result<Numeric> {
    if b.len() < 2 {
        bail!("numeric datum is {} bytes, too short for a header", b.len());
    }
    let header = u16::from_le_bytes([b[0], b[1]]);

    let (sign, weight, dscale, digit_bytes): (u16, i16, u16, &[u8]) =
        if header & NUMERIC_SIGN_MASK == NUMERIC_SPECIAL {
            // NaN and the infinities carry no digits.
            let sign = match header & NUMERIC_EXT_SIGN_MASK {
                NUMERIC_NAN => NUMERIC_NAN,
                NUMERIC_PINF => NUMERIC_PINF,
                NUMERIC_NINF => NUMERIC_NINF,
                other => bail!("unknown special numeric header {other:#06x}"),
            };
            (sign, 0, 0, &[][..])
        } else if header & NUMERIC_SIGN_MASK == NUMERIC_SHORT {
            let sign = if header & NUMERIC_SHORT_SIGN_MASK != 0 { NUMERIC_NEG } else { NUMERIC_POS };
            let dscale = (header & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT;
            // The weight is a 7-bit signed field: sign bit plus 6 magnitude bits.
            let mut weight = (header & NUMERIC_SHORT_WEIGHT_MASK) as i16;
            if header & NUMERIC_SHORT_WEIGHT_SIGN_MASK != 0 {
                weight |= !(NUMERIC_SHORT_WEIGHT_MASK as i16);
            }
            (sign, weight, dscale, &b[2..])
        } else {
            if b.len() < 4 {
                bail!("long-form numeric datum is {} bytes, too short", b.len());
            }
            (header & NUMERIC_SIGN_MASK, i16::from_le_bytes([b[2], b[3]]), header & 0x3FFF, &b[4..])
        };

    if digit_bytes.len() % 2 != 0 {
        bail!("numeric digit array is {} bytes, not a whole number of digits", digit_bytes.len());
    }
    let digits =
        digit_bytes.chunks_exact(2).map(|d| i16::from_le_bytes([d[0], d[1]])).collect();
    Ok(Numeric { sign, weight, dscale, digits })
}

/// What `numeric_send` puts on the wire: four big-endian int16s, then digits.
pub fn to_send(n: &Numeric) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + n.digits.len() * 2);
    for v in [n.digits.len() as i16, n.weight, n.sign as i16, n.dscale as i16] {
        out.extend_from_slice(&v.to_be_bytes());
    }
    for d in &n.digits {
        out.extend_from_slice(&d.to_be_bytes());
    }
    out
}

/// What `numeric_out` prints. Follows `get_str_from_var`: the first base-10000
/// digit drops its leading zeroes, every later one is zero-padded to four, and
/// the fraction is generated in whole digits then cut to `dscale`.
pub fn to_text(n: &Numeric) -> String {
    match n.sign {
        NUMERIC_NAN => return "NaN".into(),
        NUMERIC_PINF => return "Infinity".into(),
        NUMERIC_NINF => return "-Infinity".into(),
        _ => {}
    }

    let mut s = String::new();
    if n.sign == NUMERIC_NEG {
        s.push('-');
    }

    let digit_at = |d: i32| -> i16 {
        if d >= 0 && (d as usize) < n.digits.len() {
            n.digits[d as usize]
        } else {
            0
        }
    };

    // Integer part.
    let mut d: i32 = 0;
    if n.weight < 0 {
        s.push('0');
        d = n.weight as i32 + 1;
    } else {
        while d <= n.weight as i32 {
            let dig = digit_at(d);
            if d == 0 {
                s.push_str(&dig.to_string());
            } else {
                s.push_str(&format!("{dig:04}"));
            }
            d += 1;
        }
    }

    if n.dscale > 0 {
        s.push('.');
        let mut frac = String::new();
        while frac.len() < n.dscale as usize {
            frac.push_str(&format!("{:04}", digit_at(d)));
            d += 1;
        }
        frac.truncate(n.dscale as usize);
        s.push_str(&frac);
    }
    let _ = DEC_DIGITS;
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn short(sign_neg: bool, weight: i16, dscale: u16, digits: &[i16]) -> Vec<u8> {
        let mut h = NUMERIC_SHORT | (dscale << NUMERIC_SHORT_DSCALE_SHIFT);
        if sign_neg {
            h |= NUMERIC_SHORT_SIGN_MASK;
        }
        h |= (weight as u16) & NUMERIC_SHORT_WEIGHT_MASK;
        if weight < 0 {
            h |= NUMERIC_SHORT_WEIGHT_SIGN_MASK;
        }
        let mut b = h.to_le_bytes().to_vec();
        for d in digits {
            b.extend_from_slice(&d.to_le_bytes());
        }
        b
    }

    #[test]
    fn renders_like_numeric_out() {
        // 10.50 -> weight 0, dscale 2, digits [10, 5000]
        assert_eq!(to_text(&parse(&short(false, 0, 2, &[10, 5000])).unwrap()), "10.50");
        // -3.25
        assert_eq!(to_text(&parse(&short(true, 0, 2, &[3, 2500])).unwrap()), "-3.25");
        // 0.00 -> no digits at all
        assert_eq!(to_text(&parse(&short(false, 0, 2, &[])).unwrap()), "0.00");
        // 1234567.89 -> weight 1, digits [123, 4567, 8900]
        assert_eq!(to_text(&parse(&short(false, 1, 2, &[123, 4567, 8900])).unwrap()), "1234567.89");
        // 0.0001 -> weight -1, digits [1]
        assert_eq!(to_text(&parse(&short(false, -1, 4, &[1])).unwrap()), "0.0001");
    }

    #[test]
    fn specials_render_by_name() {
        assert_eq!(to_text(&parse(&NUMERIC_NAN.to_le_bytes()).unwrap()), "NaN");
        assert_eq!(to_text(&parse(&NUMERIC_PINF.to_le_bytes()).unwrap()), "Infinity");
        assert_eq!(to_text(&parse(&NUMERIC_NINF.to_le_bytes()).unwrap()), "-Infinity");
    }

    #[test]
    fn send_form_is_four_be_fields_then_digits() {
        let n = parse(&short(false, 0, 2, &[10, 5000])).unwrap();
        let w = to_send(&n);
        assert_eq!(&w[0..2], &2i16.to_be_bytes()); // ndigits
        assert_eq!(&w[2..4], &0i16.to_be_bytes()); // weight
        assert_eq!(&w[4..6], &(NUMERIC_POS as i16).to_be_bytes());
        assert_eq!(&w[6..8], &2i16.to_be_bytes()); // dscale
        assert_eq!(&w[8..10], &10i16.to_be_bytes());
        assert_eq!(&w[10..12], &5000i16.to_be_bytes());
    }
}
