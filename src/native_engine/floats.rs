// Format codes for (unsigned_int, float) sized types, used for byte-wise casts.
// In Python these are struct format strings; in Rust we handle widths directly
// via to_bits()/from_bits() for f64/f32 and manual conversion for f16.
//
// STRUCT_FORMATS = {16: ("!H", "!e"), 32: ("!I", "!f"), 64: ("!Q", "!d")}
// TO_SIGNED_FORMAT = {"!H": "!h", "!I": "!i", "!Q": "!q"}

// Smallest positive non-zero numbers that are fully representable by an
// IEEE-754 float, calculated with the width's associated minimum exponent.
// Values from https://en.wikipedia.org/wiki/IEEE_754#Basic_and_interchange_formats
//   16: 2 ** -(2 ** (5 - 1) - 2)   = 2^-14
//   32: 2 ** -(2 ** (8 - 1) - 2)   = 2^-126
//   64: 2 ** -(2 ** (11 - 1) - 2)  = 2^-1022
pub fn width_smallest_normal(width: u32) -> f64 {
    match width {
        16 => 2f64.powi(-(2i32.pow(5 - 1) - 2)),
        32 => 2f64.powi(-(2i32.pow(8 - 1) - 2)),
        64 => 2f64.powi(-(2i32.pow(11 - 1) - 2)),
        _ => panic!("unsupported width: {}", width),
    }
}

// assert width_smallest_normals[64] == float_info.min
// (verified in test below)

pub const MANTISSA_MASK: u64 = (1 << 52) - 1;

// --- f16 conversion helpers ---
// Python uses struct.pack("!e", ...) / struct.unpack("!e", ...) for half-precision.
// Rust stable doesn't have f16, so we implement IEEE 754 binary16 conversion manually.

/// Convert f64 to IEEE 754 binary16 (half-precision) bit pattern.
/// Mirrors Python's `struct.pack("!e", value)`.
fn f64_to_f16_bits(value: f64) -> u16 {
    let bits = value.to_bits();
    let sign = ((bits >> 63) & 1) as u16;
    let exp = ((bits >> 52) & 0x7FF) as i32;
    let frac = bits & 0x000F_FFFF_FFFF_FFFF; // 52-bit fraction

    if exp == 0x7FF {
        // Inf or NaN
        return if frac == 0 {
            (sign << 15) | 0x7C00 // ±Inf
        } else {
            // NaN — preserve some mantissa bits, ensure at least one is set
            (sign << 15) | 0x7C00 | ((frac >> 42) as u16).max(1)
        };
    }

    if exp == 0 {
        // f64 subnormal → way too small for f16
        return sign << 15; // ±0
    }

    let unbiased = exp - 1023; // f64 bias

    if unbiased > 15 {
        // Overflow to ±Inf
        return (sign << 15) | 0x7C00;
    }

    if unbiased < -24 {
        // Smaller than smallest f16 subnormal
        return sign << 15; // ±0
    }

    if unbiased >= -14 {
        // Normal f16 range
        let f16_exp = (unbiased + 15) as u16;
        let f16_frac = (frac >> 42) as u16; // top 10 bits of 52-bit fraction
        // Rounding: round to nearest even using bit 41
        let round_bit = (frac >> 41) & 1;
        let sticky = frac & ((1u64 << 41) - 1);
        let mut result = (sign << 15) | (f16_exp << 10) | f16_frac;
        if round_bit != 0 && (sticky != 0 || (f16_frac & 1) != 0) {
            result += 1; // carry into exponent is correct
        }
        result
    } else {
        // Subnormal f16: unbiased in [-24, -15]
        // full mantissa with implicit 1 bit
        let full_frac = frac | (1u64 << 52);
        let shift = (-14 - unbiased) as u64; // extra shift: 1..10
        // We need to shift full_frac right by (42 + shift) to get the f16 subnormal mantissa
        let total_shift = 42 + shift;
        let shifted = full_frac >> total_shift;
        let f16_frac = (shifted as u16) & 0x3FF;
        // Rounding
        let round_bit = if total_shift > 0 {
            (full_frac >> (total_shift - 1)) & 1
        } else {
            0
        };
        let sticky_mask = if total_shift > 1 {
            (1u64 << (total_shift - 1)) - 1
        } else {
            0
        };
        let sticky = full_frac & sticky_mask;
        let mut result = (sign << 15) | f16_frac;
        if round_bit != 0 && (sticky != 0 || (f16_frac & 1) != 0) {
            result += 1;
        }
        result
    }
}

/// Convert IEEE 754 binary16 (half-precision) bit pattern to f64.
/// Mirrors Python's `struct.unpack("!e", bytes)`.
/// This conversion is always exact (every f16 value is representable in f64).
fn f16_bits_to_f64(bits: u16) -> f64 {
    let sign = ((bits >> 15) & 1) as u32;
    let exp = ((bits >> 10) & 0x1F) as u32;
    let frac = (bits & 0x3FF) as u32;

    let f32_bits: u32 = if exp == 31 {
        // Inf or NaN
        (sign << 31) | 0x7F80_0000 | (frac << 13)
    } else if exp == 0 {
        if frac == 0 {
            // ±0
            sign << 31
        } else {
            // Subnormal f16 → normalize to f32
            let mut m = frac;
            let mut e = 0i32;
            // Shift until the implicit 1 bit is in position 10
            while (m & 0x400) == 0 {
                m <<= 1;
                e += 1;
            }
            m &= 0x3FF; // remove the implicit 1 bit
            let f32_exp = (127 - 15 + 1 - e) as u32;
            (sign << 31) | (f32_exp << 23) | (m << 13)
        }
    } else {
        // Normal f16 → normal f32
        let f32_exp = (exp as i32 - 15 + 127) as u32;
        (sign << 31) | (f32_exp << 23) | (frac << 13)
    };

    f32::from_bits(f32_bits) as f64
}

// --- Core functions, mirroring floats.py ---

/// `reinterpret_bits(x, fmt_flt, fmt_int)` — float bits as unsigned int
#[allow(dead_code)]
fn reinterpret_bits_f_to_u(value: f64, width: u32) -> u64 {
    float_to_int(value, width)
}

/// `reinterpret_bits(x, fmt_int, fmt_flt)` — unsigned int bits as float
#[allow(dead_code)]
fn reinterpret_bits_u_to_f(value: u64, width: u32) -> f64 {
    int_to_float(value, width)
}

/// `reinterpret_bits(x, fmt_flt, fmt_int_signed)` — float bits as signed int
fn reinterpret_bits_f_to_i(value: f64, width: u32) -> i64 {
    match width {
        16 => f64_to_f16_bits(value) as i16 as i64,
        32 => (value as f32).to_bits() as i32 as i64,
        64 => value.to_bits() as i64,
        _ => panic!("unsupported width: {}", width),
    }
}

/// `reinterpret_bits(x, fmt_int_signed, fmt_flt)` — signed int bits as float
fn reinterpret_bits_i_to_f(value: i64, width: u32) -> f64 {
    match width {
        16 => f16_bits_to_f64(value as u16),
        32 => f32::from_bits(value as u32) as f64,
        64 => f64::from_bits(value as u64),
        _ => panic!("unsupported width: {}", width),
    }
}

pub fn float_of(x: f64, width: u32) -> f64 {
    assert!(width == 16 || width == 32 || width == 64);
    if width == 64 {
        x
    } else if width == 32 {
        // reinterpret_bits(float(x), "!f", "!f")
        (x as f32) as f64
    } else {
        // reinterpret_bits(float(x), "!e", "!e")
        f16_bits_to_f64(f64_to_f16_bits(x))
    }
}

pub fn is_negative(x: f64) -> bool {
    // math.copysign(1.0, x) < 0
    1.0f64.copysign(x) < 0.0
}

pub fn count_between_floats(x: f64, y: f64, width: u32) -> u64 {
    assert!(x <= y);
    if is_negative(x) {
        if is_negative(y) {
            float_to_int(x, width) - float_to_int(y, width) + 1
        } else {
            count_between_floats(x, -0.0, width) + count_between_floats(0.0, y, width)
        }
    } else {
        assert!(!is_negative(y));
        float_to_int(y, width) - float_to_int(x, width) + 1
    }
}

pub fn float_to_int(value: f64, width: u32) -> u64 {
    match width {
        16 => f64_to_f16_bits(value) as u64,
        32 => (value as f32).to_bits() as u64,
        64 => value.to_bits(),
        _ => panic!("unsupported width: {}", width),
    }
}

pub fn int_to_float(value: u64, width: u32) -> f64 {
    match width {
        16 => f16_bits_to_f64(value as u16),
        32 => f32::from_bits(value as u32) as f64,
        64 => f64::from_bits(value),
        _ => panic!("unsupported width: {}", width),
    }
}

/// Return the first float larger than finite `value` — IEEE 754's `nextUp`.
///
/// From https://stackoverflow.com/a/10426033, with thanks to Mark Dickinson.
pub fn next_up(value: f64, width: u32) -> f64 {
    if value.is_nan() || (value.is_infinite() && value > 0.0) {
        return value;
    }
    if value == 0.0 && is_negative(value) {
        return 0.0;
    }
    // Note: n is signed; float_to_int returns unsigned
    let n = reinterpret_bits_f_to_i(value, width);
    let n = if n >= 0 { n + 1 } else { n - 1 };
    reinterpret_bits_i_to_f(n, width)
}

pub fn next_down(value: f64, width: u32) -> f64 {
    -next_up(-value, width)
}

pub fn next_down_normal(value: f64, width: u32, allow_subnormal: bool) -> f64 {
    let value = next_down(value, width);
    if !allow_subnormal && 0.0 < value.abs() && value.abs() < width_smallest_normal(width) {
        return if value > 0.0 {
            0.0
        } else {
            -width_smallest_normal(width)
        };
    }
    value
}

pub fn next_up_normal(value: f64, width: u32, allow_subnormal: bool) -> f64 {
    -next_down_normal(-value, width, allow_subnormal)
}

/// Less-than-or-equals, but strictly orders -0.0 and 0.0
pub fn sign_aware_lte(x: f64, y: f64) -> bool {
    if x == 0.0 && y == 0.0 {
        // math.copysign(1.0, x) <= math.copysign(1.0, y)
        1.0f64.copysign(x) <= 1.0f64.copysign(y)
    } else {
        x <= y
    }
}

/// Given a value and lower/upper bounds, 'clamp' the value so that
/// it satisfies lower <= value <= upper.  NaN is mapped to lower.
pub fn clamp(lower: f64, value: f64, upper: f64) -> f64 {
    // this seems pointless (and is for integers), but handles the -0.0/0.0 case.
    if !sign_aware_lte(lower, value) {
        return lower;
    }
    if !sign_aware_lte(value, upper) {
        return upper;
    }
    value
}

/// Return a function that clamps positive floats into the given bounds.
// TODO: In Python, this imports `choice_permitted` from
// `hypothesis.internal.conjecture.choice`. Replace `choice_permitted_float`
// with the real implementation once that module is ported.
pub fn make_float_clamper(
    min_value: f64,
    max_value: f64,
    allow_nan: bool,
    smallest_nonzero_magnitude: f64,
) -> impl Fn(f64) -> f64 {
    assert!(sign_aware_lte(min_value, max_value));
    let range_size = (max_value - min_value).min(f64::MAX);

    move |f: f64| -> f64 {
        if choice_permitted_float(
            f,
            min_value,
            max_value,
            allow_nan,
            smallest_nonzero_magnitude,
        ) {
            return f;
        }
        // Outside bounds; pick a new value, sampled from the allowed range,
        // using the mantissa bits.
        let mant = float_to_int(f.abs(), 64) & MANTISSA_MASK;
        let mut f = min_value + range_size * (mant as f64 / MANTISSA_MASK as f64);

        // if we resampled into the space disallowed by smallest_nonzero_magnitude,
        // default to smallest_nonzero_magnitude.
        if 0.0 < f.abs() && f.abs() < smallest_nonzero_magnitude {
            f = smallest_nonzero_magnitude;
            // we must have either -smallest_nonzero_magnitude <= min_value or
            // smallest_nonzero_magnitude >= max_value, or no values would be
            // possible. If smallest_nonzero_magnitude is not valid (because it's
            // larger than max_value), then -smallest_nonzero_magnitude must be valid.
            if smallest_nonzero_magnitude > max_value {
                f *= -1.0;
            }
        }

        // Re-enforce the bounds (just in case of floating point arithmetic error)
        clamp(min_value, f, max_value)
    }
}

// This checks whether a float value is valid for the given constraints.
pub(crate) fn choice_permitted_float(
    value: f64,
    min_value: f64,
    max_value: f64,
    allow_nan: bool,
    smallest_nonzero_magnitude: f64,
) -> bool {
    if value.is_nan() {
        return allow_nan;
    }
    if 0.0 < value.abs() && value.abs() < smallest_nonzero_magnitude {
        return false;
    }
    sign_aware_lte(min_value, value) && sign_aware_lte(value, max_value)
}

// next_up(0.0)
pub const SMALLEST_SUBNORMAL: f64 = 5e-324; // f64::from_bits(1)
// int_to_float(0x7FF8_0000_0000_0001) — nonzero mantissa
pub const SIGNALING_NAN: f64 = f64::NAN; // placeholder: exact bit pattern is 0x7FF8_0000_0000_0001
pub const MAX_PRECISE_INTEGER: f64 = 9007199254740992.0; // 2**53
// assert math.isnan(SIGNALING_NAN) — verified by NAN
// assert math.copysign(1, SIGNALING_NAN) == 1 — verified: positive NAN

// ============================================================================
// From hypothesis/internal/conjecture/floats.py
//
// This module implements support for arbitrary floating point numbers in
// Conjecture. It doesn't make any attempt to get a good distribution, only to
// get a format that will shrink well.
//
// It works by defining an encoding of non-negative floating point numbers
// (including NaN values with a zero sign bit) that has good lexical shrinking
// properties.
//
// This encoding is a tagged union of two separate encodings for floating point
// numbers, with the tag being the first bit of 64 and the remaining 63-bits being
// the payload.
//
// If the tag bit is 0, the next 7 bits are ignored, and the remaining 7 bytes are
// interpreted as a 7 byte integer in big-endian order and then converted to a
// float (there is some redundancy here, as 7 * 8 = 56, which is larger than the
// largest integer that floating point numbers can represent exactly, so multiple
// encodings may map to the same float).
//
// If the tag bit is 1, we instead use something that is closer to the normal
// representation of floats (and can represent every non-negative float exactly)
// but has a better ordering:
//
// 1. NaNs are ordered after everything else.
// 2. Infinity is ordered after every finite number.
// 3. The sign is ignored unless two floating point numbers are identical in
//    absolute magnitude. In that case, the positive is ordered before the
//    negative.
// 4. Positive floating point numbers are ordered first by int(x) where
//    encoding(x) < encoding(y) if int(x) < int(y).
// 5. If int(x) == int(y) then x and y are sorted towards lower denominators of
//    their fractional parts.
//
// The format of this encoding of floating point goes as follows:
//
//     [exponent] [mantissa]
//
// Each of these is the same size their equivalent in IEEE floating point, but are
// in a different format.
//
// We translate exponents as follows:
//
//     1. The maximum exponent (2 ** 11 - 1) is left unchanged.
//     2. We reorder the remaining exponents so that all of the positive exponents
//        are first, in increasing order, followed by all of the negative
//        exponents in decreasing order (where positive/negative is done by the
//        unbiased exponent e - 1023).
//
// We translate the mantissa as follows:
//
//     1. If the unbiased exponent is <= 0 we reverse it bitwise.
//     2. If the unbiased exponent is >= 52 we leave it alone.
//     3. If the unbiased exponent is in the range [1, 51] then we reverse the
//        low k bits, where k is 52 - unbiased exponent.
//
// The low bits correspond to the fractional part of the floating point number.
// Reversing it bitwise means that we try to minimize the low bits, which kills
// off the higher powers of 2 in the fraction first.
// ============================================================================

pub const MAX_EXPONENT: u16 = 0x7FF;

const BIAS: i32 = 1023;
#[allow(dead_code)]
const MAX_POSITIVE_EXPONENT: i32 = MAX_EXPONENT as i32 - 1 - BIAS;

fn exponent_key(e: u16) -> f64 {
    if e == MAX_EXPONENT {
        return f64::INFINITY;
    }
    let unbiased = e as i32 - BIAS;
    if unbiased < 0 {
        10000.0 - unbiased as f64
    } else {
        unbiased as f64
    }
}

// ENCODING_TABLE = array("H", sorted(range(MAX_EXPONENT + 1), key=exponent_key))
// DECODING_TABLE = array("H", [0]) * len(ENCODING_TABLE)
// for i, b in enumerate(ENCODING_TABLE):
//     DECODING_TABLE[b] = i
use std::sync::LazyLock;

struct ExponentTables {
    encoding: Vec<u16>,
    decoding: Vec<u16>,
}

static EXPONENT_TABLES: LazyLock<ExponentTables> = LazyLock::new(|| {
    let mut indices: Vec<u16> = (0..=MAX_EXPONENT).collect();
    indices.sort_by(|&a, &b| exponent_key(a).partial_cmp(&exponent_key(b)).unwrap());
    let encoding = indices;

    let mut decoding = vec![0u16; encoding.len()];
    for (i, &b) in encoding.iter().enumerate() {
        decoding[b as usize] = i as u16;
    }

    ExponentTables { encoding, decoding }
});

/// Take an integer and turn it into a suitable floating point exponent
/// such that lexicographically simpler leads to simpler floats.
pub fn decode_exponent(e: u16) -> u16 {
    assert!(e <= MAX_EXPONENT);
    EXPONENT_TABLES.encoding[e as usize]
}

/// Take a floating point exponent and turn it back into the equivalent
/// result from conjecture.
pub fn encode_exponent(e: u16) -> u16 {
    assert!(e <= MAX_EXPONENT);
    EXPONENT_TABLES.decoding[e as usize]
}

fn reverse_byte(b: u8) -> u8 {
    let mut result: u8 = 0;
    let mut b = b;
    for _ in 0..8 {
        result <<= 1;
        result |= b & 1;
        b >>= 1;
    }
    result
}

// Table mapping individual bytes to the equivalent byte with the bits of the
// byte reversed. e.g. 1=0b1 is mapped to 0b10000000=0x80=128. We use this
// precalculated table to simplify calculating the bitwise reversal of a longer
// integer.
static REVERSE_BITS_TABLE: LazyLock<[u8; 256]> = LazyLock::new(|| {
    let mut table = [0u8; 256];
    for i in 0..256 {
        table[i] = reverse_byte(i as u8);
    }
    table
});

/// Reverse a 64-bit integer bitwise.
///
/// We do this by breaking it up into 8 bytes. The 64-bit integer is then the
/// concatenation of each of these bytes. We reverse it by reversing each byte
/// on its own using the REVERSE_BITS_TABLE above, and then concatenating the
/// reversed bytes.
///
/// In this case concatenating consists of shifting them into the right
/// position for the word and then oring the bits together.
fn reverse64(v: u64) -> u64 {
    let t = &*REVERSE_BITS_TABLE;
    ((t[((v >> 0) & 0xFF) as usize] as u64) << 56)
        | ((t[((v >> 8) & 0xFF) as usize] as u64) << 48)
        | ((t[((v >> 16) & 0xFF) as usize] as u64) << 40)
        | ((t[((v >> 24) & 0xFF) as usize] as u64) << 32)
        | ((t[((v >> 32) & 0xFF) as usize] as u64) << 24)
        | ((t[((v >> 40) & 0xFF) as usize] as u64) << 16)
        | ((t[((v >> 48) & 0xFF) as usize] as u64) << 8)
        | ((t[((v >> 56) & 0xFF) as usize] as u64) << 0)
}

// Note: MANTISSA_MASK is already defined above as (1 << 52) - 1.
// The conjecture/floats.py also defines MANTISSA_MASK = (1 << 52) - 1,
// identical to the one in internal/floats.py.

fn reverse_bits(x: u64, n: u32) -> u64 {
    assert!(64 - x.leading_zeros() <= n && n <= 64);
    let x = reverse64(x);
    x >> (64 - n)
}

fn update_mantissa(unbiased_exponent: i32, mantissa: u64) -> u64 {
    if unbiased_exponent <= 0 {
        reverse_bits(mantissa, 52)
    } else if unbiased_exponent <= 51 {
        let n_fractional_bits = (52 - unbiased_exponent) as u32;
        let fractional_part = mantissa & ((1u64 << n_fractional_bits) - 1);
        let mantissa = mantissa ^ fractional_part;
        mantissa | reverse_bits(fractional_part, n_fractional_bits)
    } else {
        mantissa
    }
}

pub fn lex_to_float(i: u64) -> f64 {
    let has_fractional_part = i >> 63;
    if has_fractional_part != 0 {
        let exponent = ((i >> 52) & ((1 << 11) - 1)) as u16;
        let exponent = decode_exponent(exponent);
        let mantissa = i & MANTISSA_MASK;
        let mantissa = update_mantissa(exponent as i32 - BIAS, mantissa);

        assert!(64 - mantissa.leading_zeros() <= 52);

        int_to_float(((exponent as u64) << 52) | mantissa, 64)
    } else {
        let integral_part = i & ((1u64 << 56) - 1);
        integral_part as f64
    }
}

pub fn float_to_lex(f: f64) -> u64 {
    if is_simple(f) {
        assert!(f >= 0.0);
        return f as u64;
    }
    base_float_to_lex(f)
}

fn base_float_to_lex(f: f64) -> u64 {
    let i = float_to_int(f, 64);
    let i = i & ((1u64 << 63) - 1);
    let exponent = (i >> 52) as u16;
    let mantissa = i & MANTISSA_MASK;
    let mantissa = update_mantissa(exponent as i32 - BIAS, mantissa);
    let exponent = encode_exponent(exponent);

    assert!(64 - mantissa.leading_zeros() <= 52);
    (1u64 << 63) | ((exponent as u64) << 52) | mantissa
}

pub fn is_simple(f: f64) -> bool {
    // try: i = int(f)
    // except (ValueError, OverflowError): return False
    if f.is_nan() || f.is_infinite() {
        return false;
    }
    let i = f as i64;
    // if i != f: return False
    if i as f64 != f {
        return false;
    }
    // return i.bit_length() <= 56
    let bit_length = if i == 0 {
        0
    } else {
        64 - (i.unsigned_abs()).leading_zeros()
    };
    bit_length <= 56
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_width_smallest_normal_64() {
        assert_eq!(width_smallest_normal(64), f64::MIN_POSITIVE);
    }

    #[test]
    fn test_is_negative() {
        assert!(is_negative(-1.0));
        assert!(is_negative(-0.0));
        assert!(!is_negative(0.0));
        assert!(!is_negative(1.0));
        assert!(is_negative(f64::NEG_INFINITY));
        assert!(!is_negative(f64::INFINITY));
    }

    #[test]
    fn test_float_to_int_int_to_float_roundtrip_64() {
        for &v in &[
            0.0,
            -0.0,
            1.0,
            -1.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN_POSITIVE,
        ] {
            assert_eq!(int_to_float(float_to_int(v, 64), 64).to_bits(), v.to_bits());
        }
    }

    #[test]
    fn test_float_to_int_int_to_float_roundtrip_32() {
        for &v in &[0.0f32, -0.0, 1.0, -1.0, f32::INFINITY, f32::NEG_INFINITY] {
            let v64 = v as f64;
            let bits = float_to_int(v64, 32);
            let back = int_to_float(bits, 32);
            assert_eq!(back.to_bits(), v64.to_bits());
        }
    }

    #[test]
    fn test_next_up_64() {
        assert_eq!(next_up(0.0, 64), SMALLEST_SUBNORMAL);
        assert_eq!(next_up(-0.0, 64), 0.0);
        assert_eq!(next_up(f64::INFINITY, 64), f64::INFINITY);
        assert!(next_up(f64::NAN, 64).is_nan());
        assert!(next_up(1.0, 64) > 1.0);
        assert!(next_up(-1.0, 64) > -1.0);
    }

    #[test]
    fn test_next_down_64() {
        assert_eq!(next_down(0.0, 64), -SMALLEST_SUBNORMAL);
        assert_eq!(next_down(-0.0, 64), -SMALLEST_SUBNORMAL);
        assert_eq!(next_down(f64::NEG_INFINITY, 64), f64::NEG_INFINITY);
        assert!(next_down(1.0, 64) < 1.0);
    }

    #[test]
    fn test_sign_aware_lte() {
        assert!(sign_aware_lte(-0.0, 0.0));
        assert!(!sign_aware_lte(0.0, -0.0));
        assert!(sign_aware_lte(0.0, 0.0));
        assert!(sign_aware_lte(-0.0, -0.0));
        assert!(sign_aware_lte(-1.0, 1.0));
        assert!(!sign_aware_lte(1.0, -1.0));
    }

    #[test]
    fn test_clamp() {
        assert_eq!(clamp(0.0, 5.0, 10.0), 5.0);
        assert_eq!(clamp(0.0, -1.0, 10.0), 0.0);
        assert_eq!(clamp(0.0, 15.0, 10.0), 10.0);
        // NaN is mapped to lower
        assert_eq!(clamp(0.0, f64::NAN, 10.0), 0.0);
    }

    #[test]
    fn test_clamp_neg_zero() {
        let result = clamp(-0.0, -0.0, 0.0);
        assert!(result == 0.0); // -0.0 == 0.0
        assert!(is_negative(result)); // but it's -0.0
    }

    #[test]
    fn test_count_between_floats() {
        assert_eq!(count_between_floats(0.0, 0.0, 64), 1);
        assert_eq!(count_between_floats(-0.0, -0.0, 64), 1);
        assert_eq!(count_between_floats(1.0, 1.0, 64), 1);
    }

    #[test]
    fn test_float_of_64() {
        assert_eq!(float_of(1.5, 64), 1.5);
    }

    #[test]
    fn test_float_of_32() {
        let val = float_of(1.1, 32);
        // Should lose precision to f32
        assert_eq!(val, 1.1f32 as f64);
    }

    #[test]
    fn test_constants() {
        assert!(SMALLEST_SUBNORMAL > 0.0);
        assert!(SMALLEST_SUBNORMAL < f64::MIN_POSITIVE);
        assert!(SIGNALING_NAN.is_nan());
        assert_eq!(MAX_PRECISE_INTEGER, 2.0f64.powi(53));
    }

    #[test]
    fn test_f16_roundtrip_special_values() {
        // 0.0
        assert_eq!(f16_bits_to_f64(f64_to_f16_bits(0.0)), 0.0);
        assert!(!is_negative(f16_bits_to_f64(f64_to_f16_bits(0.0))));

        // -0.0
        let neg_zero = f16_bits_to_f64(f64_to_f16_bits(-0.0));
        assert_eq!(neg_zero, 0.0);
        assert!(is_negative(neg_zero));

        // Inf
        assert_eq!(
            f16_bits_to_f64(f64_to_f16_bits(f64::INFINITY)),
            f64::INFINITY
        );
        assert_eq!(
            f16_bits_to_f64(f64_to_f16_bits(f64::NEG_INFINITY)),
            f64::NEG_INFINITY
        );

        // NaN
        assert!(f16_bits_to_f64(f64_to_f16_bits(f64::NAN)).is_nan());

        // 1.0
        assert_eq!(f16_bits_to_f64(f64_to_f16_bits(1.0)), 1.0);

        // -1.0
        assert_eq!(f16_bits_to_f64(f64_to_f16_bits(-1.0)), -1.0);
    }

    #[test]
    fn test_next_down_normal() {
        // With subnormals allowed
        let v = next_down_normal(width_smallest_normal(64), 64, true);
        assert!(v > 0.0);
        assert!(v < width_smallest_normal(64));

        // Without subnormals
        let v = next_down_normal(width_smallest_normal(64), 64, false);
        assert_eq!(v, 0.0);
    }

    #[test]
    fn test_next_up_normal() {
        // Without subnormals: next_up_normal from 0.0 should skip subnormals
        let v = next_up_normal(0.0, 64, false);
        assert_eq!(v, width_smallest_normal(64));
    }

    // --- conjecture/floats tests ---

    #[test]
    fn test_exponent_encode_decode_roundtrip() {
        for e in 0..=MAX_EXPONENT {
            let encoded = encode_exponent(e);
            let decoded = decode_exponent(encoded);
            assert_eq!(decoded, e, "roundtrip failed for exponent {}", e);
        }
    }

    #[test]
    fn test_decode_encode_roundtrip() {
        for e in 0..=MAX_EXPONENT {
            let decoded = decode_exponent(e);
            let encoded = encode_exponent(decoded);
            assert_eq!(encoded, e, "roundtrip failed for index {}", e);
        }
    }

    #[test]
    fn test_max_exponent_maps_to_self() {
        // The maximum exponent is left unchanged by the encoding
        assert_eq!(decode_exponent(MAX_EXPONENT), MAX_EXPONENT);
        assert_eq!(encode_exponent(MAX_EXPONENT), MAX_EXPONENT);
    }

    #[test]
    fn test_reverse64() {
        assert_eq!(reverse64(0), 0);
        assert_eq!(reverse64(1), 1u64 << 63);
        assert_eq!(reverse64(1u64 << 63), 1);
        assert_eq!(reverse64(0xFF), 0xFFu64 << 56);
    }

    #[test]
    fn test_reverse_bits() {
        assert_eq!(reverse_bits(0, 8), 0);
        assert_eq!(reverse_bits(1, 8), 128);
        assert_eq!(reverse_bits(0b1010, 4), 0b0101);
    }

    #[test]
    fn test_is_simple() {
        assert!(is_simple(0.0));
        assert!(is_simple(1.0));
        assert!(is_simple(42.0));
        assert!(!is_simple(0.5));
        assert!(!is_simple(1.5));
        assert!(!is_simple(f64::NAN));
        assert!(!is_simple(f64::INFINITY));
        assert!(!is_simple(f64::NEG_INFINITY));
    }

    #[test]
    fn test_lex_to_float_integers() {
        // Tag bit 0: integral encoding
        assert_eq!(lex_to_float(0), 0.0);
        assert_eq!(lex_to_float(1), 1.0);
        assert_eq!(lex_to_float(42), 42.0);
    }

    #[test]
    fn test_float_to_lex_integers() {
        assert_eq!(float_to_lex(0.0), 0);
        assert_eq!(float_to_lex(1.0), 1);
        assert_eq!(float_to_lex(42.0), 42);
    }

    #[test]
    fn test_lex_float_roundtrip() {
        // Test roundtrip for simple integers
        for i in 0..100u64 {
            let f = i as f64;
            assert_eq!(
                lex_to_float(float_to_lex(f)),
                f,
                "roundtrip failed for {}",
                f
            );
        }
    }

    #[test]
    fn test_lex_float_roundtrip_fractional() {
        // Test roundtrip for non-integer floats
        for &f in &[0.5, 1.5, 0.1, 3.14, 100.001, f64::INFINITY] {
            let lex = float_to_lex(f);
            let back = lex_to_float(lex);
            assert_eq!(back.to_bits(), f.to_bits(), "roundtrip failed for {}", f);
        }
    }

    #[test]
    fn test_lex_ordering_integers_before_fractions() {
        // Integers should have smaller lex values than non-integers
        assert!(float_to_lex(1.0) < float_to_lex(0.5));
        assert!(float_to_lex(1.0) < float_to_lex(1.5));
    }

    #[test]
    fn test_lex_ordering_smaller_integers_first() {
        assert!(float_to_lex(0.0) < float_to_lex(1.0));
        assert!(float_to_lex(1.0) < float_to_lex(2.0));
        assert!(float_to_lex(2.0) < float_to_lex(3.0));
    }

    #[test]
    fn test_update_mantissa_high_exponent_passthrough() {
        // unbiased_exponent >= 52: mantissa unchanged
        assert_eq!(update_mantissa(52, 0xABCD), 0xABCD);
        assert_eq!(update_mantissa(100, 0xABCD), 0xABCD);
    }

    #[test]
    fn test_update_mantissa_zero_exponent_full_reverse() {
        // unbiased_exponent <= 0: full 52-bit reversal
        let m = update_mantissa(0, 1);
        // bit 0 reversed to bit 51
        assert_eq!(m, 1u64 << 51);
    }
}
