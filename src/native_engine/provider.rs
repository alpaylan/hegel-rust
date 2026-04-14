use std::{collections::HashMap, i128};

use rand::distr::weighted;

use crate::native_engine::{
    choice::{ChoiceConstraints, ChoiceValue},
    data::ConjectureData,
    floats::next_up,
    intervalset::IntervalSet,
    random::Random,
};

trait Provider {
    fn draw_boolean(&mut self, p: f64) -> bool;
    fn draw_integer(
        &mut self,
        min_value: Option<i128>,
        max_value: Option<i128>,
        weights: Option<HashMap<i128, f64>>,
        shrink_towards: i128,
    ) -> i128;
    fn draw_float(
        &mut self,
        min_value: f64,
        max_value: f64,
        allow_nan: bool,
        smallest_nonzero_magnitude: f64,
    ) -> f64;
    fn draw_bytes(&mut self, min_size: usize, max_size: usize) -> Vec<u8>;
    fn draw_string(&mut self, intervals: IntervalSet, min_size: usize, max_size: usize) -> String;
}

struct HypothesisProvider {
    cd: Option<ConjectureData>,
    random: Random,
}

impl HypothesisProvider {
    fn new(cd: Option<ConjectureData>) -> Self {
        if let Some(cd) = cd {
            // If we have conjecture data, we need to be able to draw constants from it, which
            // requires random sampling to pick from the pools of constants. If we don't have
            // conjecture data, then we won't be drawing from any constant pools, so we don't
            // need a random source at all.
            Self {
                cd: Some(cd),
                random: todo!(),
            }
        } else {
            Self {
                cd: None,
                random: todo!(),
            }
        }
    }
}

impl Provider for HypothesisProvider {
    fn draw_boolean(&mut self, p: f64) -> bool {
        if p <= 0.0 {
            false
        } else if p >= 1.0 {
            true
        } else {
            self.random.random() < p
        }
    }

    fn draw_integer(
        &mut self,
        min_value: Option<i128>,
        max_value: Option<i128>,
        weights: Option<HashMap<i128, f64>>,
        shrink_towards: i128,
    ) -> i128 {
        if let Some(constant) = self.maybe_draw_constant(
            &ChoiceConstraints::Integer {
                min_value,
                max_value,
                weights,
                shrink_towards,
            },
            0.5,
        ) {
            if let ChoiceValue::Integer(value) = constant {
                return value;
            } else {
                unreachable!("maybe_draw_constant should only return an integer constant when given integer constraints");
            }
        }

        let center = 0.max(min_value.unwrap_or(i128::MIN)).min(max_value.unwrap_or(i128::MAX));

        if let Some(weights) = weights {
            let min_value = min_value.expect("Weights require min_value");
            let max_value = max_value.expect("Weights require max_value");
            todo!()
        }

        match (min_value, max_value) {
            (None, None) => self.draw_unbounded_integer(),
            (None, Some(max)) => {
                // TODO: This is a direct Hypothesis port, but it seems inefficient, maybe check how many times
                // this fails and consider a more direct approach that doesn't involve rejection sampling?
                let mut probe = max + 1;
                while max < probe {
                    probe = center + self.draw_unbounded_integer();
                }
                probe
            }
            (Some(min), None) => {
                // TODO: This is a direct Hypothesis port, but it seems inefficient, maybe check how many times
                // this fails and consider a more direct approach that doesn't involve rejection sampling?
                let mut probe = min - 1;
                while probe < min {
                    probe = center + self.draw_unbounded_integer();
                }
                probe
            }
            (Some(min), Some(max)) => self.draw_bounded_integer(min, max, true),
        }


    }

    fn draw_float(
        &mut self,
        min_value: f64,
        max_value: f64,
        allow_nan: bool,
        smallest_nonzero_magnitude: f64,
    ) -> f64 {
        todo!()
    }

    fn draw_bytes(&mut self, min_size: usize, max_size: usize) -> Vec<u8> {
        todo!()
    }

    fn draw_string(&mut self, intervals: IntervalSet, min_size: usize, max_size: usize) -> String {
        todo!()
    }
}

impl HypothesisProvider {
    fn maybe_draw_constant(
        &mut self,
        constraints: &ChoiceConstraints,
        p: f64,
    ) -> Option<ChoiceValue> {
        if self.random.random() > p {
            return None;
        }

        match constraints {
            ChoiceConstraints::Boolean { .. } => None,
            ChoiceConstraints::Integer { .. } => Some(ChoiceValue::Integer(
                self.random.choice(constant_integers()),
            )),
            ChoiceConstraints::Float {
                ..
            } => Some(ChoiceValue::Float(self.random.choice(constant_floats()))),
            ChoiceConstraints::Bytes { .. } => None,
            ChoiceConstraints::String {
                ..,
            } => Some(ChoiceValue::String(
                self.random.choice(constant_strings()).to_owned(),
            )),
        }
    }
    fn draw_collection(&mut self) {
        todo!()
    }
    fn draw_bounded_integer(&mut self, min: i128, max: i128, vary_size: bool) -> i128 {
        if min == max {
            return min;
        }

        let bits = bit_length(max - min);

        if bits > 24 && vary_size && self.random.random() < 0.875 {
            let idx = 
        }
    }
    fn draw_unbounded_integer(&mut self) {
        todo!()
    }
}

use std::sync::LazyLock;

fn factorial(n: u32) -> i128 {
    (1..=n as i128).product()
}

static CONSTANT_INTEGERS: LazyLock<Vec<i128>> = LazyLock::new(|| {
    let mut arr = Vec::new();

    for n in 16..66 {
        arr.push(2i128.pow(n));
    }
    for n in 5..20 {
        arr.push(10i128.pow(n));
    }
    for n in 9..21 {
        arr.push(factorial(n));
    }

    arr.extend([510510, 6469693230, 304250263527210, 32589158477190044730]);

    // Snapshot the base values so the next 3 extends don't keep reusing
    // the already-extended vector.
    let base = arr.clone();

    arr.extend(base.iter().map(|&n| n - 1));
    arr.extend(base.iter().map(|&n| n + 1));
    arr.extend(base.iter().map(|&n| -n));

    arr
});

fn constant_integers() -> &'static [i128] {
    CONSTANT_INTEGERS.as_slice()
}

static CONSTANT_FLOATS: LazyLock<Vec<f64>> = LazyLock::new(|| {
    let mut arr = Vec::new();

    arr.extend([
        0.5,
        1.1,
        1.5,
        1.9,
        1.0 / 3.0,
        10e6,
        10e-6,
        1.175494351e-38,
        next_up(0.0, 64),
        f64::MIN,
        f64::MAX,
        3.402823466e38,
        9007199254740992.0,
        1.0 - 10e-6,
        2.0 + 10e-6,
        1.192092896e-07,
        2.2204460492503131e-016,
    ]);

    // minimum (sub)normals for float16/32 and a subnormal-ish float64 case
    for &n in &[24, 14, 149, 126] {
        arr.push(2.0f64.powi(-n));
    }

    // if you want tiny positive values derived from the smallest positive normal f64,
    // use MIN_POSITIVE, not MIN.
    for &n in &[2.0, 10.0, 1000.0, 100_000.0] {
        arr.push(f64::MIN_POSITIVE / n);
    }

    let base = arr.clone();
    arr.extend(base.iter().copied().map(|x| -x));

    arr
});

fn constant_floats() -> &'static [f64] {
    CONSTANT_FLOATS.as_slice()
}

static CONSTANT_STRINGS: &[&str] = &[
    // strings which can be interpreted as code / logic
    "undefined",
    "null",
    "NULL",
    "nil",
    "NIL",
    "true",
    "false",
    "True",
    "False",
    "TRUE",
    "FALSE",
    "None",
    "none",
    "if",
    "then",
    "else",
    "__dict__",
    "__proto__", // javascript
    // strings which can be interpreted as a number
    "0",
    "1e100",
    "0..0",
    "0/0",
    "1/0",
    "+0.0",
    "Infinity",
    "-Infinity",
    "Inf",
    "INF",
    "NaN",
    "999999999999999999999999999999",
    // common ascii characters
    ",./;'[]\\-=<>?:\"{}|_+!@#$%^&*()`~",
    // common unicode characters
    "Ω≈ç√∫˜µ≤≥÷åß∂ƒ©˙∆˚¬…æœ∑´®†¥¨ˆøπ“‘¡™£¢∞§¶•ªº–≠¸˛Ç◊ı˜Â¯˘¿ÅÍÎÏ˝ÓÔÒÚÆ☃Œ„´‰ˇÁ¨ˆØ∏”’`⁄€‹›ﬁﬂ‡°·‚—±",
    // characters which increase in length when lowercased
    "Ⱥ",
    "Ⱦ",
    // ligatures
    "æœÆŒﬀʤʨß",
    // emoticons
    "(╯°□°）╯︵ ┻━┻)",
    // emojis
    "😍",
    "🇺🇸",
    // emoji modifiers
    "🏻",
    "👍🏻",
    // RTL text
    "الكل في المجمو عة",
    // Ogham text
    "᚛ᚄᚓᚐᚋᚒᚄ ᚑᚄᚂᚑᚏᚅ᚜",
    // readable variations on text
    "𝐓𝐡𝐞 𝐪𝐮𝐢𝐜𝐤 𝐛𝐫𝐨𝐰𝐧 𝐟𝐨𝐱 𝐣𝐮𝐦𝐩𝐬 𝐨𝐯𝐞𝐫 𝐭𝐡𝐞 𝐥𝐚𝐳𝐲 𝐝𝐨𝐠",
    "𝕿𝖍𝖊 𝖖𝖚𝖎𝖈𝖐 𝖇𝖗𝖔𝖜𝖓 𝖋𝖔𝖝 𝖏𝖚𝖒𝖕𝖘 𝖔𝖛𝖊𝖗 𝖙𝖍𝖊 𝖑𝖆𝖟𝖞 𝖉𝖔𝖌",
    "𝑻𝒉𝒆 𝒒𝒖𝒊𝒄𝒌 𝒃𝒓𝒐𝒘𝒏 𝒇𝒐𝒙 𝒋𝒖𝒎𝒑𝒔 𝒐𝒗𝒆𝒓 𝒕𝒉𝒆 𝒍𝒂𝒛𝒚 𝒅𝒐𝒈",
    "𝓣𝓱𝓮 𝓺𝓾𝓲𝓬𝓴 𝓫𝓻𝓸𝔀𝓷 𝓯𝓸𝔁 𝓳𝓾𝓶𝓹𝓼 𝓸𝓿𝓮𝓻 𝓽𝓱𝓮 𝓵𝓪𝔃𝔂 𝓭𝓸𝓰",
    "𝕋𝕙𝕖 𝕢𝕦𝕚𝕔𝕜 𝕓𝕣𝕠𝕨𝕟 𝕗𝕠𝕩 𝕛𝕦𝕞𝕡𝕤 𝕠𝕧𝕖𝕣 𝕥𝕙𝕖 𝕝𝕒𝕫𝕪 𝕕𝕠𝕘",
    // upside-down text
    "ʇǝɯɐ ʇᴉs ɹolop ɯnsdᴉ ɯǝɹo˥",
    // reserved strings in windows
    "NUL",
    "COM1",
    "LPT1",
    // scunthorpe problem
    "Scunthorpe",
    // zalgo text
    "Ṱ̺̺̕o͞ ̷i̲̬͇̪͙n̝̗͕v̟̜̘̦͟o̶̙̰̠kè͚̮̺̪̹̱̤ ̖t̝͕̳̣̻̪͞h̼͓̲̦̳̘̲e͇̣̰̦̬͎ ̢̼̻̱̘h͚͎͙̜̣̲ͅi̦̲̣̰̤v̻͍e̺̭̳̪̰-m̢iͅn̖̺̞̲̯̰d̵̼̟͙̩̼̘̳ ̞̥̱̳̭r̛̗̘e͙p͠r̼̞̻̭̗e̺̠̣͟s̘͇̳͍̝͉e͉̥̯̞̲͚̬͜ǹ̬͎͎̟̖͇̤t͍̬̤͓̼̭͘ͅi̪̱n͠g̴͉ ͏͉ͅc̬̟h͡a̫̻̯͘o̫̟̖͍̙̝͉s̗̦̲.̨̹͈̣",
    // examples from faultlore
    "मनीष منش",
    "पन्ह पन्ह त्र र्च कृकृ ड्ड न्हृे إلا بسم الله",
    "lorem لا بسم الله ipsum 你好1234你好",
];

fn constant_strings() -> &'static [&'static str] {
    CONSTANT_STRINGS
}

const ENV_HYP_CONSTANTS_MODE: &str = "HEGEL_HYPOTHESIS_CONSTANTS_MODE";
const ENV_HYP_GLOBAL_INTEGER_CONSTANTS: &str = "HEGEL_HYPOTHESIS_GLOBAL_INTEGER_CONSTANTS";
const ENV_HYP_LOCAL_INTEGER_CONSTANTS: &str = "HEGEL_HYPOTHESIS_LOCAL_INTEGER_CONSTANTS";
const ENV_HYP_GLOBAL_FLOAT_CONSTANTS_BITS: &str = "HEGEL_HYPOTHESIS_GLOBAL_FLOAT_CONSTANTS_BITS";
const ENV_HYP_LOCAL_FLOAT_CONSTANTS_BITS: &str = "HEGEL_HYPOTHESIS_LOCAL_FLOAT_CONSTANTS_BITS";
const ENV_HYP_GLOBAL_BYTES_CONSTANTS_HEX: &str = "HEGEL_HYPOTHESIS_GLOBAL_BYTES_CONSTANTS_HEX";
const ENV_HYP_LOCAL_BYTES_CONSTANTS_HEX: &str = "HEGEL_HYPOTHESIS_LOCAL_BYTES_CONSTANTS_HEX";
const ENV_HYP_GLOBAL_STRING_CONSTANTS_HEX: &str = "HEGEL_HYPOTHESIS_GLOBAL_STRING_CONSTANTS_HEX";
const ENV_HYP_LOCAL_STRING_CONSTANTS_HEX: &str = "HEGEL_HYPOTHESIS_LOCAL_STRING_CONSTANTS_HEX";

fn parse_i128_constants_csv(raw: &str) -> Vec<i128> {
    raw.split(',')
        .filter_map(|part| {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                None
            } else {
                trimmed.parse::<i128>().ok()
            }
        })
        .collect()
}

fn parse_string_constants_hex_csv(raw: &str) -> Vec<String> {
    raw.split(',')
        .filter_map(|part| {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                return None;
            }
            let bytes = hex_decode(trimmed)?;
            String::from_utf8(bytes).ok()
        })
        .collect()
}

fn parse_f64_bits_hex_csv(raw: &str) -> Vec<f64> {
    raw.split(',')
        .filter_map(|part| {
            let trimmed = part.trim().trim_start_matches("0x");
            if trimmed.is_empty() {
                return None;
            }
            let bits = u64::from_str_radix(trimmed, 16).ok()?;
            Some(f64::from_bits(bits))
        })
        .collect()
}

fn parse_bytes_constants_hex_csv(raw: &str) -> Vec<Vec<u8>> {
    raw.split(',')
        .filter_map(|part| {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                return None;
            }
            hex_decode(trimmed)
        })
        .collect()
}

fn sort_dedup_i128(values: &mut Vec<i128>) {
    values.sort_unstable();
    values.dedup();
}

fn sort_dedup_strings(values: &mut Vec<String>) {
    values.sort_unstable();
    values.dedup();
}

fn dedup_f64_by_bits(values: &mut Vec<f64>) {
    let mut seen = HashSet::new();
    values.retain(|value| seen.insert(value.to_bits()));
}

fn dedup_bytes(values: &mut Vec<Vec<u8>>) {
    let mut seen = HashSet::new();
    values.retain(|value| seen.insert(value.clone()));
}



fn sign_aware_lte_f64(x: f64, y: f64) -> bool {
    if x == 0.0 && y == 0.0 {
        let sx = if x.is_sign_negative() { -1i8 } else { 1i8 };
        let sy = if y.is_sign_negative() { -1i8 } else { 1i8 };
        sx <= sy
    } else {
        x <= y
    }
}

fn float_permitted_by_bounds(
    value: f64,
    min_value: f64,
    max_value: f64,
    allow_nan: bool,
    smallest_nonzero_magnitude: f64,
) -> bool {
    if value.is_nan() {
        return allow_nan;
    }
    if value != 0.0 && value.abs() < smallest_nonzero_magnitude {
        return false;
    }
    sign_aware_lte_f64(min_value, value) && sign_aware_lte_f64(value, max_value)
}

fn run_maybe_draw_float_constant(
    engine: &mut EngineState,
    min_value: f64,
    max_value: f64,
    allow_nan: bool,
    smallest_nonzero_magnitude: f64,
    p: f64,
) -> Option<f64> {
    if run_random_f64(engine) > p {
        return None;
    }

    let (global_constants, local_constants) = {
        let (global_source, local_source): (&[f64], &[f64]) =
            if let Some(run) = engine.active_run.as_ref() {
                (
                    run.constant_pools.global_float_constants.as_slice(),
                    run.constant_pools.local_float_constants.as_slice(),
                )
            } else {
                (
                    hypothesis_global_float_constants(),
                    hypothesis_local_float_constants(),
                )
            };
        let global_constants = global_source
            .iter()
            .copied()
            .filter(|v| {
                float_permitted_by_bounds(
                    *v,
                    min_value,
                    max_value,
                    allow_nan,
                    smallest_nonzero_magnitude,
                )
            })
            .collect::<Vec<_>>();
        let local_constants = local_source
            .iter()
            .copied()
            .filter(|v| {
                float_permitted_by_bounds(
                    *v,
                    min_value,
                    max_value,
                    allow_nan,
                    smallest_nonzero_magnitude,
                )
            })
            .collect::<Vec<_>>();
        (global_constants, local_constants)
    };

    let constants_lists: Vec<&Vec<f64>> = [
        (!global_constants.is_empty()).then_some(&global_constants),
        (!local_constants.is_empty()).then_some(&local_constants),
    ]
    .into_iter()
    .flatten()
    .collect();
    if constants_lists.is_empty() {
        return None;
    }

    let list_index = run_randbelow_u128(engine, constants_lists.len() as u128) as usize;
    let selected = constants_lists[list_index];
    let idx = run_randbelow_u128(engine, selected.len() as u128) as usize;
    selected.get(idx).copied()
}

fn bytes_permitted_by_bounds(value: &[u8], min_size: usize, max_size: usize) -> bool {
    value.len() >= min_size && value.len() <= max_size
}

fn run_maybe_draw_bytes_constant(
    engine: &mut EngineState,
    min_size: usize,
    max_size: usize,
    p: f64,
) -> Option<Vec<u8>> {
    if run_random_f64(engine) > p {
        return None;
    }

    let (global_constants, local_constants) = {
        let (global_source, local_source): (&[Vec<u8>], &[Vec<u8>]) =
            if let Some(run) = engine.active_run.as_ref() {
                (
                    run.constant_pools.global_bytes_constants.as_slice(),
                    run.constant_pools.local_bytes_constants.as_slice(),
                )
            } else {
                (
                    hypothesis_global_bytes_constants(),
                    hypothesis_local_bytes_constants(),
                )
            };
        let global_constants = global_source
            .iter()
            .filter(|value| bytes_permitted_by_bounds(value, min_size, max_size))
            .cloned()
            .collect::<Vec<_>>();
        let local_constants = local_source
            .iter()
            .filter(|value| bytes_permitted_by_bounds(value, min_size, max_size))
            .cloned()
            .collect::<Vec<_>>();
        (global_constants, local_constants)
    };

    let constants_lists: Vec<&Vec<Vec<u8>>> = [
        (!global_constants.is_empty()).then_some(&global_constants),
        (!local_constants.is_empty()).then_some(&local_constants),
    ]
    .into_iter()
    .flatten()
    .collect();
    if constants_lists.is_empty() {
        return None;
    }

    let list_index = run_randbelow_u128(engine, constants_lists.len() as u128) as usize;
    let selected = constants_lists[list_index];
    let idx = run_randbelow_u128(engine, selected.len() as u128) as usize;
    selected.get(idx).cloned()
}

fn run_sample_hypothesis_int_size_index(engine: &mut EngineState) -> usize {
    let row = run_draw_integer_hypothesis_choice(
        engine,
        Some(0),
        Some((HYPOTHESIS_INT_SIZE_SAMPLER_TABLE.len() - 1) as i128),
    ) as usize;
    let (base, alternate, alternate_chance) = HYPOTHESIS_INT_SIZE_SAMPLER_TABLE[row];
    if alternate_chance > 0.0 && run_random_f64(engine) < alternate_chance {
        alternate
    } else {
        base
    }
}

fn run_draw_unbounded_integer_hypothesis(engine: &mut EngineState) -> i128 {
    let size = HYPOTHESIS_INT_SIZES[run_sample_hypothesis_int_size_index(engine)];
    let mut r = run_getrandbits_u128(engine, size);
    let sign = (r & 1) != 0;
    r >>= 1;
    let mut value = r as i128;
    if sign {
        value = -value;
    }
    value
}

fn run_draw_bounded_integer_hypothesis(
    engine: &mut EngineState,
    lower: i128,
    upper: i128,
    vary_size: bool,
) -> i128 {
    assert!(lower <= upper, "lower must be <= upper");
    if lower == upper {
        return lower;
    }

    let span = (upper as u128).wrapping_sub(lower as u128);
    let bits = bit_length_u128(span);
    if bits > 24 && vary_size && run_random_f64(engine) < (7.0 / 8.0) {
        let idx = run_sample_hypothesis_int_size_index(engine);
        let cap_bits = bits.min(HYPOTHESIS_INT_SIZES[idx]);
        let cap_span = if cap_bits >= 128 {
            u128::MAX
        } else {
            (1u128 << cap_bits).saturating_sub(1)
        };
        let capped_upper = if cap_span >= span {
            upper
        } else {
            ((lower as u128).wrapping_add(cap_span)) as i128
        };
        return run_randint_i128(engine, lower, capped_upper);
    }

    run_randint_i128(engine, lower, upper)
}

fn draw_integer_from_case(
    case: &mut CaseBufferState,
    min: i128,
    max: i128,
    _shrink_towards: i128,
    _derandomized_small_spans: bool,
) -> i128 {
    draw_integer_uniform_from_case(case, min, max)
}

fn draw_unobserved_randint(
    engine: &mut EngineState,
    case_id: CaseId,
    lower: i128,
    upper: i128,
) -> i128 {
    assert!(lower <= upper, "lower must be <= upper");
    if lower == upper {
        return lower;
    }
    if engine.active_run.is_some() {
        // Keep byte-buffer progression stable for replay/shrinking bookkeeping.
        let case = case_state_mut(engine, case_id);
        let _ = draw_integer_from_case(case, lower, upper, 0, false);
        run_randint_i128(engine, lower, upper)
    } else {
        let case = case_state_mut(engine, case_id);
        draw_integer_from_case(case, lower, upper, 0, false)
    }
}

fn draw_unobserved_byte(engine: &mut EngineState, case_id: CaseId) -> u8 {
    if engine.active_run.is_some() {
        // Keep byte-buffer progression stable for replay/shrinking bookkeeping.
        let case = case_state_mut(engine, case_id);
        let _ = draw_bits_from_case(case, 8);
        run_getrandbits_u128(engine, 8) as u8
    } else {
        let case = case_state_mut(engine, case_id);
        draw_bits_from_case(case, 8) as u8
    }
}

fn run_draw_integer_hypothesis_choice(
    engine: &mut EngineState,
    min_value: Option<i128>,
    max_value: Option<i128>,
) -> i128 {
    if let Some(constant) = run_maybe_draw_integer_constant(engine, min_value, max_value, 0.05) {
        return constant;
    }

    let mut center = 0_i128;
    if let Some(min_bound) = min_value {
        center = center.max(min_bound);
    }
    if let Some(max_bound) = max_value {
        center = center.min(max_bound);
    }

    match (min_value, max_value) {
        (None, None) => run_draw_unbounded_integer_hypothesis(engine),
        (None, Some(max_bound)) => {
            let mut probe = max_bound.saturating_add(1);
            while max_bound < probe {
                probe = center.saturating_add(run_draw_unbounded_integer_hypothesis(engine));
            }
            probe
        }
        (Some(min_bound), None) => {
            let mut probe = min_bound.saturating_sub(1);
            while probe < min_bound {
                probe = center.saturating_add(run_draw_unbounded_integer_hypothesis(engine));
            }
            probe
        }
        (Some(min_bound), Some(max_bound)) => {
            run_draw_bounded_integer_hypothesis(engine, min_bound, max_bound, true)
        }
    }
}

pub(crate) fn draw_integer_choice(
    engine: &mut EngineState,
    case_id: CaseId,
    min_value: Option<i128>,
    max_value: Option<i128>,
    shrink_towards: i128,
    observe: bool,
) -> i128 {
    let (min, max) = normalize_integer_bounds(min_value, max_value);
    assert!(min <= max, "min_value cannot be greater than max_value");
    let derandomized_run = engine
        .active_run
        .as_ref()
        .map(|run| run.settings.derandomize)
        .unwrap_or(false);

    let forced = {
        let case = case_state_mut(engine, case_id);
        if observe {
            take_forced_choice(case, ChoiceKind::Integer)
        } else {
            None
        }
    };
    let prefix_choice = {
        let case = case_state_mut(engine, case_id);
        if observe && forced.is_none() {
            pop_prefix_choice(case)
        } else {
            None
        }
    };
    let simplest_value = clamped_shrink_towards(min_value, max_value, shrink_towards);
    let value = if let Some(ChoiceValue::Integer(v)) = forced.as_ref() {
        if engine.active_run.is_some() {
            let case = case_state_mut(engine, case_id);
            let _ =
                draw_integer_from_case(case, min, max, shrink_towards, observe && derandomized_run);
        }
        if *v < min || *v > max {
            let case = case_state_mut(engine, case_id);
            case.exhausted = true;
            stop_test_now();
        }
        *v
    } else if let Some(choice) = prefix_choice {
        if engine.active_run.is_some() {
            let case = case_state_mut(engine, case_id);
            let _ =
                draw_integer_from_case(case, min, max, shrink_towards, observe && derandomized_run);
        }
        match choice {
            ChoiceValue::Integer(v) if v >= min && v <= max => v,
            _ => simplest_value,
        }
    } else if should_use_simplest_observed_draws(engine, observe) {
        if engine.active_run.is_some() {
            let case = case_state_mut(engine, case_id);
            let _ =
                draw_integer_from_case(case, min, max, shrink_towards, observe && derandomized_run);
        }
        simplest_value
    } else {
        if engine.active_run.is_some() {
            // Keep byte-buffer progression stable for replay/shrinking bookkeeping.
            let case = case_state_mut(engine, case_id);
            let _ =
                draw_integer_from_case(case, min, max, shrink_towards, observe && derandomized_run);
            run_draw_integer_hypothesis_choice(engine, min_value, max_value)
        } else {
            let case = case_state_mut(engine, case_id);
            draw_integer_from_case(case, min, max, shrink_towards, observe && derandomized_run)
        }
    };

    let case = case_state_mut(engine, case_id);
    record_choice_or_stop(
        case,
        ChoiceValue::Integer(value),
        ChoiceConstraints::Integer {
            min_value: Some(min),
            max_value: Some(max),
            shrink_towards,
        },
        forced.is_some(),
        observe,
    );
    value
}

fn clamp_float_for_constraints(
    mut value: f64,
    min_value: f64,
    max_value: f64,
    allow_nan: bool,
    smallest_nonzero_magnitude: f64,
) -> f64 {
    if value.is_nan() {
        if allow_nan {
            return value;
        }
        value = 0.0;
    }

    if value != 0.0 && value.abs() < smallest_nonzero_magnitude {
        value = value.signum() * smallest_nonzero_magnitude;
    }
    if value < min_value {
        value = min_value;
    }
    if value > max_value {
        value = max_value;
    }
    value
}

fn finite_float_in_bounds(value: f64, min_value: f64, max_value: f64) -> bool {
    value.is_finite() && value >= min_value && value <= max_value
}

fn push_interesting_float(candidates: &mut Vec<f64>, value: f64, min_value: f64, max_value: f64) {
    let allowed = if value.is_nan() {
        true
    } else if value.is_infinite() {
        value >= min_value && value <= max_value
    } else {
        finite_float_in_bounds(value, min_value, max_value)
    };
    if allowed && !candidates.iter().any(|v| v.to_bits() == value.to_bits()) {
        candidates.push(value);
    }
}

fn interesting_float_candidates(
    min_value: f64,
    max_value: f64,
    allow_nan: bool,
    smallest_nonzero_magnitude: f64,
) -> Vec<f64> {
    let mut interesting = Vec::new();
    for value in [0.0, -0.0, 1.0, -1.0, 2.0, -2.0, 0.5, -0.5] {
        push_interesting_float(&mut interesting, value, min_value, max_value);
    }
    for value in [
        3.0,
        4.0,
        8.0,
        16.0,
        32.0,
        64.0,
        100.0,
        128.0,
        255.0,
        256.0,
        511.0,
        512.0,
        1000.0,
        10_000.0,
        1_000_000.0,
        f64::MAX / 2.0,
    ] {
        push_interesting_float(&mut interesting, value, min_value, max_value);
        push_interesting_float(&mut interesting, -value, min_value, max_value);
    }
    if min_value.is_finite() && max_value.is_finite() {
        let midpoint = (min_value + max_value) / 2.0;
        if min_value > 1.0 && max_value < 2.0 {
            // Prefer "simple" dyadics in narrow positive ranges.
            push_interesting_float(&mut interesting, 1.5, min_value, max_value);
            push_interesting_float(&mut interesting, 1.25, min_value, max_value);
            push_interesting_float(&mut interesting, 1.75, min_value, max_value);
        }
        push_interesting_float(&mut interesting, midpoint, min_value, max_value);
    }
    if min_value.is_finite() {
        push_interesting_float(&mut interesting, min_value, min_value, max_value);
        push_interesting_float(&mut interesting, min_value.next_up(), min_value, max_value);
    }
    if max_value.is_finite() {
        push_interesting_float(&mut interesting, max_value, min_value, max_value);
        push_interesting_float(
            &mut interesting,
            max_value.next_down(),
            min_value,
            max_value,
        );
    }
    if allow_nan {
        push_interesting_float(&mut interesting, f64::NAN, min_value, max_value);
    }
    if smallest_nonzero_magnitude.is_finite() && smallest_nonzero_magnitude > 0.0 {
        push_interesting_float(
            &mut interesting,
            smallest_nonzero_magnitude,
            min_value,
            max_value,
        );
        push_interesting_float(
            &mut interesting,
            -smallest_nonzero_magnitude,
            min_value,
            max_value,
        );
    }
    // Put infinities late so one-sided finite thresholds shrink before +/-inf.
    push_interesting_float(&mut interesting, f64::INFINITY, min_value, max_value);
    push_interesting_float(&mut interesting, f64::NEG_INFINITY, min_value, max_value);
    interesting
}

fn value_as_i128(value: &Value) -> Option<i128> {
    match value {
        Value::Integer(i) => Some((*i).into()),
        _ => None,
    }
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Float(f) => Some(*f),
        Value::Integer(i) => Some(i128::from(*i) as f64),
        _ => None,
    }
}

fn decode_u128_be(bytes: &[u8]) -> Option<u128> {
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }
    Some(
        bytes
            .iter()
            .fold(0u128, |acc, byte| (acc << 8) | u128::from(*byte)),
    )
}

fn value_as_u128(value: &Value) -> Option<u128> {
    match value {
        Value::Integer(i) => {
            let n: i128 = (*i).into();
            if n < 0 { None } else { Some(n as u128) }
        }
        Value::Tag(2, inner) => match inner.as_ref() {
            Value::Bytes(bytes) => decode_u128_be(bytes),
            _ => None,
        },
        _ => None,
    }
}

fn value_as_usize(value: &Value) -> Option<usize> {
    value_as_i128(value).and_then(|n| usize::try_from(n).ok())
}

fn map_i128(schema: &Value, key: &str) -> Option<i128> {
    crate::cbor_utils::map_get(schema, key).and_then(value_as_i128)
}

fn map_f64(schema: &Value, key: &str) -> Option<f64> {
    crate::cbor_utils::map_get(schema, key).and_then(value_as_f64)
}

fn map_u64(schema: &Value, key: &str) -> Option<u64> {
    crate::cbor_utils::map_get(schema, key).and_then(crate::cbor_utils::as_u64)
}

fn map_usize(schema: &Value, key: &str) -> Option<usize> {
    crate::cbor_utils::map_get(schema, key).and_then(value_as_usize)
}

fn map_bool(schema: &Value, key: &str) -> Option<bool> {
    crate::cbor_utils::map_get(schema, key).and_then(crate::cbor_utils::as_bool)
}

fn map_text<'a>(schema: &'a Value, key: &str) -> Option<&'a str> {
    crate::cbor_utils::map_get(schema, key).and_then(crate::cbor_utils::as_text)
}

fn map_text_array(schema: &Value, key: &str) -> Option<Vec<String>> {
    let raw = crate::cbor_utils::map_get(schema, key)?;
    let Value::Array(values) = raw else {
        panic!("schema field `{}` must be an array of text", key);
    };
    Some(
        values
            .iter()
            .map(|value| {
                crate::cbor_utils::as_text(value)
                    .unwrap_or_else(|| panic!("schema field `{}` must contain only text", key))
                    .to_string()
            })
            .collect(),
    )
}

fn validate_string_codec(codec: &str) {
    let normalized = codec.to_ascii_lowercase();
    let supported = matches!(
        normalized.as_str(),
        "ascii" | "utf-8" | "utf8" | "latin-1" | "latin1" | "iso-8859-1"
    );
    assert!(supported, "Unsupported codec: {}", codec);
}

fn float_next_up_for_width(value: f64, width: u64) -> f64 {
    if width == 32 {
        (value as f32).next_up() as f64
    } else {
        value.next_up()
    }
}

fn float_next_down_for_width(value: f64, width: u64) -> f64 {
    if width == 32 {
        (value as f32).next_down() as f64
    } else {
        value.next_down()
    }
}

fn parse_collection_bounds(schema: &Value, default_max_delta: usize) -> (usize, usize) {
    let min_size = map_usize(schema, "min_size").unwrap_or(0);
    let max_size = map_usize(schema, "max_size")
        .unwrap_or_else(|| min_size.saturating_add(default_max_delta).max(min_size));
    assert!(min_size <= max_size, "Cannot have max_size < min_size");
    (min_size, max_size)
}

fn p_continue_to_avg(p_continue: f64, max_size: f64) -> f64 {
    if p_continue >= 1.0 {
        return max_size;
    }
    (1.0 / (1.0 - p_continue) - 1.0) * (1.0 - p_continue.powf(max_size))
}

fn calc_p_continue(desired_avg: f64, max_size: Option<usize>) -> f64 {
    if desired_avg <= 0.0 {
        return 0.0;
    }
    let mut p_continue = 1.0 - 1.0 / (1.0 + desired_avg);
    if let Some(max) = max_size {
        let max_f = max as f64;
        if p_continue > 0.0 {
            while p_continue_to_avg(p_continue, max_f) > desired_avg {
                p_continue -= 0.0001;
                if p_continue <= f64::MIN_POSITIVE {
                    p_continue = f64::MIN_POSITIVE;
                    break;
                }
            }
            let mut hi = 1.0;
            while desired_avg - p_continue_to_avg(p_continue, max_f) > 0.01 {
                let mid = (p_continue + hi) / 2.0;
                if p_continue_to_avg(mid, max_f) <= desired_avg {
                    p_continue = mid;
                } else {
                    hi = mid;
                }
            }
        }
    }
    p_continue.clamp(0.0, 1.0 - f64::EPSILON)
}

fn default_average_size(min_size: usize, max_size: Option<usize>) -> f64 {
    let doubled = (min_size.saturating_mul(2)).max(min_size.saturating_add(5)) as f64;
    match max_size {
        // Avoid usize overflow for huge max bounds (e.g. drawn usize::MAX).
        Some(max) => doubled.min(0.5 * (min_size as f64 + max as f64)),
        None => doubled,
    }
}

fn value_fingerprint(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    ciborium::into_writer(value, &mut out).expect("CBOR encode for fingerprint failed");
    out
}

#[derive(Debug, Clone)]
struct StringCharConstraints {
    min_codepoint: u32,
    max_codepoint: u32,
    codec: Option<String>,
    categories: Option<Vec<String>>,
    exclude_categories: Vec<String>,
    include_characters: Vec<char>,
    exclude_characters: HashSet<char>,
}

fn ordered_unique_chars(s: &str) -> Vec<char> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for c in s.chars() {
        if seen.insert(c) {
            out.push(c);
        }
    }
    out
}

fn string_permitted_by_bounds(
    value: &str,
    min_size: usize,
    max_size: usize,
    alphabet: Option<&[char]>,
) -> bool {
    let len = value.chars().count();
    if len < min_size || len > max_size {
        return false;
    }
    if let Some(allowed) = alphabet {
        value.chars().all(|ch| allowed.contains(&ch))
    } else {
        true
    }
}

fn run_maybe_draw_string_constant(
    engine: &mut EngineState,
    min_size: usize,
    max_size: usize,
    alphabet: Option<&[char]>,
    p: f64,
) -> Option<String> {
    if run_random_f64(engine) > p {
        return None;
    }

    let (global_constants, local_constants) = {
        let (global_source, local_source): (&[String], &[String]) =
            if let Some(run) = engine.active_run.as_ref() {
                (
                    run.constant_pools.global_string_constants.as_slice(),
                    run.constant_pools.local_string_constants.as_slice(),
                )
            } else {
                (
                    hypothesis_global_string_constants(),
                    hypothesis_local_string_constants(),
                )
            };
        let global_constants = global_source
            .iter()
            .filter(|value| string_permitted_by_bounds(value, min_size, max_size, alphabet))
            .cloned()
            .collect::<Vec<_>>();
        let local_constants = local_source
            .iter()
            .filter(|value| string_permitted_by_bounds(value, min_size, max_size, alphabet))
            .cloned()
            .collect::<Vec<_>>();
        (global_constants, local_constants)
    };
    let constants_lists: Vec<&Vec<String>> = [
        (!global_constants.is_empty()).then_some(&global_constants),
        (!local_constants.is_empty()).then_some(&local_constants),
    ]
    .into_iter()
    .flatten()
    .collect();
    if constants_lists.is_empty() {
        return None;
    }

    let list_index = run_randbelow_u128(engine, constants_lists.len() as u128) as usize;
    let selected = constants_lists[list_index];
    let idx = run_randbelow_u128(engine, selected.len() as u128) as usize;
    selected.get(idx).cloned()
}

fn parse_string_char_constraints(schema: &Value) -> StringCharConstraints {
    let codec = map_text(schema, "codec").map(str::to_string);
    if let Some(codec) = codec.as_deref() {
        validate_string_codec(codec);
    }

    let min_codepoint = map_i128(schema, "min_codepoint")
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(0);
    let mut max_codepoint = map_i128(schema, "max_codepoint")
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(0x10FFFF);

    if let Some(codec) = codec.as_deref() {
        match codec.to_ascii_lowercase().as_str() {
            "ascii" => {
                max_codepoint = max_codepoint.min(0x7F);
            }
            "latin-1" | "latin1" | "iso-8859-1" => {
                max_codepoint = max_codepoint.min(0xFF);
            }
            "utf-8" | "utf8" => {}
            _ => unreachable!("codec validity is asserted above"),
        }
    }

    assert!(
        min_codepoint <= max_codepoint,
        "Cannot have max_codepoint < min_codepoint"
    );

    let categories = map_text_array(schema, "categories");
    let exclude_categories = map_text_array(schema, "exclude_categories").unwrap_or_default();
    let include_characters =
        ordered_unique_chars(map_text(schema, "include_characters").unwrap_or(""));
    let exclude_characters = map_text(schema, "exclude_characters")
        .unwrap_or("")
        .chars()
        .collect();

    StringCharConstraints {
        min_codepoint,
        max_codepoint,
        codec,
        categories,
        exclude_categories,
        include_characters,
        exclude_characters,
    }
}

fn category_matches(c: char, category: &str) -> bool {
    match category {
        "L" => c.is_alphabetic(),
        "Lu" => c.is_uppercase(),
        "Ll" => c.is_lowercase(),
        "N" => c.is_numeric(),
        "Nd" => c.to_digit(10).is_some(),
        "Cc" => c.is_control(),
        // Rust `char` cannot represent surrogates.
        "Cs" => false,
        // Approximation: we only represent `Cc` at runtime.
        "C" => c.is_control(),
        other => panic!("Unsupported Unicode category: {}", other),
    }
}

fn codec_allows_char(c: char, codec: Option<&str>) -> bool {
    match codec.map(|v| v.to_ascii_lowercase()) {
        Some(value) if value == "ascii" => c.is_ascii(),
        Some(value) if value == "latin-1" || value == "latin1" || value == "iso-8859-1" => {
            (c as u32) <= 0xFF
        }
        Some(value) if value == "utf-8" || value == "utf8" => true,
        Some(_) => false,
        None => true,
    }
}

fn char_allowed_by_constraints(c: char, constraints: &StringCharConstraints) -> bool {
    if constraints.exclude_characters.contains(&c) {
        return false;
    }
    if constraints.include_characters.contains(&c) {
        return true;
    }

    let cp = c as u32;
    if cp < constraints.min_codepoint || cp > constraints.max_codepoint {
        return false;
    }
    if !codec_allows_char(c, constraints.codec.as_deref()) {
        return false;
    }
    if let Some(categories) = constraints.categories.as_ref()
        && (categories.is_empty() || !categories.iter().any(|cat| category_matches(c, cat)))
    {
        return false;
    }
    if constraints
        .exclude_categories
        .iter()
        .any(|cat| category_matches(c, cat))
    {
        return false;
    }

    true
}

pub(crate) fn draw_bytes_choice(
    engine: &mut EngineState,
    case_id: CaseId,
    min_size: usize,
    max_size: usize,
    observe: bool,
) -> Vec<u8> {
    let case = case_state_mut(engine, case_id);
    if observe && let Some(ChoiceValue::Bytes(bytes)) = take_forced_choice(case, ChoiceKind::Bytes)
    {
        if bytes.len() < min_size || bytes.len() > max_size {
            case.exhausted = true;
            stop_test_now();
        }
        record_choice_or_stop(
            case,
            ChoiceValue::Bytes(bytes.clone()),
            ChoiceConstraints::Bytes { min_size, max_size },
            true,
            observe,
        );
        return bytes;
    }
    let prefix_choice = {
        let case = case_state_mut(engine, case_id);
        if observe {
            pop_prefix_choice(case)
        } else {
            None
        }
    };
    if let Some(choice) = prefix_choice {
        let simplest = vec![0u8; min_size];
        let bytes = match choice {
            ChoiceValue::Bytes(bytes) if bytes.len() >= min_size && bytes.len() <= max_size => {
                bytes
            }
            _ => simplest,
        };
        let case = case_state_mut(engine, case_id);
        record_choice_or_stop(
            case,
            ChoiceValue::Bytes(bytes.clone()),
            ChoiceConstraints::Bytes { min_size, max_size },
            false,
            observe,
        );
        return bytes;
    }

    if should_use_simplest_observed_draws(engine, observe) {
        let bytes = vec![0u8; min_size];
        let case = case_state_mut(engine, case_id);
        record_choice_or_stop(
            case,
            ChoiceValue::Bytes(bytes.clone()),
            ChoiceConstraints::Bytes { min_size, max_size },
            false,
            observe,
        );
        return bytes;
    }

    if engine.active_run.is_some() {
        // HypothesisProvider._maybe_draw_constant("bytes", p=0.05) check.
        // We currently model no bytes constants, but still consume the RNG check.
        let _ = run_random_f64(engine) <= 0.05;
    }

    let average_size = default_average_size(min_size, Some(max_size));
    let desired_avg = (average_size - min_size as f64).max(0.0);
    let max_delta = Some(max_size.saturating_sub(min_size));
    let p_continue = calc_p_continue(desired_avg, max_delta);

    let mut bytes = Vec::new();
    let mut count = 0usize;
    loop {
        let forced_result = if count < min_size {
            Some(true)
        } else if count >= max_size {
            Some(false)
        } else {
            None
        };
        let should_continue =
            draw_boolean_choice_with_forced(engine, case_id, p_continue, forced_result, false);
        if !should_continue {
            break;
        }
        bytes.push(draw_unobserved_byte(engine, case_id));
        count = count.saturating_add(1);
    }

    let case = case_state_mut(engine, case_id);
    record_choice_or_stop(
        case,
        ChoiceValue::Bytes(bytes.clone()),
        ChoiceConstraints::Bytes { min_size, max_size },
        false,
        observe,
    );
    bytes
}

fn string_alphabet_from_schema(schema: &Value) -> Vec<char> {
    if let Some(include) = map_text(schema, "include_characters") {
        let chars: Vec<char> = include.chars().collect();
        if !chars.is_empty() {
            return chars;
        }
    }
    (32u8..=126u8).map(char::from).collect()
}

fn draw_unobserved_string_from_alphabet(
    engine: &mut EngineState,
    case_id: CaseId,
    min_size: usize,
    max_size: usize,
    alphabet: &[char],
) -> String {
    assert!(!alphabet.is_empty(), "string alphabet cannot be empty");

    let average_size = default_average_size(min_size, Some(max_size));
    let desired_avg = (average_size - min_size as f64).max(0.0);
    let max_delta = Some(max_size.saturating_sub(min_size));
    let p_continue = calc_p_continue(desired_avg, max_delta);

    let mut out = String::new();
    let mut count = 0usize;
    loop {
        let forced_result = if count < min_size {
            Some(true)
        } else if count >= max_size {
            Some(false)
        } else {
            None
        };
        let should_continue =
            draw_boolean_choice_with_forced(engine, case_id, p_continue, forced_result, false);
        if !should_continue {
            break;
        }
        let idx = draw_alphabet_index_hypothesis(engine, case_id, alphabet.len());
        out.push(alphabet[idx]);
        count = count.saturating_add(1);
    }
    out
}

fn draw_collection_size_unobserved(
    engine: &mut EngineState,
    case_id: CaseId,
    min_size: usize,
    max_size: usize,
) -> usize {
    if min_size == max_size {
        return min_size;
    }
    let average_size = default_average_size(min_size, Some(max_size));
    let desired_avg = (average_size - min_size as f64).max(0.0);
    let max_delta = Some(max_size.saturating_sub(min_size));
    let p_continue = calc_p_continue(desired_avg, max_delta);

    let mut count = 0usize;
    loop {
        let forced_result = if count < min_size {
            Some(true)
        } else if count >= max_size {
            Some(false)
        } else {
            None
        };
        let should_continue =
            draw_boolean_choice_with_forced(engine, case_id, p_continue, forced_result, false);
        if should_continue {
            count = count.saturating_add(1);
        } else {
            return count;
        }
    }
}

fn draw_alphabet_index_hypothesis(
    engine: &mut EngineState,
    case_id: CaseId,
    alphabet_len: usize,
) -> usize {
    assert!(alphabet_len > 0, "alphabet cannot be empty");
    if alphabet_len == 1 {
        return 0;
    }
    if alphabet_len > 256 {
        let use_tail = draw_boolean_choice(engine, case_id, 0.2, false);
        if use_tail {
            return draw_unobserved_randint(engine, case_id, 256, (alphabet_len - 1) as i128)
                as usize;
        }
        return draw_unobserved_randint(engine, case_id, 0, 255) as usize;
    }
    draw_unobserved_randint(engine, case_id, 0, (alphabet_len - 1) as i128) as usize
}

pub(crate) fn draw_string_choice(
    engine: &mut EngineState,
    case_id: CaseId,
    min_size: usize,
    max_size: usize,
    alphabet: &[char],
    observe: bool,
) -> String {
    let case = case_state_mut(engine, case_id);
    if observe
        && let Some(ChoiceValue::String(value)) = take_forced_choice(case, ChoiceKind::String)
    {
        let len = value.chars().count();
        if len < min_size || len > max_size || !value.chars().all(|ch| alphabet.contains(&ch)) {
            case.exhausted = true;
            stop_test_now();
        }
        record_choice_or_stop(
            case,
            ChoiceValue::String(value.clone()),
            ChoiceConstraints::String {
                min_size,
                max_size,
                alphabet: Some(alphabet.to_vec()),
            },
            true,
            observe,
        );
        return value;
    }
    let prefix_choice = {
        let case = case_state_mut(engine, case_id);
        if observe {
            pop_prefix_choice(case)
        } else {
            None
        }
    };
    if let Some(choice) = prefix_choice {
        let default_char = *alphabet
            .first()
            .expect("string alphabet cannot be empty for observed draw");
        let mut simplest = String::with_capacity(min_size);
        for _ in 0..min_size {
            simplest.push(default_char);
        }
        let out = match choice {
            ChoiceValue::String(value)
                if value.chars().count() >= min_size
                    && value.chars().count() <= max_size
                    && value.chars().all(|ch| alphabet.contains(&ch)) =>
            {
                value
            }
            _ => simplest,
        };
        let case = case_state_mut(engine, case_id);
        record_choice_or_stop(
            case,
            ChoiceValue::String(out.clone()),
            ChoiceConstraints::String {
                min_size,
                max_size,
                alphabet: Some(alphabet.to_vec()),
            },
            false,
            observe,
        );
        return out;
    }

    if should_use_simplest_observed_draws(engine, observe) {
        let mut out = String::with_capacity(min_size);
        let default_char = *alphabet
            .first()
            .expect("string alphabet cannot be empty for observed draw");
        for _ in 0..min_size {
            out.push(default_char);
        }

        let case = case_state_mut(engine, case_id);
        record_choice_or_stop(
            case,
            ChoiceValue::String(out.clone()),
            ChoiceConstraints::String {
                min_size,
                max_size,
                alphabet: Some(alphabet.to_vec()),
            },
            false,
            observe,
        );
        return out;
    }

    let out = if engine.active_run.is_some() {
        run_maybe_draw_string_constant(engine, min_size, max_size, Some(alphabet), 0.05)
            .unwrap_or_else(|| {
                draw_unobserved_string_from_alphabet(engine, case_id, min_size, max_size, alphabet)
            })
    } else {
        draw_unobserved_string_from_alphabet(engine, case_id, min_size, max_size, alphabet)
    };

    let case = case_state_mut(engine, case_id);
    record_choice_or_stop(
        case,
        ChoiceValue::String(out.clone()),
        ChoiceConstraints::String {
            min_size,
            max_size,
            alphabet: Some(alphabet.to_vec()),
        },
        false,
        observe,
    );
    out
}

fn finite_alphabet_for_string_constraints(
    constraints: &StringCharConstraints,
) -> Option<Vec<char>> {
    if constraints
        .categories
        .as_ref()
        .is_some_and(|categories| !categories.is_empty())
    {
        return None;
    }
    let mut alphabet = Vec::new();
    for ch in constraints
        .include_characters
        .iter()
        .copied()
        .filter(|ch| !constraints.exclude_characters.contains(ch))
    {
        if !alphabet.contains(&ch) {
            alphabet.push(ch);
        }
    }
    if alphabet.is_empty() {
        None
    } else {
        Some(alphabet)
    }
}

fn draw_char_with_constraints(
    engine: &mut EngineState,
    case_id: CaseId,
    constraints: &StringCharConstraints,
) -> char {
    if let Some(alphabet) = finite_alphabet_for_string_constraints(constraints) {
        let idx = draw_alphabet_index_hypothesis(engine, case_id, alphabet.len());
        return alphabet[idx];
    }

    loop {
        let codepoint = if constraints.min_codepoint == constraints.max_codepoint {
            constraints.min_codepoint
        } else {
            draw_unobserved_randint(
                engine,
                case_id,
                constraints.min_codepoint as i128,
                constraints.max_codepoint as i128,
            ) as u32
        };
        let Some(c) = char::from_u32(codepoint) else {
            continue;
        };
        if char_allowed_by_constraints(c, constraints) {
            return c;
        }
    }
}

fn simplest_char_with_constraints(constraints: &StringCharConstraints) -> Option<char> {
    if let Some(alphabet) = finite_alphabet_for_string_constraints(constraints) {
        return alphabet.first().copied();
    }

    for codepoint in constraints.min_codepoint..=constraints.max_codepoint {
        let Some(c) = char::from_u32(codepoint) else {
            continue;
        };
        if char_allowed_by_constraints(c, constraints) {
            return Some(c);
        }
    }
    None
}

fn draw_string_choice_with_constraints(
    engine: &mut EngineState,
    case_id: CaseId,
    min_size: usize,
    max_size: usize,
    constraints: &StringCharConstraints,
    observe: bool,
) -> String {
    let case = case_state_mut(engine, case_id);
    if observe
        && let Some(ChoiceValue::String(value)) = take_forced_choice(case, ChoiceKind::String)
    {
        let len = value.chars().count();
        if len < min_size
            || len > max_size
            || !value
                .chars()
                .all(|ch| char_allowed_by_constraints(ch, constraints))
        {
            case.exhausted = true;
            stop_test_now();
        }
        record_choice_or_stop(
            case,
            ChoiceValue::String(value.clone()),
            ChoiceConstraints::String {
                min_size,
                max_size,
                alphabet: finite_alphabet_for_string_constraints(constraints),
            },
            true,
            observe,
        );
        return value;
    }
    let prefix_choice = {
        let case = case_state_mut(engine, case_id);
        if observe {
            pop_prefix_choice(case)
        } else {
            None
        }
    };
    if let Some(choice) = prefix_choice {
        let fill = simplest_char_with_constraints(constraints).unwrap_or_else(|| stop_test_now());
        let mut simplest = String::with_capacity(min_size);
        for _ in 0..min_size {
            simplest.push(fill);
        }
        let out = match choice {
            ChoiceValue::String(value)
                if value.chars().count() >= min_size
                    && value.chars().count() <= max_size
                    && value
                        .chars()
                        .all(|ch| char_allowed_by_constraints(ch, constraints)) =>
            {
                value
            }
            _ => simplest,
        };
        let case = case_state_mut(engine, case_id);
        record_choice_or_stop(
            case,
            ChoiceValue::String(out.clone()),
            ChoiceConstraints::String {
                min_size,
                max_size,
                alphabet: finite_alphabet_for_string_constraints(constraints),
            },
            false,
            observe,
        );
        return out;
    }

    if should_use_simplest_observed_draws(engine, observe) {
        let fill = simplest_char_with_constraints(constraints).unwrap_or_else(|| stop_test_now());
        let mut out = String::with_capacity(min_size);
        for _ in 0..min_size {
            out.push(fill);
        }
        let case = case_state_mut(engine, case_id);
        record_choice_or_stop(
            case,
            ChoiceValue::String(out.clone()),
            ChoiceConstraints::String {
                min_size,
                max_size,
                alphabet: finite_alphabet_for_string_constraints(constraints),
            },
            false,
            observe,
        );
        return out;
    }

    if engine.active_run.is_some() {
        let finite_alphabet = finite_alphabet_for_string_constraints(constraints);
        if let Some(out) = run_maybe_draw_string_constant(
            engine,
            min_size,
            max_size,
            finite_alphabet.as_deref(),
            0.05,
        ) {
            let case = case_state_mut(engine, case_id);
            record_choice_or_stop(
                case,
                ChoiceValue::String(out.clone()),
                ChoiceConstraints::String {
                    min_size,
                    max_size,
                    alphabet: finite_alphabet_for_string_constraints(constraints),
                },
                false,
                observe,
            );
            return out;
        }
    }

    let average_size = default_average_size(min_size, Some(max_size));
    let desired_avg = (average_size - min_size as f64).max(0.0);
    let max_delta = Some(max_size.saturating_sub(min_size));
    let p_continue = calc_p_continue(desired_avg, max_delta);

    let mut out = String::new();
    let mut count = 0usize;
    loop {
        let forced_result = if count < min_size {
            Some(true)
        } else if count >= max_size {
            Some(false)
        } else {
            None
        };
        let should_continue =
            draw_boolean_choice_with_forced(engine, case_id, p_continue, forced_result, false);
        if !should_continue {
            break;
        }
        out.push(draw_char_with_constraints(engine, case_id, constraints));
        count = count.saturating_add(1);
    }

    let case = case_state_mut(engine, case_id);
    record_choice_or_stop(
        case,
        ChoiceValue::String(out.clone()),
        ChoiceConstraints::String {
            min_size,
            max_size,
            alphabet: finite_alphabet_for_string_constraints(constraints),
        },
        false,
        observe,
    );
    out
}

pub(crate) fn draw_float_choice(
    engine: &mut EngineState,
    case_id: CaseId,
    min_value: f64,
    max_value: f64,
    allow_nan: bool,
    smallest_nonzero_magnitude: f64,
    observe: bool,
) -> f64 {
    {
        let case = case_state_mut(engine, case_id);
        if observe && let Some(ChoiceValue::Float(v)) = take_forced_choice(case, ChoiceKind::Float)
        {
            let permitted = if v.is_nan() {
                allow_nan
            } else {
                clamp_float_for_constraints(
                    v,
                    min_value,
                    max_value,
                    allow_nan,
                    smallest_nonzero_magnitude,
                )
                .to_bits()
                    == v.to_bits()
            };
            if !permitted {
                case.exhausted = true;
                stop_test_now();
            }
            record_choice_or_stop(
                case,
                ChoiceValue::Float(v),
                ChoiceConstraints::Float {
                    min_value,
                    max_value,
                    allow_nan,
                    smallest_nonzero_magnitude,
                },
                false,
                observe,
            );
            return v;
        }
    }

    let prefix_choice = {
        let case = case_state_mut(engine, case_id);
        if observe {
            pop_prefix_choice(case)
        } else {
            None
        }
    };
    if let Some(choice) = prefix_choice {
        let simplest = clamp_float_for_constraints(
            0.0,
            min_value,
            max_value,
            allow_nan,
            smallest_nonzero_magnitude,
        );
        let value = match choice {
            ChoiceValue::Float(v)
                if (v.is_nan() && allow_nan)
                    || (clamp_float_for_constraints(
                        v,
                        min_value,
                        max_value,
                        allow_nan,
                        smallest_nonzero_magnitude,
                    )
                    .to_bits()
                        == v.to_bits()) =>
            {
                v
            }
            _ => simplest,
        };
        let case = case_state_mut(engine, case_id);
        record_choice_or_stop(
            case,
            ChoiceValue::Float(value),
            ChoiceConstraints::Float {
                min_value,
                max_value,
                allow_nan,
                smallest_nonzero_magnitude,
            },
            false,
            observe,
        );
        return value;
    }

    if should_use_simplest_observed_draws(engine, observe) {
        let value = clamp_float_for_constraints(
            0.0,
            min_value,
            max_value,
            allow_nan,
            smallest_nonzero_magnitude,
        );
        let case = case_state_mut(engine, case_id);
        record_choice_or_stop(
            case,
            ChoiceValue::Float(value),
            ChoiceConstraints::Float {
                min_value,
                max_value,
                allow_nan,
                smallest_nonzero_magnitude,
            },
            false,
            observe,
        );
        return value;
    }

    let interesting =
        interesting_float_candidates(min_value, max_value, allow_nan, smallest_nonzero_magnitude);

    let case = case_state_mut(engine, case_id);
    let mode = draw_bits_from_case(case, 8) as u8;
    let raw_value = if mode < 176 && !interesting.is_empty() {
        let idx = if interesting.len() == 1 {
            0usize
        } else {
            draw_integer_uniform_from_case(case, 0, (interesting.len() - 1) as i128) as usize
        };
        interesting[idx]
    } else if min_value.is_finite() && max_value.is_finite() && min_value < max_value {
        let raw = draw_bits_from_case(case, 53) as f64;
        let frac = raw / ((1_u64 << 53) - 1) as f64;
        min_value + (max_value - min_value) * frac
    } else if min_value.is_finite() && !max_value.is_finite() {
        let exp = draw_bits_from_case(case, 6) as i32;
        let frac = draw_bits_from_case(case, 20) as f64 / ((1_u64 << 20) - 1) as f64;
        min_value + (2f64.powi(exp)) * (1.0 + frac)
    } else if !min_value.is_finite() && max_value.is_finite() {
        let exp = draw_bits_from_case(case, 6) as i32;
        let frac = draw_bits_from_case(case, 20) as f64 / ((1_u64 << 20) - 1) as f64;
        max_value - (2f64.powi(exp)) * (1.0 + frac)
    } else {
        let raw = draw_bits_from_case(case, 64) as u64;
        f64::from_bits(raw)
    };
    let value = clamp_float_for_constraints(
        raw_value,
        min_value,
        max_value,
        allow_nan,
        smallest_nonzero_magnitude,
    );

    record_choice_or_stop(
        case,
        ChoiceValue::Float(value),
        ChoiceConstraints::Float {
            min_value,
            max_value,
            allow_nan,
            smallest_nonzero_magnitude,
        },
        false,
        observe,
    );
    value
}

fn run_draw_choice_from_constraints(
    engine: &mut EngineState,
    constraints: &ChoiceConstraints,
) -> ChoiceValue {
    match constraints {
        ChoiceConstraints::Boolean { p } => {
            ChoiceValue::Boolean(run_draw_boolean_hypothesis(engine, *p))
        }
        ChoiceConstraints::Integer {
            min_value,
            max_value,
            ..
        } => ChoiceValue::Integer(run_draw_integer_hypothesis_choice(
            engine, *min_value, *max_value,
        )),
        ChoiceConstraints::Float {
            min_value,
            max_value,
            allow_nan,
            smallest_nonzero_magnitude,
        } => {
            if let Some(constant) = run_maybe_draw_float_constant(
                engine,
                *min_value,
                *max_value,
                *allow_nan,
                *smallest_nonzero_magnitude,
                0.15,
            ) {
                return ChoiceValue::Float(constant);
            }

            let weird = hypothesis_weird_floats(
                *min_value,
                *max_value,
                *allow_nan,
                *smallest_nonzero_magnitude,
            );
            if !weird.is_empty() && run_random_f64(engine) < 0.05 {
                let idx = run_randbelow_u128(engine, weird.len() as u128) as usize;
                return ChoiceValue::Float(weird[idx]);
            }

            let mut result = run_draw_float_hypothesis(engine);
            if !(*allow_nan && result.is_nan()) {
                let clamped = clamp_float_hypothesis_style(
                    result,
                    *min_value,
                    *max_value,
                    *allow_nan,
                    *smallest_nonzero_magnitude,
                );
                if clamped.to_bits() != result.to_bits() && !(result.is_nan() && *allow_nan) {
                    result = clamped;
                }
            }
            ChoiceValue::Float(result)
        }
        ChoiceConstraints::Bytes { min_size, max_size } => {
            if let Some(constant) =
                run_maybe_draw_bytes_constant(engine, *min_size, *max_size, 0.05)
            {
                return ChoiceValue::Bytes(constant);
            }

            let average_size = default_average_size(*min_size, Some(*max_size));
            let desired_avg = (average_size - *min_size as f64).max(0.0);
            let max_delta = Some(max_size.saturating_sub(*min_size));
            let p_continue = calc_p_continue(desired_avg, max_delta);

            let mut out = Vec::new();
            let mut count = 0usize;
            loop {
                let forced_result = if count < *min_size {
                    Some(true)
                } else if count >= *max_size {
                    Some(false)
                } else {
                    None
                };
                let should_continue = forced_result
                    .unwrap_or_else(|| run_draw_boolean_hypothesis(engine, p_continue));
                if !should_continue {
                    break;
                }
                out.push(run_getrandbits_u128(engine, 8) as u8);
                count = count.saturating_add(1);
            }
            ChoiceValue::Bytes(out)
        }
        ChoiceConstraints::String {
            min_size,
            max_size,
            alphabet,
        } => {
            let alphabet = alphabet
                .as_ref()
                .filter(|chars| !chars.is_empty())
                .cloned()
                .unwrap_or_else(|| (32u8..=126u8).map(char::from).collect());
            if let Some(constant) =
                run_maybe_draw_string_constant(engine, *min_size, *max_size, Some(&alphabet), 0.05)
            {
                return ChoiceValue::String(constant);
            }

            let average_size = default_average_size(*min_size, Some(*max_size));
            let desired_avg = (average_size - *min_size as f64).max(0.0);
            let max_delta = Some(max_size.saturating_sub(*min_size));
            let p_continue = calc_p_continue(desired_avg, max_delta);

            let mut out = String::new();
            let mut count = 0usize;
            loop {
                let forced_result = if count < *min_size {
                    Some(true)
                } else if count >= *max_size {
                    Some(false)
                } else {
                    None
                };
                let should_continue = forced_result
                    .unwrap_or_else(|| run_draw_boolean_hypothesis(engine, p_continue));
                if !should_continue {
                    break;
                }
                let idx = run_draw_alphabet_index_hypothesis(engine, alphabet.len());
                out.push(alphabet[idx]);
                count = count.saturating_add(1);
            }
            ChoiceValue::String(out)
        }
    }
}

fn generate_regex_value(engine: &mut EngineState, case_id: CaseId, schema: &Value) -> Value {
    let pattern = map_text(schema, "pattern").unwrap_or_else(|| panic!("regex missing `pattern`"));
    let fullmatch = map_bool(schema, "fullmatch").unwrap_or(false);
    let regex = Regex::new(pattern)
        .unwrap_or_else(|err| panic!("invalid regex pattern `{}`: {}", pattern, err));

    let mut alphabet: Vec<char> = (' '..='~').collect();
    if let Some(alphabet_schema) = crate::cbor_utils::map_get(schema, "alphabet") {
        let constraints = parse_string_char_constraints(alphabet_schema);
        alphabet.retain(|ch| char_allowed_by_constraints(*ch, &constraints));
    }
    if alphabet.is_empty() {
        stop_test_now();
    }

    let min_size = if fullmatch { 1 } else { 0 };
    let max_size = 32usize;
    for _ in 0..64 {
        let candidate =
            draw_unobserved_string_from_alphabet(engine, case_id, min_size, max_size, &alphabet);
        let matches = if fullmatch {
            regex
                .find(&candidate)
                .is_some_and(|m| m.start() == 0 && m.end() == candidate.len())
        } else {
            regex.is_match(&candidate)
        };
        if matches {
            let case = case_state_mut(engine, case_id);
            record_choice_or_stop(
                case,
                ChoiceValue::String(candidate.clone()),
                ChoiceConstraints::String {
                    min_size,
                    max_size,
                    alphabet: None,
                },
                false,
                true,
            );
            return Value::Text(candidate);
        }
    }
    stop_test_now();
}

fn draw_bits_from_case(case: &mut CaseBufferState, n_bits: usize) -> u128 {
    if n_bits == 0 {
        return 0;
    }
    if n_bits > 128 {
        panic!(
            "draw_bits currently supports up to 128 bits, got {}",
            n_bits
        );
    }
    if case.exhausted {
        stop_test_now();
    }

    let n_bytes = bits_to_bytes(n_bits);
    if case.cursor + n_bytes > case.bytes.len() {
        case.exhausted = true;
        case.stopped_because_overrun = true;
        stop_test_now();
    }

    let mut chunk = case.bytes[case.cursor..case.cursor + n_bytes].to_vec();
    case.cursor += n_bytes;
    chunk[0] &= BYTE_MASKS[n_bits % 8];

    chunk
        .iter()
        .fold(0u128, |acc, byte| (acc << 8) | u128::from(*byte))
}
