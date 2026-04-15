use std::{collections::HashMap, i128};

use sha2::digest::typenum::Pow;

use crate::native_engine::{
    choice::{ChoiceConstraints, ChoiceValue},
    data::ConjectureData,
    floats::{
        choice_permitted_float, float_to_int, int_to_float, lex_to_float, make_float_clamper,
        next_down, next_up,
    },
    intervalset::IntervalSet,
    random::Random,
    utils::{Many, Sampler},
};

pub trait Provider {
    fn draw(&mut self, constraint: ChoiceConstraints) -> ChoiceValue;
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
    cd: ConjectureData,
    random: Random,
}

impl HypothesisProvider {
    fn new(cd: ConjectureData) -> Self {
        Self {
            cd: todo!(),
            random: todo!(),
        }
    }
}

static INT_SIZES: &[usize] = &[8, 16, 32, 64, 128];

impl Provider for HypothesisProvider {
    fn draw(&mut self, constraint: ChoiceConstraints) -> ChoiceValue {
        match constraint {
            ChoiceConstraints::Boolean { p } => ChoiceValue::Boolean(self.draw_boolean(p)),
            ChoiceConstraints::Integer {
                min_value,
                max_value,
                weights,
                shrink_towards,
            } => ChoiceValue::Integer(self.draw_integer(
                min_value,
                max_value,
                weights,
                shrink_towards,
            )),
            ChoiceConstraints::Float {
                min_value,
                max_value,
                allow_nan,
                smallest_nonzero_magnitude,
            } => ChoiceValue::Float(self.draw_float(
                min_value,
                max_value,
                allow_nan,
                smallest_nonzero_magnitude,
            )),
            ChoiceConstraints::Bytes { min_size, max_size } => {
                ChoiceValue::Bytes(self.draw_bytes(min_size, max_size))
            }
            ChoiceConstraints::String {
                intervals,
                min_size,
                max_size,
            } => ChoiceValue::String(self.draw_string(intervals, min_size, max_size)),
        }
    }
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
                weights: weights.clone(),
                shrink_towards,
            },
            0.5,
        ) {
            if let ChoiceValue::Integer(value) = constant {
                return value;
            } else {
                unreachable!(
                    "maybe_draw_constant should only return an integer constant when given integer constraints"
                );
            }
        }

        let center = 0
            .max(min_value.unwrap_or(i128::MIN))
            .min(max_value.unwrap_or(i128::MAX));

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
        if let Some(constant) = self.maybe_draw_constant(
            &ChoiceConstraints::Float {
                min_value,
                max_value,
                allow_nan,
                smallest_nonzero_magnitude,
            },
            0.5,
        ) {
            if let ChoiceValue::Float(value) = constant {
                return value;
            } else {
                unreachable!(
                    "maybe_draw_constant should only return a float constant when given float constraints"
                );
            }
        }

        let weird_floats = vec![
            0.0,
            -0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
            -f64::NAN,
            int_to_float(0x7FF8_0000_0000_0001, 64),
            -int_to_float(0xFFF8_0000_0000_0001, 64),
            min_value,
            next_up(min_value, 64),
            min_value + 1.0,
            max_value - 1.0,
            next_down(max_value, 64),
            max_value,
        ]
        .into_iter()
        .filter(|c| {
            choice_permitted_float(
                *c,
                min_value,
                max_value,
                allow_nan,
                smallest_nonzero_magnitude,
            )
        })
        .collect::<Vec<_>>();

        if !weird_floats.is_empty() && self.random.random() < 0.5 {
            return self.random.choice(&weird_floats);
        }

        let clamper =
            make_float_clamper(min_value, max_value, allow_nan, smallest_nonzero_magnitude);

        let f = lex_to_float(self.random.get_rand_bits(64) as u64);
        let sign = if self.random.get_rand_bits(1) == 0 {
            1.0
        } else {
            -1.0
        };
        let mut result = f * sign;
        let clamped = if allow_nan && result.is_nan() {
            result
        } else {
            clamper(result)
        };

        if float_to_int(clamped, 64) != float_to_int(result, 64)
            && !(result.is_nan() && clamped.is_nan())
        {
            // If the clamping changed the value, return the clamped value with 50% probability to increase the chances of hitting edge cases.
            result = clamped;
        }

        result
    }

    fn draw_bytes(&mut self, min_size: usize, max_size: usize) -> Vec<u8> {
        if let Some(constant) =
            self.maybe_draw_constant(&ChoiceConstraints::Bytes { min_size, max_size }, 0.5)
        {
            if let ChoiceValue::Bytes(value) = constant {
                return value;
            } else {
                unreachable!(
                    "maybe_draw_constant should only return a bytes constant when given bytes constraints"
                );
            }
        }

        let mut bytes = Vec::new();
        let average_size = (min_size * 2)
            .max(min_size + 5)
            .min(min_size.midpoint(max_size));

        let mut elements = Many::new(min_size, max_size, average_size, None, false);

        while elements.more(&mut self.cd) {
            bytes.push(self.random.random_byte());
        }

        bytes
    }

    fn draw_string(&mut self, intervals: IntervalSet, min_size: usize, max_size: usize) -> String {
        if let Some(constant) = self.maybe_draw_constant(
            &ChoiceConstraints::String {
                intervals: intervals.clone(),
                min_size,
                max_size,
            },
            0.5,
        ) {
            if let ChoiceValue::String(value) = constant {
                return value;
            } else {
                unreachable!(
                    "maybe_draw_constant should only return a string constant when given string constraints"
                );
            }
        }

        let average_size = (min_size * 2)
            .max(min_size + 5)
            .min(min_size.midpoint(max_size));

        let mut chars = Vec::new();

        let mut elements = Many::new(min_size, max_size, average_size, None, false);

        while elements.more(&mut self.cd) {
            let i = if intervals.len() > 256 {
                if self.draw_boolean(0.2) {
                    self.random.random_int(256, intervals.len() as i128 - 1)
                } else {
                    self.random.random_int(0, 255)
                }
            } else {
                self.random.random_int(0, intervals.len() as i128 - 1)
            };

            chars.push(intervals.char_in_shrink_order(i as usize));
        }

        chars.into_iter().collect()
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
            ChoiceConstraints::Float { .. } => {
                Some(ChoiceValue::Float(self.random.choice(constant_floats())))
            }
            ChoiceConstraints::Bytes { .. } => None,
            ChoiceConstraints::String { .. } => Some(ChoiceValue::String(
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

        let bits = Self::bit_length(max - min);

        if bits > 24 && vary_size && self.random.random() < 0.875 {
            let idx = Sampler::int_sizes_sampler().sample(&mut self.cd, None);
            let cap_bits = bits.midpoint(INT_SIZES[idx] as u32);
            let upper = max.min(min + 2_i128.pow(cap_bits) - 1);
            return self.random.random_int(min, upper);
        }

        self.random.random_int(min, max)
    }

    fn draw_unbounded_integer(&mut self) -> i128 {
        let size = INT_SIZES[Sampler::int_sizes_sampler().sample(&mut self.cd, None)];
        let mut r = self.random.get_rand_bits(size);
        let sign = r & 1;
        r >>= 1;
        if sign != 0 {
            r = -r;
        }
        r
    }

    fn bit_length(mut n: i128) -> u32 {
        let mut bits = 0;
        while n > 0 {
            bits += 1;
            n >>= 1;
        }
        bits
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
