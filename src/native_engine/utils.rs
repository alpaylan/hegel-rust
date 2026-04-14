pub(crate) fn zigzag_value(index: u128, shrink_towards: i128) -> i128 {
    let n = index.saturating_add(1) as i128 / 2;
    let delta = if (index & 1) == 0 { -n } else { n };
    shrink_towards + delta as i128
}

pub(crate) fn zigzag_index(value: i128, shrink_towards: i128) -> u128 {
    let mut index = (shrink_towards - value).unsigned_abs().saturating_mul(2);
    if value > shrink_towards {
        index = index.saturating_sub(1);
    }
    index
}
