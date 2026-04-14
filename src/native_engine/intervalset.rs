use std::fmt;
use std::ops::{BitAnd, BitOr, Sub};

/// A compact and efficient representation of a set of `(a, b)` intervals. Can
/// be treated like a set of integers, in that `contains(n)` will return
/// `true` if `n` is contained in any of the `(a, b)` intervals, and
/// `false` otherwise.
#[derive(Clone, Eq)]
pub struct IntervalSet {
    pub intervals: Vec<(u32, u32)>,
    pub offsets: Vec<usize>,
    pub size: usize,
    _idx_of_zero: usize,
    _idx_of_z_upper: usize,
}

impl IntervalSet {
    /// Return an IntervalSet covering the codepoints of characters in `s`.
    ///
    /// ```text
    /// IntervalSet::from_string("abcdef0123456789")
    /// // => ((48, 57), (97, 102))
    /// ```
    pub fn from_string(s: &str) -> Self {
        let mut chars: Vec<u32> = s.chars().map(|c| c as u32).collect();
        chars.sort();
        let intervals: Vec<(u32, u32)> = chars.iter().map(|&c| (c, c)).collect();
        let x = IntervalSet::new(intervals);
        x.union(&x)
    }

    pub fn new(intervals: Vec<(u32, u32)>) -> Self {
        // Validate that all intervals are pairs (enforced by type system)
        // and that start <= end
        for &(u, v) in &intervals {
            assert!(u <= v);
        }

        let mut offsets: Vec<usize> = vec![0];
        for &(u, v) in &intervals {
            let last = *offsets.last().unwrap();
            offsets.push(last + (v - u + 1) as usize);
        }
        let size = offsets.pop().unwrap();

        let tmp = IntervalSet {
            intervals,
            offsets,
            size,
            _idx_of_zero: 0,
            _idx_of_z_upper: 0,
        };

        let _idx_of_zero = tmp.index_above(b'0' as u32);
        let _idx_of_z_upper =
            std::cmp::min(tmp.index_above(b'Z' as u32), tmp.len().saturating_sub(1));

        IntervalSet {
            intervals: tmp.intervals,
            offsets: tmp.offsets,
            size: tmp.size,
            _idx_of_zero,
            _idx_of_z_upper,
        }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn iter(&self) -> IntervalSetIter<'_> {
        IntervalSetIter {
            intervals: &self.intervals,
            interval_idx: 0,
            current: 0,
        }
    }

    pub fn getitem(&self, mut i: isize) -> u32 {
        if i < 0 {
            i += self.size as isize;
        }
        if i < 0 || i as usize >= self.size {
            panic!("Invalid index {} for [0, {})", i, self.size);
        }
        let i = i as usize;
        // Want j = maximal such that offsets[j] <= i

        let mut j = self.intervals.len() - 1;
        if self.offsets[j] > i {
            let mut hi = j;
            let mut lo: usize = 0;
            // Invariant: offsets[lo] <= i < offsets[hi]
            while lo + 1 < hi {
                let mid = (lo + hi) / 2;
                if self.offsets[mid] <= i {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }
            j = lo;
        }
        let t = i - self.offsets[j];
        let (u, v) = self.intervals[j];
        let r = u + t as u32;
        assert!(r <= v);
        r
    }

    pub fn contains_value(&self, elem: u32) -> bool {
        assert!(elem <= 0x10FFFF);
        self.intervals
            .iter()
            .any(|&(start, end)| start <= elem && elem <= end)
    }

    pub fn contains_char(&self, c: char) -> bool {
        self.contains_value(c as u32)
    }

    pub fn index(&self, value: u32) -> Result<usize, String> {
        for (offset, &(u, v)) in self.offsets.iter().zip(self.intervals.iter()) {
            if u == value {
                return Ok(*offset);
            } else if u > value {
                return Err(format!("{} is not in list", value));
            }
            if value <= v {
                return Ok(offset + (value - u) as usize);
            }
        }
        Err(format!("{} is not in list", value))
    }

    pub fn index_above(&self, value: u32) -> usize {
        for (offset, &(u, v)) in self.offsets.iter().zip(self.intervals.iter()) {
            if u >= value {
                return *offset;
            }
            if value <= v {
                return offset + (value - u) as usize;
            }
        }
        self.size
    }

    pub fn union(&self, other: &IntervalSet) -> IntervalSet {
        //! Merge two sequences of intervals into a single IntervalSet.
        //!
        //! Any integer bounded by `self` or `other` is also bounded by the result.
        //!
        //! ```text
        //! union([(3, 10)], [(1, 2), (5, 17)])
        //! // => ((1, 17),)
        //! ```
        let x = &self.intervals;
        let y = &other.intervals;
        if x.is_empty() {
            return IntervalSet::new(y.clone());
        }
        if y.is_empty() {
            return IntervalSet::new(x.clone());
        }
        let mut intervals: Vec<(u32, u32)> = x.iter().chain(y.iter()).copied().collect();
        intervals.sort();
        intervals.reverse();
        let mut result: Vec<(u32, u32)> = vec![intervals.pop().unwrap()];
        while let Some((u, v)) = intervals.pop() {
            // 1. intervals is in descending order
            // 2. pop() takes from the RHS.
            // 3. (a, b) was popped 1st, then (u, v) was popped 2nd
            // 4. Therefore: a <= u
            // 5. We assume that u <= v and a <= b
            // 6. So we need to handle 2 cases of overlap, and one disjoint case
            //    |   u--v     |   u----v   |       u--v  |
            //    |   a----b   |   a--b     |  a--b       |
            let (a, b) = result.last_mut().unwrap();
            if u <= *b + 1 {
                // Overlap cases
                *b = std::cmp::max(v, *b);
            } else {
                // Disjoint case
                result.push((u, v));
            }
        }
        IntervalSet::new(result)
    }

    pub fn difference(&self, other: &IntervalSet) -> IntervalSet {
        //! Set difference for lists of intervals. That is, returns a list of
        //! intervals that bounds all values bounded by x that are not also bounded by
        //! y. x and y are expected to be in sorted order.
        //!
        //! For example difference([(1, 10)], [(2, 3), (9, 15)]) would
        //! return [(1, 1), (4, 8)], removing the values 2, 3, 9 and 10 from the
        //! interval.
        let y = &other.intervals;
        if y.is_empty() {
            return IntervalSet::new(self.intervals.clone());
        }
        let mut x: Vec<[u32; 2]> = self.intervals.iter().map(|&(a, b)| [a, b]).collect();
        let mut i: usize = 0;
        let mut j: usize = 0;
        let mut result: Vec<(u32, u32)> = Vec::new();
        while i < x.len() && j < y.len() {
            // Iterate in parallel over x and y. j stays pointing at the smallest
            // interval in the left hand side that could still overlap with some
            // element of x at index >= i.
            // Similarly, i is not incremented until we know that it does not
            // overlap with any element of y at index >= j.

            let [xl, xr] = x[i];
            assert!(xl <= xr);
            let (yl, yr) = y[j];
            assert!(yl <= yr);

            if yr < xl {
                // The interval at y[j] is strictly to the left of the interval at
                // x[i], so will not overlap with it or any later interval of x.
                j += 1;
            } else if yl > xr {
                // The interval at y[j] is strictly to the right of the interval at
                // x[i], so all of x[i] goes into the result as no further intervals
                // in y will intersect it.
                result.push((x[i][0], x[i][1]));
                i += 1;
            } else if yl <= xl {
                if yr >= xr {
                    // x[i] is contained entirely in y[j], so we just skip over it
                    // without adding it to the result.
                    i += 1;
                } else {
                    // The beginning of x[i] is contained in y[j], so we update the
                    // left endpoint of x[i] to remove this, and increment j as we
                    // now have moved past it. Note that this is not added to the
                    // result as is, as more intervals from y may intersect it so it
                    // may need updating further.
                    x[i][0] = yr + 1;
                    j += 1;
                }
            } else {
                // yl > xl, so the left hand part of x[i] is not contained in y[j],
                // so there are some values we should add to the result.
                result.push((xl, yl - 1));

                if yr + 1 <= xr {
                    // If y[j] finishes before x[i] does, there may be some values
                    // in x[i] left that should go in the result (or they may be
                    // removed by a later interval in y), so we update x[i] to
                    // reflect that and increment j because it no longer overlaps
                    // with any remaining element of x.
                    x[i][0] = yr + 1;
                    j += 1;
                } else {
                    // Every element of x[i] other than the initial part we have
                    // already added is contained in y[j], so we move to the next
                    // interval.
                    i += 1;
                }
            }
        }
        // Any remaining intervals in x do not overlap with any of y, as if they did
        // we would not have incremented j to the end, so can be added to the result
        // as they are.
        for item in &x[i..] {
            result.push((item[0], item[1]));
        }
        IntervalSet::new(result)
    }

    pub fn intersection(&self, other: &IntervalSet) -> IntervalSet {
        /// Set intersection for lists of intervals.
        let mut intervals: Vec<(u32, u32)> = Vec::new();
        let mut i: usize = 0;
        let mut j: usize = 0;
        while i < self.intervals.len() && j < other.intervals.len() {
            let (u, v) = self.intervals[i];
            let (u_other, v_other) = other.intervals[j];
            if u > v_other {
                j += 1;
            } else if u_other > v {
                i += 1;
            } else {
                intervals.push((std::cmp::max(u, u_other), std::cmp::min(v, v_other)));
                if v < v_other {
                    i += 1;
                } else {
                    j += 1;
                }
            }
        }
        IntervalSet::new(intervals)
    }

    pub fn char_in_shrink_order(&self, mut i: usize) -> char {
        // We would like it so that, where possible, shrinking replaces
        // characters with simple ascii characters, so we rejig this
        // bit so that the smallest values are 0, 1, 2, ..., Z.
        //
        // Imagine that numbers are laid out as abc0yyyZ...
        // this rearranges them so that they are laid out as
        // 0yyyZcba..., which gives a better shrinking order.
        if i <= self._idx_of_z_upper {
            // We want to rewrite the integers [0, n] inclusive
            // to [zero_point, Z_point].
            let n = self._idx_of_z_upper - self._idx_of_zero;
            if i <= n {
                i += self._idx_of_zero;
            } else {
                // We want to rewrite the integers [n + 1, Z_point] to
                // [zero_point, 0] (reversing the order so that codepoints below
                // zero_point shrink upwards).
                i = self._idx_of_zero - (i - n);
                assert!(i < self._idx_of_zero);
            }
            assert!(i <= self._idx_of_z_upper);
        }

        char::from_u32(self.getitem(i as isize)).unwrap()
    }

    pub fn index_from_char_in_shrink_order(&self, c: char) -> usize {
        /// Inverse of char_in_shrink_order.
        let mut i = self.index(c as u32).unwrap();

        if i <= self._idx_of_z_upper {
            let n = self._idx_of_z_upper - self._idx_of_zero;
            // Rewrite [zero_point, Z_point] to [0, n].
            if self._idx_of_zero <= i && i <= self._idx_of_z_upper {
                i -= self._idx_of_zero;
                assert!(i <= n);
            }
            // Rewrite [zero_point, 0] to [n + 1, Z_point].
            else {
                i = self._idx_of_zero - i + n;
                assert!(n + 1 <= i && i <= self._idx_of_z_upper);
            }
            assert!(i <= self._idx_of_z_upper);
        }

        i
    }
}

impl fmt::Debug for IntervalSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IntervalSet({:?})", self.intervals)
    }
}

impl fmt::Display for IntervalSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IntervalSet({:?})", self.intervals)
    }
}

impl PartialEq for IntervalSet {
    fn eq(&self, other: &Self) -> bool {
        self.intervals == other.intervals
    }
}

impl std::hash::Hash for IntervalSet {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.intervals.hash(state);
    }
}

impl BitOr for &IntervalSet {
    type Output = IntervalSet;
    fn bitor(self, rhs: Self) -> IntervalSet {
        self.union(rhs)
    }
}

impl Sub for &IntervalSet {
    type Output = IntervalSet;
    fn sub(self, rhs: Self) -> IntervalSet {
        self.difference(rhs)
    }
}

impl BitAnd for &IntervalSet {
    type Output = IntervalSet;
    fn bitand(self, rhs: Self) -> IntervalSet {
        self.intersection(rhs)
    }
}

pub struct IntervalSetIter<'a> {
    intervals: &'a [(u32, u32)],
    interval_idx: usize,
    current: u32,
}

impl<'a> Iterator for IntervalSetIter<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        loop {
            if self.interval_idx >= self.intervals.len() {
                return None;
            }
            let (u, v) = self.intervals[self.interval_idx];
            if self.interval_idx == 0 && self.current < u {
                self.current = u;
            }
            if self.current <= v {
                let val = self.current;
                self.current += 1;
                return Some(val);
            }
            self.interval_idx += 1;
            if self.interval_idx < self.intervals.len() {
                self.current = self.intervals[self.interval_idx].0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let s = IntervalSet::new(vec![]);
        assert_eq!(s.len(), 0);
        assert!(s.is_empty());
    }

    #[test]
    fn test_single_interval() {
        let s = IntervalSet::new(vec![(1, 5)]);
        assert_eq!(s.len(), 5);
        assert_eq!(s.getitem(0), 1);
        assert_eq!(s.getitem(4), 5);
    }

    #[test]
    fn test_multiple_intervals() {
        let s = IntervalSet::new(vec![(1, 3), (7, 9)]);
        assert_eq!(s.len(), 6);
        assert_eq!(s.getitem(0), 1);
        assert_eq!(s.getitem(2), 3);
        assert_eq!(s.getitem(3), 7);
        assert_eq!(s.getitem(5), 9);
    }

    #[test]
    fn test_negative_indexing() {
        let s = IntervalSet::new(vec![(1, 3)]);
        assert_eq!(s.getitem(-1), 3);
        assert_eq!(s.getitem(-3), 1);
    }

    #[test]
    #[should_panic(expected = "Invalid index")]
    fn test_out_of_bounds() {
        let s = IntervalSet::new(vec![(1, 3)]);
        s.getitem(5);
    }

    #[test]
    fn test_contains() {
        let s = IntervalSet::new(vec![(1, 3), (7, 9)]);
        assert!(s.contains_value(1));
        assert!(s.contains_value(2));
        assert!(s.contains_value(3));
        assert!(!s.contains_value(4));
        assert!(s.contains_value(7));
        assert!(s.contains_value(9));
        assert!(!s.contains_value(10));
    }

    #[test]
    fn test_index() {
        let s = IntervalSet::new(vec![(1, 3), (7, 9)]);
        assert_eq!(s.index(1).unwrap(), 0);
        assert_eq!(s.index(3).unwrap(), 2);
        assert_eq!(s.index(7).unwrap(), 3);
        assert_eq!(s.index(9).unwrap(), 5);
        assert!(s.index(5).is_err());
    }

    #[test]
    fn test_index_above() {
        let s = IntervalSet::new(vec![(1, 3), (7, 9)]);
        assert_eq!(s.index_above(0), 0);
        assert_eq!(s.index_above(1), 0);
        assert_eq!(s.index_above(2), 1);
        assert_eq!(s.index_above(5), 3);
        assert_eq!(s.index_above(7), 3);
        assert_eq!(s.index_above(10), 6);
    }

    #[test]
    fn test_union() {
        let a = IntervalSet::new(vec![(3, 10)]);
        let b = IntervalSet::new(vec![(1, 2), (5, 17)]);
        let c = a.union(&b);
        assert_eq!(c.intervals, vec![(1, 17)]);
    }

    #[test]
    fn test_union_disjoint() {
        let a = IntervalSet::new(vec![(1, 3)]);
        let b = IntervalSet::new(vec![(7, 9)]);
        let c = a.union(&b);
        assert_eq!(c.intervals, vec![(1, 3), (7, 9)]);
    }

    #[test]
    fn test_union_adjacent() {
        let a = IntervalSet::new(vec![(1, 3)]);
        let b = IntervalSet::new(vec![(4, 6)]);
        let c = a.union(&b);
        assert_eq!(c.intervals, vec![(1, 6)]);
    }

    #[test]
    fn test_difference() {
        let a = IntervalSet::new(vec![(1, 10)]);
        let b = IntervalSet::new(vec![(2, 3), (9, 15)]);
        let c = a.difference(&b);
        assert_eq!(c.intervals, vec![(1, 1), (4, 8)]);
    }

    #[test]
    fn test_difference_no_overlap() {
        let a = IntervalSet::new(vec![(1, 3)]);
        let b = IntervalSet::new(vec![(7, 9)]);
        let c = a.difference(&b);
        assert_eq!(c.intervals, vec![(1, 3)]);
    }

    #[test]
    fn test_difference_empty_rhs() {
        let a = IntervalSet::new(vec![(1, 3)]);
        let b = IntervalSet::new(vec![]);
        let c = a.difference(&b);
        assert_eq!(c.intervals, vec![(1, 3)]);
    }

    #[test]
    fn test_intersection() {
        let a = IntervalSet::new(vec![(1, 10)]);
        let b = IntervalSet::new(vec![(5, 15)]);
        let c = a.intersection(&b);
        assert_eq!(c.intervals, vec![(5, 10)]);
    }

    #[test]
    fn test_intersection_disjoint() {
        let a = IntervalSet::new(vec![(1, 3)]);
        let b = IntervalSet::new(vec![(7, 9)]);
        let c = a.intersection(&b);
        assert_eq!(c.intervals, vec![]);
    }

    #[test]
    fn test_iter() {
        let s = IntervalSet::new(vec![(1, 3), (7, 9)]);
        let vals: Vec<u32> = s.iter().collect();
        assert_eq!(vals, vec![1, 2, 3, 7, 8, 9]);
    }

    #[test]
    fn test_from_string() {
        let s = IntervalSet::from_string("abcdef0123456789");
        assert_eq!(s.intervals, vec![(48, 57), (97, 102)]);
    }

    #[test]
    fn test_operators() {
        let a = IntervalSet::new(vec![(1, 5)]);
        let b = IntervalSet::new(vec![(3, 8)]);
        assert_eq!((&a | &b).intervals, vec![(1, 8)]);
        assert_eq!((&a - &b).intervals, vec![(1, 2)]);
        assert_eq!((&a & &b).intervals, vec![(3, 5)]);
    }

    #[test]
    fn test_equality_and_hash() {
        use std::collections::HashSet;
        let a = IntervalSet::new(vec![(1, 3)]);
        let b = IntervalSet::new(vec![(1, 3)]);
        let c = IntervalSet::new(vec![(1, 4)]);
        assert_eq!(a, b);
        assert_ne!(a, c);
        let mut set = HashSet::new();
        set.insert(a.clone());
        assert!(set.contains(&b));
    }

    #[test]
    fn test_char_in_shrink_order_roundtrip() {
        // Use a set that includes '0'..'Z' and some chars below
        let s = IntervalSet::new(vec![(32, 126)]); // printable ASCII
        for i in 0..s.len() {
            let c = s.char_in_shrink_order(i);
            let j = s.index_from_char_in_shrink_order(c);
            assert_eq!(i, j, "roundtrip failed for i={}", i);
        }
    }
}
