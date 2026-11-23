//! `Math.cos` as V8 computes it: V8's `base::ieee754::cos` is a port of
//! fdlibm 5.3 (`s_cos.c`, `k_cos.c`, `k_sin.c`, `e_rem_pio2.c`,
//! `k_rem_pio2.c`), reproduced here. The platform `f64::cos` differs from
//! it in the last ulp for a few percent of inputs, which would break
//! bit-exact address interpolation.
//!
//! V8's own bits depend on how it was compiled: clang contracts `a * b + c`
//! into fused multiply-adds where the target always has FMA (arm64), while
//! baseline x86-64 builds cannot. [`CONTRACTED`] mirrors that per target, so
//! the result matches Node/Chrome built for the same architecture (verified
//! bit-for-bit against Node 24 on arm64 by the parity fixtures).
//!
//! fdlibm notice (preserved as its license requires):
//! Copyright (C) 1993 by Sun Microsystems, Inc. All rights reserved.
//! Developed at SunSoft, a Sun Microsystems, Inc. business.
//! Permission to use, copy, modify, and distribute this
//! software is freely granted, provided that this notice
//! is preserved.

#![allow(clippy::excessive_precision, clippy::unreadable_literal)]

/// Whether `a * b + c` sub-expressions are fused, like a clang build of
/// fdlibm for this target.
const CONTRACTED: bool = cfg!(target_arch = "aarch64");

/// `a * b + c`, fused when the reference build would fuse it.
#[inline]
fn madd(a: f64, b: f64, c: f64) -> f64 {
    if CONTRACTED {
        a.mul_add(b, c)
    } else {
        a * b + c
    }
}

fn high_word(x: f64) -> i32 {
    (x.to_bits() >> 32) as i32
}

fn low_word(x: f64) -> u32 {
    x.to_bits() as u32
}

fn from_words(high: u32, low: u32) -> f64 {
    f64::from_bits(((high as u64) << 32) | low as u64)
}

/// `scalbn(x, n)`: exact power-of-two scaling.
fn scalbn(x: f64, n: i32) -> f64 {
    let mut value = x;
    let mut n = n;
    while n > 1023 {
        value *= f64::from_bits(0x7fe0_0000_0000_0000);
        n -= 1023;
    }
    while n < -1022 {
        value *= f64::from_bits(0x0010_0000_0000_0000);
        n += 1022;
    }
    value * f64::from_bits(((n + 1023) as u64) << 52)
}

const C1: f64 = 4.16666666666666019037e-02;
const C2: f64 = -1.38888888888741095749e-03;
const C3: f64 = 2.48015872894767294178e-05;
const C4: f64 = -2.75573143513906633035e-07;
const C5: f64 = 2.08757232129817482790e-09;
const C6: f64 = -1.13596475577881948265e-11;

fn kernel_cos(x: f64, y: f64) -> f64 {
    let ix = high_word(x) & 0x7fff_ffff;
    if ix < 0x3e40_0000 && x as i32 == 0 {
        return 1.0;
    }
    let z = x * x;
    let r = z * madd(z, madd(z, madd(z, madd(z, madd(z, C6, C5), C4), C3), C2), C1);
    // z * r - x * y
    let w = madd(z, r, -(x * y));
    if ix < 0x3fd3_3333 {
        return 1.0 - madd(0.5, z, -w);
    }
    let qx = if ix > 0x3fe9_0000 { 0.28125 } else { from_words((ix - 0x0020_0000) as u32, 0) };
    let iz = madd(0.5, z, -qx);
    let a = 1.0 - qx;
    a - (iz - w)
}

const S1: f64 = -1.66666666666666324348e-01;
const S2: f64 = 8.33333333332248946124e-03;
const S3: f64 = -1.98412698298579493134e-04;
const S4: f64 = 2.75573137070700676789e-06;
const S5: f64 = -2.50507602534068634195e-08;
const S6: f64 = 1.58969099521155010221e-10;

fn kernel_sin(x: f64, y: f64, iy: i32) -> f64 {
    let ix = high_word(x) & 0x7fff_ffff;
    if ix < 0x3e40_0000 && x as i32 == 0 {
        return x;
    }
    let z = x * x;
    let v = z * x;
    let r = madd(z, madd(z, madd(z, madd(z, S6, S5), S4), S3), S2);
    if iy == 0 {
        return madd(v, madd(z, r, S1), x);
    }
    // x - ((z * (0.5 * y - v * r) - y) - v * S1)
    let inner = madd(0.5, y, -(v * r));
    let u = madd(z, inner, -y);
    x - madd(-v, S1, u)
}

static TWO_OVER_PI: [i32; 66] = [
    0xA2F983, 0x6E4E44, 0x1529FC, 0x2757D1, 0xF534DD, 0xC0DB62, 0x95993C, 0x439041, 0xFE5163, 0xABDEBB, 0xC561B7,
    0x246E3A, 0x424DD2, 0xE00649, 0x2EEA09, 0xD1921C, 0xFE1DEB, 0x1CB129, 0xA73EE8, 0x8235F5, 0x2EBB44, 0x84E99C,
    0x7026B4, 0x5F7E41, 0x3991D6, 0x398353, 0x39F49C, 0x845F8B, 0xBDF928, 0x3B1FF8, 0x97FFDE, 0x05980F, 0xEF2F11,
    0x8B5A0A, 0x6D1F6D, 0x367ECF, 0x27CB09, 0xB74F46, 0x3F669E, 0x5FEA2D, 0x7527BA, 0xC7EBE5, 0xF17B3D, 0x0739F7,
    0x8A5292, 0xEA6BFB, 0x5FB11F, 0x8D5D08, 0x560330, 0x46FC7B, 0x6BABF0, 0xCFBC20, 0x9AF436, 0x1DA9E3, 0x91615E,
    0xE61B08, 0x659985, 0x5F14A0, 0x68408D, 0xFFD880, 0x4D7327, 0x310606, 0x1556CA, 0x73A8C9, 0x60E27B, 0xC08C6B,
];

static NPIO2_HW: [i32; 32] = [
    0x3FF921FB, 0x400921FB, 0x4012D97C, 0x401921FB, 0x401F6A7A, 0x4022D97C, 0x4025FDBB, 0x402921FB, 0x402C463A,
    0x402F6A7A, 0x4031475C, 0x4032D97C, 0x40346B9C, 0x4035FDBB, 0x40378FDB, 0x403921FB, 0x403AB41B, 0x403C463A,
    0x403DD85A, 0x403F6A7A, 0x40407E4C, 0x4041475C, 0x4042106C, 0x4042D97C, 0x4043A28C, 0x40446B9C, 0x404534AC,
    0x4045FDBB, 0x4046C6CB, 0x40478FDB, 0x404858EB, 0x404921FB,
];

const TWO24: f64 = 1.67772160000000000000e+07;
const TWON24: f64 = 5.96046447753906250000e-08;
const INVPIO2: f64 = 6.36619772367581382433e-01;
const PIO2_1: f64 = 1.57079632673412561417e+00;
const PIO2_1T: f64 = 6.07710050650619224932e-11;
const PIO2_2: f64 = 6.07710050630396597660e-11;
const PIO2_2T: f64 = 2.02226624879595063154e-21;
const PIO2_3: f64 = 2.02226624871116645580e-21;
const PIO2_3T: f64 = 8.47842766036889956997e-32;

static PIO2: [f64; 8] = [
    1.57079625129699707031e+00,
    7.54978941586159635335e-08,
    5.39030252995776476554e-15,
    3.28200341580791294123e-22,
    1.27065575308067607349e-29,
    1.22933308981111328932e-36,
    2.73370053816464559624e-44,
    2.16741683877804819444e-51,
];

/// `__kernel_rem_pio2` for `prec = 2` (double precision).
fn kernel_rem_pio2(x: &[f64; 3], y: &mut [f64; 2], e0: i32, nx: usize) -> i32 {
    let jk: i32 = 4;
    let jp = jk;
    let jx = nx as i32 - 1;
    let mut jv = (e0 - 3) / 24;
    if jv < 0 {
        jv = 0;
    }
    let mut q0 = e0 - 24 * (jv + 1);

    let mut f = [0f64; 20];
    let mut q = [0f64; 20];
    let mut fq = [0f64; 20];
    let mut iq = [0i32; 20];

    let mut j = jv - jx;
    let m = jx + jk;
    for slot in f.iter_mut().take(m as usize + 1) {
        *slot = if j < 0 { 0.0 } else { TWO_OVER_PI[j as usize] as f64 };
        j += 1;
    }
    for i in 0..=jk {
        let mut fw = 0.0;
        for j in 0..=jx {
            fw = madd(x[j as usize], f[(jx + i - j) as usize], fw);
        }
        q[i as usize] = fw;
    }

    let mut jz = jk;
    let mut z;
    let mut n;
    let mut ih;
    loop {
        // distill q[] into iq[] reversingly
        let mut i = 0usize;
        let mut j = jz;
        z = q[jz as usize];
        while j > 0 {
            let fw = ((TWON24 * z) as i32) as f64;
            iq[i] = madd(-TWO24, fw, z) as i32;
            z = q[(j - 1) as usize] + fw;
            i += 1;
            j -= 1;
        }

        z = scalbn(z, q0);
        z = madd(-8.0, (z * 0.125).floor(), z);
        n = z as i32;
        z -= n as f64;
        ih = 0;
        if q0 > 0 {
            let i = iq[(jz - 1) as usize] >> (24 - q0);
            n += i;
            iq[(jz - 1) as usize] -= i << (24 - q0);
            ih = iq[(jz - 1) as usize] >> (23 - q0);
        } else if q0 == 0 {
            ih = iq[(jz - 1) as usize] >> 23;
        } else if z >= 0.5 {
            ih = 2;
        }

        if ih > 0 {
            n += 1;
            let mut carry = 0;
            for slot in iq.iter_mut().take(jz as usize) {
                let j = *slot;
                if carry == 0 {
                    if j != 0 {
                        carry = 1;
                        *slot = 0x1000000 - j;
                    }
                } else {
                    *slot = 0xFFFFFF - j;
                }
            }
            if q0 > 0 {
                match q0 {
                    1 => iq[(jz - 1) as usize] &= 0x7FFFFF,
                    2 => iq[(jz - 1) as usize] &= 0x3FFFFF,
                    _ => {}
                }
            }
            if ih == 2 {
                z = 1.0 - z;
                if carry != 0 {
                    z -= scalbn(1.0, q0);
                }
            }
        }

        if z == 0.0 {
            let mut j = 0;
            let mut i = jz - 1;
            while i >= jk {
                j |= iq[i as usize];
                i -= 1;
            }
            if j == 0 {
                let mut k = 1;
                while jk >= k && iq[(jk - k) as usize] == 0 {
                    k += 1;
                }
                for i in jz + 1..=jz + k {
                    f[(jx + i) as usize] = TWO_OVER_PI[(jv + i) as usize] as f64;
                    let mut fw = 0.0;
                    for j in 0..=jx {
                        fw = madd(x[j as usize], f[(jx + i - j) as usize], fw);
                    }
                    q[i as usize] = fw;
                }
                jz += k;
                continue;
            }
        }
        break;
    }

    if z == 0.0 {
        jz -= 1;
        q0 -= 24;
        while iq[jz as usize] == 0 {
            jz -= 1;
            q0 -= 24;
        }
    } else {
        z = scalbn(z, -q0);
        if z >= TWO24 {
            let fw = ((TWON24 * z) as i32) as f64;
            iq[jz as usize] = madd(-TWO24, fw, z) as i32;
            jz += 1;
            q0 += 24;
            iq[jz as usize] = fw as i32;
        } else {
            iq[jz as usize] = z as i32;
        }
    }

    let mut fw = scalbn(1.0, q0);
    let mut i = jz;
    while i >= 0 {
        q[i as usize] = fw * iq[i as usize] as f64;
        fw *= TWON24;
        i -= 1;
    }

    let mut i = jz;
    while i >= 0 {
        let mut fw = 0.0;
        let mut k = 0;
        while k <= jp && k <= jz - i {
            fw = madd(PIO2[k as usize], q[(i + k) as usize], fw);
            k += 1;
        }
        fq[(jz - i) as usize] = fw;
        i -= 1;
    }

    let mut fw = 0.0;
    let mut i = jz;
    while i >= 0 {
        fw += fq[i as usize];
        i -= 1;
    }
    y[0] = if ih == 0 { fw } else { -fw };
    fw = fq[0] - fw;
    for i in 1..=jz {
        fw += fq[i as usize];
    }
    y[1] = if ih == 0 { fw } else { -fw };
    n & 7
}

/// `__ieee754_rem_pio2(x, y)`: `x` reduced by the nearest multiple of π/2.
fn rem_pio2(x: f64, y: &mut [f64; 2]) -> i32 {
    let hx = high_word(x);
    let ix = hx & 0x7fff_ffff;
    if ix <= 0x3fe9_21fb {
        y[0] = x;
        y[1] = 0.0;
        return 0;
    }
    if ix < 0x4002_d97c {
        if hx > 0 {
            let mut z = x - PIO2_1;
            if ix != 0x3ff9_21fb {
                y[0] = z - PIO2_1T;
                y[1] = (z - y[0]) - PIO2_1T;
            } else {
                z -= PIO2_2;
                y[0] = z - PIO2_2T;
                y[1] = (z - y[0]) - PIO2_2T;
            }
            return 1;
        }
        let mut z = x + PIO2_1;
        if ix != 0x3ff9_21fb {
            y[0] = z + PIO2_1T;
            y[1] = (z - y[0]) + PIO2_1T;
        } else {
            z += PIO2_2;
            y[0] = z + PIO2_2T;
            y[1] = (z - y[0]) + PIO2_2T;
        }
        return -1;
    }
    if ix <= 0x4139_21fb {
        let t = x.abs();
        let n = madd(t, INVPIO2, 0.5) as i32;
        let fn_ = n as f64;
        let mut r = madd(-fn_, PIO2_1, t);
        let mut w = fn_ * PIO2_1T;
        if n < 32 && ix != NPIO2_HW[(n - 1) as usize] {
            y[0] = r - w;
        } else {
            let j = ix >> 20;
            y[0] = r - w;
            let mut i = j - ((high_word(y[0]) >> 20) & 0x7ff);
            if i > 16 {
                let t = r;
                w = fn_ * PIO2_2;
                r = t - w;
                w = madd(fn_, PIO2_2T, -((t - r) - w));
                y[0] = r - w;
                i = j - ((high_word(y[0]) >> 20) & 0x7ff);
                if i > 49 {
                    let t = r;
                    w = fn_ * PIO2_3;
                    r = t - w;
                    w = madd(fn_, PIO2_3T, -((t - r) - w));
                    y[0] = r - w;
                }
            }
        }
        y[1] = (r - y[0]) - w;
        if hx < 0 {
            y[0] = -y[0];
            y[1] = -y[1];
            return -n;
        }
        return n;
    }
    if ix >= 0x7ff0_0000 {
        y[0] = x - x;
        y[1] = y[0];
        return 0;
    }
    let e0 = (ix >> 20) - 1046;
    let mut z = from_words((ix - ((e0 as u32) << 20) as i32) as u32, low_word(x));
    let mut tx = [0f64; 3];
    for slot in tx.iter_mut().take(2) {
        *slot = (z as i32) as f64;
        z = (z - *slot) * TWO24;
    }
    tx[2] = z;
    let mut nx = 3;
    while tx[nx - 1] == 0.0 {
        nx -= 1;
    }
    let n = kernel_rem_pio2(&tx, y, e0, nx);
    if hx < 0 {
        y[0] = -y[0];
        y[1] = -y[1];
        return -n;
    }
    n
}


/// V8 `Math.cos(x)`.
pub(crate) fn cos(x: f64) -> f64 {
    let ix = high_word(x) & 0x7fff_ffff;
    if ix <= 0x3fe9_21fb {
        return kernel_cos(x, 0.0);
    }
    if ix >= 0x7ff0_0000 {
        return x - x;
    }
    let mut y = [0f64; 2];
    let n = rem_pio2(x, &mut y);
    match n & 3 {
        0 => kernel_cos(y[0], y[1]),
        1 => -kernel_sin(y[0], y[1], 1),
        2 => -kernel_cos(y[0], y[1]),
        _ => kernel_sin(y[0], y[1], 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn npio2_table_is_the_high_words_of_n_pi_over_2() {
        for (index, high) in NPIO2_HW.iter().enumerate() {
            let value = (index + 1) as f64 * std::f64::consts::FRAC_PI_2;
            assert_eq!(high_word(value), *high, "n = {}", index + 1);
        }
    }
}
