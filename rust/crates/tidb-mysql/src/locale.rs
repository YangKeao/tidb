// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Locale-aware `FORMAT()` number grouping and separators.

/// Mirrors Go `unicode.IsDigit` for password and locale classification.
pub fn is_unicode_decimal_digit(character: char) -> bool {
    let character = u32::from(character);
    UNICODE_DECIMAL_DIGITS
        .get(UNICODE_DECIMAL_DIGITS.partition_point(|&(_, end)| end < character))
        .is_some_and(|&(start, _)| start <= character)
}

const UNICODE_DECIMAL_DIGITS: &[(u32, u32)] = &[
    (0x30, 0x39),
    (0x660, 0x669),
    (0x6F0, 0x6F9),
    (0x7C0, 0x7C9),
    (0x966, 0x96F),
    (0x9E6, 0x9EF),
    (0xA66, 0xA6F),
    (0xAE6, 0xAEF),
    (0xB66, 0xB6F),
    (0xBE6, 0xBEF),
    (0xC66, 0xC6F),
    (0xCE6, 0xCEF),
    (0xD66, 0xD6F),
    (0xDE6, 0xDEF),
    (0xE50, 0xE59),
    (0xED0, 0xED9),
    (0xF20, 0xF29),
    (0x1040, 0x1049),
    (0x1090, 0x1099),
    (0x17E0, 0x17E9),
    (0x1810, 0x1819),
    (0x1946, 0x194F),
    (0x19D0, 0x19D9),
    (0x1A80, 0x1A89),
    (0x1A90, 0x1A99),
    (0x1B50, 0x1B59),
    (0x1BB0, 0x1BB9),
    (0x1C40, 0x1C49),
    (0x1C50, 0x1C59),
    (0xA620, 0xA629),
    (0xA8D0, 0xA8D9),
    (0xA900, 0xA909),
    (0xA9D0, 0xA9D9),
    (0xA9F0, 0xA9F9),
    (0xAA50, 0xAA59),
    (0xABF0, 0xABF9),
    (0xFF10, 0xFF19),
    (0x104A0, 0x104A9),
    (0x10D30, 0x10D39),
    (0x11066, 0x1106F),
    (0x110F0, 0x110F9),
    (0x11136, 0x1113F),
    (0x111D0, 0x111D9),
    (0x112F0, 0x112F9),
    (0x11450, 0x11459),
    (0x114D0, 0x114D9),
    (0x11650, 0x11659),
    (0x116C0, 0x116C9),
    (0x11730, 0x11739),
    (0x118E0, 0x118E9),
    (0x11950, 0x11959),
    (0x11C50, 0x11C59),
    (0x11D50, 0x11D59),
    (0x11DA0, 0x11DA9),
    (0x11F50, 0x11F59),
    (0x16A60, 0x16A69),
    (0x16AC0, 0x16AC9),
    (0x16B50, 0x16B59),
    (0x1D7CE, 0x1D7FF),
    (0x1E140, 0x1E149),
    (0x1E2F0, 0x1E2F9),
    (0x1E4F0, 0x1E4F9),
    (0x1E950, 0x1E959),
    (0x1FBF0, 0x1FBF9),
];

/// Separator/grouping rules for a MySQL locale.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocaleFormatStyle {
    /// Thousands separator.
    pub thousands_separator: &'static str,
    /// Decimal point.
    pub decimal_point: &'static str,
    /// Whether grouping is 3 digits then repeated 2-digit groups.
    pub indian_grouping: bool,
}
const COMMA_DOT: LocaleFormatStyle = LocaleFormatStyle {
    thousands_separator: ",",
    decimal_point: ".",
    indian_grouping: false,
};
const DOT_COMMA: LocaleFormatStyle = LocaleFormatStyle {
    thousands_separator: ".",
    decimal_point: ",",
    indian_grouping: false,
};
const SPACE_COMMA: LocaleFormatStyle = LocaleFormatStyle {
    thousands_separator: " ",
    decimal_point: ",",
    indian_grouping: false,
};
const NONE_COMMA: LocaleFormatStyle = LocaleFormatStyle {
    thousands_separator: "",
    decimal_point: ",",
    indian_grouping: false,
};
const APOS_DOT: LocaleFormatStyle = LocaleFormatStyle {
    thousands_separator: "'",
    decimal_point: ".",
    indian_grouping: false,
};
const APOS_COMMA: LocaleFormatStyle = LocaleFormatStyle {
    thousands_separator: "'",
    decimal_point: ",",
    indian_grouping: false,
};
const NONE_DOT: LocaleFormatStyle = LocaleFormatStyle {
    thousands_separator: "",
    decimal_point: ".",
    indian_grouping: false,
};
const INDIAN: LocaleFormatStyle = LocaleFormatStyle {
    thousands_separator: ",",
    decimal_point: ".",
    indian_grouping: true,
};

const COMMA_DOT_LOCALES: &[&str] = &[
    "aa_et", "af_za", "ak_gh", "am_et", "ar_ae", "ar_bh", "ar_dz", "ar_eg", "ar_in", "ar_iq",
    "ar_jo", "ar_kw", "ar_lb", "ar_ly", "ar_ma", "ar_om", "ar_qa", "ar_sd", "ar_ss", "ar_sy",
    "ar_tn", "ar_ye", "az_ir", "bi_vu", "bo_cn", "bo_in", "cy_gb", "dv_mv", "en_ag", "en_au",
    "en_bw", "en_ca", "en_gb", "en_hk", "en_ie", "en_il", "en_ng", "en_nz", "en_ph", "en_sg",
    "en_us", "en_za", "en_zm", "en_zw", "es_do", "es_gt", "es_hn", "es_ni", "es_pa", "es_pr",
    "es_sv", "es_us", "fa_ir", "ga_ie", "gd_gb", "gu_in", "gv_gb", "ha_ng", "he_il", "hi_in",
    "hy_am", "ig_ng", "ik_ca", "iu_ca", "ja_jp", "km_kh", "kn_in", "ko_kr", "ks_in", "kw_gb",
    "lg_ug", "lo_la", "mi_nz", "mr_in", "ms_my", "mt_mt", "my_mm", "ne_np", "nr_za", "om_et",
    "om_ke", "pa_in", "pa_pk", "sa_in", "sd_in", "si_lk", "sm_ws", "so_et", "so_ke", "so_so",
    "ss_za", "st_za", "sw_ke", "sw_tz", "th_th", "ti_et", "tk_tm", "tl_ph", "tn_za", "to_to",
    "ts_za", "ug_cn", "ur_in", "ur_pk", "ve_za", "xh_za", "yi_us", "yo_ng", "zh_cn", "zh_hk",
    "zh_sg", "zh_tw", "zu_za", "an_es", "az_az", "ca_ad", "ca_fr", "ca_it", "de_it", "en_dk",
    "es_pe", "ff_sn", "fy_de", "fy_nl", "ka_ge", "kl_gl", "ku_tr", "lb_lu", "li_be", "li_nl",
    "nl_aw", "sc_it", "se_no", "sq_mk", "tg_tj", "tr_cy", "wa_be", "br_fr", "kk_kz", "nn_no",
    "oc_fr", "uz_uz", "bs_ba", "el_cy", "es_cu", "ln_cd", "mg_mg", "rw_rw", "sr_me", "wo_sn",
    "es_mx", "ce_ru", "cv_ru", "ht_ht", "ia_fr", "ky_kg", "os_ru", "tt_ru", "aa_dj", "aa_er",
    "so_dj", "ti_er", "ps_af", "kv_ru", "su_id",
];
const DOT_COMMA_LOCALES: &[&str] = &[
    "be_by", "da_dk", "de_be", "de_de", "de_lu", "es_ar", "es_bo", "es_cl", "es_co", "es_ec",
    "es_es", "es_py", "es_uy", "es_ve", "fo_fo", "hu_hu", "id_id", "is_is", "lt_lt", "mn_mn",
    "ro_ro", "ru_ua", "sq_al", "tr_tr", "vi_vn", "nb_no", "uk_ua", "no_no",
];
const SPACE_COMMA_LOCALES: &[&str] = &[
    "cs_cz", "es_cr", "et_ee", "fi_fi", "lv_lv", "mk_mk", "ru_ru", "sk_sk", "sv_fi", "sv_se",
];
const NONE_COMMA_LOCALES: &[&str] = &[
    "el_gr", "gl_es", "pt_pt", "sl_si", "ca_es", "de_at", "eu_es", "fr_be", "hr_hr", "it_it",
    "nl_be", "nl_nl", "pt_br", "fr_ca", "fr_fr", "fr_lu", "pl_pl", "fr_ch", "bg_bg",
];

/// Returns a locale's rules and whether the case-insensitive locale was found.
/// Unknown locales use the source `en_US` fallback while returning false.
pub fn locale_format_style(locale: &str) -> (LocaleFormatStyle, bool) {
    let locale = crate::to_lowercase(locale);
    let key = locale.as_str();
    if COMMA_DOT_LOCALES.contains(&key) {
        (COMMA_DOT, true)
    } else if DOT_COMMA_LOCALES.contains(&key) {
        (DOT_COMMA, true)
    } else if SPACE_COMMA_LOCALES.contains(&key) {
        (SPACE_COMMA, true)
    } else if NONE_COMMA_LOCALES.contains(&key) {
        (NONE_COMMA, true)
    } else {
        match key {
            "de_ch" => (APOS_DOT, true),
            "it_ch" => (APOS_COMMA, true),
            "ar_sa" | "sr_rs" => (NONE_DOT, true),
            "en_in" | "ta_in" | "te_in" => (INDIAN, true),
            _ => (COMMA_DOT, false),
        }
    }
}

fn standard_grouping_bytes(integer: &[u8], separator: &[u8]) -> Vec<u8> {
    let first = match integer.len() % 3 {
        0 if !integer.is_empty() => 3,
        value => value,
    };
    let mut result = Vec::with_capacity(integer.len() + integer.len() / 3 * separator.len());
    result.extend_from_slice(&integer[..first]);
    for position in (first..integer.len()).step_by(3) {
        result.extend_from_slice(separator);
        result.extend_from_slice(&integer[position..position + 3]);
    }
    result
}
fn indian_grouping_bytes(integer: &[u8], separator: &[u8]) -> Vec<u8> {
    if integer.len() <= 3 {
        return integer.to_owned();
    }
    let split = integer.len() - 3;
    let remaining = &integer[..split];
    let mut first = remaining.len() % 2;
    if first == 0 && !remaining.is_empty() {
        first = 2
    }
    let mut result = Vec::with_capacity(integer.len() + integer.len() / 2 * separator.len());
    result.extend_from_slice(&remaining[..first]);
    for position in (first..remaining.len()).step_by(2) {
        result.extend_from_slice(separator);
        result.extend_from_slice(&remaining[position..position + 2]);
    }
    result.extend_from_slice(separator);
    result.extend_from_slice(&integer[split..]);
    result
}

/// Formats a decimal string using MySQL locale rules, returning the exact Go
/// string bytes and whether the locale was recognized. Numeric conversion
/// intentionally follows the source: it truncates at the first invalid
/// character and pads/truncates precision. The output is bytes because Go's
/// byte-counted grouping can split a UTF-8 rune.
pub fn format_by_locale(
    number: &str,
    precision: &str,
    locale: &str,
) -> Result<(Vec<u8>, bool), std::convert::Infallible> {
    let (style, found) = locale_format_style(locale);
    Ok((format_with_style(number, precision, style), found))
}

/// Round and format a SQL number, reporting unknown or NULL locale separately.
pub fn format_sql_number<'a>(
    number: &str,
    precision: i64,
    locale: Option<&'a str>,
) -> (Vec<u8>, Option<&'a str>) {
    let precision = precision.clamp(0, 30) as usize;
    let rounded = round_format_args(number, precision);
    let (style, found) = locale_format_style(locale.unwrap_or("en_US"));
    let unknown = match locale {
        None => Some("NULL"),
        Some(locale) if !found => Some(locale),
        _ => None,
    };
    (
        format_with_style(&rounded, &precision.to_string(), style),
        unknown,
    )
}

fn round_format_args(number: &str, precision: usize) -> String {
    let (negative, number) = number
        .strip_prefix('-')
        .map_or((false, number), |n| (true, n));
    let (mut integer, fraction) = number.split_once('.').unwrap_or((number, ""));
    if !integer.bytes().all(|digit| digit.is_ascii_digit())
        || !fraction.bytes().all(|digit| digit.is_ascii_digit())
    {
        integer = "0";
    }
    let mut fraction: Vec<u8> = fraction.bytes().take(precision).collect();
    while fraction.len() < precision {
        fraction.push(b'0');
    }
    let round_up = number
        .split_once('.')
        .and_then(|(_, f)| f.as_bytes().get(precision))
        .is_some_and(|d| *d >= b'5');
    if round_up {
        let mut carry = true;
        for digit in fraction.iter_mut().rev() {
            if *digit == b'9' {
                *digit = b'0';
            } else {
                *digit += 1;
                carry = false;
                break;
            }
        }
        if carry {
            let mut digits = integer.as_bytes().to_vec();
            for digit in digits.iter_mut().rev() {
                if *digit == b'9' {
                    *digit = b'0';
                } else {
                    *digit += 1;
                    carry = false;
                    break;
                }
            }
            if carry {
                return format_number_parts(
                    negative,
                    format!("1{}", "0".repeat(integer.len())),
                    fraction,
                );
            }
            return format_number_parts(negative, String::from_utf8(digits).unwrap(), fraction);
        }
    }
    format_number_parts(negative, integer.to_string(), fraction)
}

fn format_number_parts(negative: bool, integer: String, fraction: Vec<u8>) -> String {
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    out.push_str(&integer);
    if !fraction.is_empty() {
        out.push('.');
        out.push_str(std::str::from_utf8(&fraction).unwrap());
    }
    out
}

fn format_with_style(number: &str, precision: &str, style: LocaleFormatStyle) -> Vec<u8> {
    // The Go source applies unicode.IsDigit to rune(precision[0]), which is a
    // single byte converted to a rune. Consequently only an ASCII first digit
    // can start a precision, while subsequent runes use Unicode Nd membership.
    let precision = if precision.as_bytes()[0].is_ascii_digit() {
        &precision[..precision
            .char_indices()
            .find_map(|(i, c)| (!is_unicode_decimal_digit(c)).then_some(i))
            .unwrap_or(precision.len())]
    } else {
        "0"
    };
    let places = precision.parse::<u64>().ok();
    let normalized = if let Some(rest) = number.strip_prefix("-.") {
        format!("-0.{rest}")
    } else if let Some(rest) = number.strip_prefix('.') {
        format!("0.{rest}")
    } else {
        number.to_owned()
    };
    let number = normalized.as_str();
    // Go repeats the byte-to-rune conversion for the first number digit.
    let invalid = if let Some(rest) = number.strip_prefix('-') {
        !rest.as_bytes()[0].is_ascii_digit()
    } else {
        !number.as_bytes()[0].is_ascii_digit()
    };
    if invalid {
        let mut result = b"0".to_vec();
        if let Some(value) = places.filter(|v| *v > 0) {
            result.extend_from_slice(style.decimal_point.as_bytes());
            if let Ok(value) = usize::try_from(value) {
                result.extend(std::iter::repeat_n(b'0', value));
            }
        }
        return result;
    }
    let (sign, unsigned) = if let Some(rest) = number.strip_prefix('-') {
        ("-", rest)
    } else {
        ("", number)
    };
    let second_is_dot = unsigned.as_bytes().get(1) == Some(&b'.');
    let valid_end = unsigned
        .char_indices()
        .find_map(|(i, c)| {
            if is_unicode_decimal_digit(c) || i == 1 && second_is_dot || c == '.' && !second_is_dot
            {
                None
            } else {
                Some(i)
            }
        })
        .unwrap_or(unsigned.len());
    let valid = &unsigned[..valid_end];
    let parts: Vec<_> = valid.split('.').collect();
    let integer = parts[0];
    let grouped = if style.thousands_separator.is_empty() {
        integer.as_bytes().to_owned()
    } else if style.indian_grouping {
        indian_grouping_bytes(integer.as_bytes(), style.thousands_separator.as_bytes())
    } else {
        standard_grouping_bytes(integer.as_bytes(), style.thousands_separator.as_bytes())
    };
    let mut result = sign.as_bytes().to_vec();
    result.extend_from_slice(&grouped);
    if let Some(value) = places
        .filter(|v| *v > 0)
        .and_then(|v| usize::try_from(v).ok())
    {
        result.extend_from_slice(style.decimal_point.as_bytes());
        let fraction = if parts.len() == 2 { parts[1] } else { "" };
        let take = fraction.len().min(value);
        result.extend_from_slice(&fraction.as_bytes()[..take]);
        result.extend(std::iter::repeat_n(b'0', value - take));
    }
    result
}
