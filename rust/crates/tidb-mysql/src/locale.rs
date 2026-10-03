// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Locale-aware `FORMAT()` number grouping and separators.
//! The shared implementation preserves the public SDK's full string/byte domain.

pub use tidb_query_datatype::codec::mysql::locale::{
    format_by_locale, locale_format_style, LocaleFormatStyle,
};
