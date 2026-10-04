// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The session's `time_zone` as a value the temporal code can convert with:
//! the Rust shape of the `*time.Location` Go threads out of
//! `SessionVars.Location()` (`pkg/sessionctx/variable/session.go`) and into
//! `types.Context` (`pkg/types/context.go`).
//!
//! # Why it lives here and implements `chrono::TimeZone`
//!
//! Go has exactly ONE zone type. `time.FixedZone("+08:00", 8*3600)` and
//! `time.LoadLocation("America/Los_Angeles")` are both `*time.Location`, so
//! every function that takes a zone -- `Time.ConvertTimeZone`,
//! `tablecodec.flatten`/`unflatten`, `codec.EncodeKey`, `ParseTime` --
//! takes the same parameter and needs no case analysis.
//!
//! Rust's `chrono` splits the two: a fixed offset is `FixedOffset` and an
//! IANA zone is `chrono_tz::Tz`, and they are distinct types. Matching on
//! the pair at each call site would put a two-arm `match` in front of every
//! conversion in the engine -- and, worse, would let a call site silently
//! handle only one arm. Implementing [`TimeZone`] for the union once
//! restores Go's shape: there is one zone type, it goes anywhere a zone
//! goes, and the DST-aware arm cannot be forgotten.
//!
//! The type sits in `tidb-datatype` rather than beside the session because
//! the storage codecs (`tidb-codec`, `tidb-tablecodec`) are the code that
//! needs it most and they are BELOW the session in the crate graph, exactly
//! as Go's `tablecodec` is below `sessionctx`.

pub use tidb_query_datatype::codec::mysql::time::{
    NativeSessionTimeZone as SessionTimeZone, NativeSessionTimeZoneOffset as SessionTimeZoneOffset,
};

#[cfg(any(test, doc))]
use chrono::TimeZone;
#[cfg(test)]
use chrono::{LocalResult, NaiveDateTime, Offset};

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn naive(text: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S").expect("parsable")
    }

    #[test]
    fn fixed_zone_projects_like_its_offset() {
        let zone = SessionTimeZone::Fixed {
            name: "+08:00".to_owned(),
            offset_secs: 8 * 3600,
        };
        let utc = naive("2020-01-03 07:16:59");
        let local = chrono::Utc
            .from_utc_datetime(&utc)
            .with_timezone(&zone)
            .naive_local();
        assert_eq!(local, naive("2020-01-03 15:16:59"));
    }

    /// The whole reason the named arm exists: a fixed offset would answer
    /// the same hour on both sides of a DST boundary.
    #[test]
    fn named_zone_follows_daylight_saving() {
        let zone = SessionTimeZone::Named(chrono_tz::America::Los_Angeles);
        let before = chrono::Utc
            .from_utc_datetime(&naive("2021-03-14 09:30:00"))
            .with_timezone(&zone)
            .naive_local();
        let after = chrono::Utc
            .from_utc_datetime(&naive("2021-03-14 11:30:00"))
            .with_timezone(&zone)
            .naive_local();
        assert_eq!(before, naive("2021-03-14 01:30:00"));
        assert_eq!(after, naive("2021-03-14 04:30:00"));
    }

    /// A local time inside the spring-forward gap does not exist, and the
    /// verdict has to survive the lift or the DST diagnostic is lost.
    #[test]
    fn nonexistent_local_time_stays_none() {
        let zone = SessionTimeZone::Named(chrono_tz::America::Los_Angeles);
        assert!(matches!(
            zone.offset_from_local_datetime(&naive("2021-03-14 02:30:00")),
            LocalResult::None
        ));
    }

    /// A local time the fall-back repeats is ambiguous, and `chrono`'s
    /// earliest-wins resolution is what Go's `time.Date` picks too.
    #[test]
    fn repeated_local_time_stays_ambiguous() {
        let zone = SessionTimeZone::Named(chrono_tz::America::Los_Angeles);
        assert!(matches!(
            zone.offset_from_local_datetime(&naive("2021-11-07 01:30:00")),
            LocalResult::Ambiguous(_, _)
        ));
    }

    #[test]
    fn utc_is_recognised_in_both_spellings() {
        assert!(SessionTimeZone::utc().is_utc());
        assert!(SessionTimeZone::Named(chrono_tz::UTC).is_utc());
        assert!(!SessionTimeZone::Named(chrono_tz::America::Los_Angeles).is_utc());
        assert!(!SessionTimeZone::Fixed {
            name: "+00:00".to_owned(),
            offset_secs: 0,
        }
        .is_utc());
        assert!(!SessionTimeZone::Fixed {
            name: "+08:00".to_owned(),
            offset_secs: 8 * 3600,
        }
        .is_utc());
    }

    #[test]
    fn dates_project_through_both_arms() {
        let day = NaiveDate::from_ymd_opt(2021, 7, 1).expect("valid date");
        for zone in [
            SessionTimeZone::Named(chrono_tz::America::Los_Angeles),
            SessionTimeZone::Fixed {
                name: "-07:00".to_owned(),
                offset_secs: -7 * 3600,
            },
        ] {
            assert_eq!(
                zone.offset_from_utc_date(&day).fix().local_minus_utc(),
                -7 * 3600
            );
            assert!(matches!(
                zone.offset_from_local_date(&day),
                LocalResult::Single(_) | LocalResult::Ambiguous(_, _)
            ));
        }
    }
}
