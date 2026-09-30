#!/usr/bin/env python3
"""Verify TiKV-owned General/UCA/GB tables against TiDB's Go authorities.

Every mode requires TiKV's shared tables (default: sibling ``tikv`` repository;
override with ``--tikv-root``). Every General/UCA static slot, including
surrogates, and every u128 long expansion is compared with TiDB's Go authorities.
UCA 4.0 is also checked against the retained original Go fixture. GBK's 65,536
Go weights are compared numerically with TiKV's big-endian u16 table; GB18030's
1,114,112 little-endian u32 weights are byte-compared with the Go source data.
Source-pinned hashes and the former image tests' invariants remain
verification-only; no native collation image is generated.

Normal mode and ``--check`` are read-only and reject obsolete native images.
``--prune-obsolete`` still removes only the five exact General/UCA filenames.
The separate ``--prune-gb-images`` removes only the two former native GB CI
images. Both require all shared-source checks to pass first, and every present
image selected for pruning must be a regular file with its original pinned
length and SHA256. Unexpected modifications abort before deletion; neither
pruning mode silently removes the other family's images.

UCA 9.0's surrogate markers agree, but the unreachable non-scalar helper results
differ: Go's absent map entry is zero, TiKV's match fallback is 0xFFFD. This
explicitly checked distinction is not a claim of runtime weight equality.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import struct
import sys
from collections.abc import Iterable
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
CRATE = ROOT / "rust/crates/tidb-datatype"
OUTPUT = CRATE / "src/collation_data"
GENERAL_GO = ROOT / "pkg/util/collate/general_ci.go"
UCA_GO = ROOT / "pkg/util/collate/ucadata/unicode_ci_data_generated.go"
UCA_ORIGINAL_GO = ROOT / "pkg/util/collate/ucadata/unicode_ci_data_original_test.go"
UCA_0900_GO = ROOT / "pkg/util/collate/ucadata/unicode_0900_ai_ci_data_generated.go"
GBK_GO = ROOT / "pkg/util/collate/gbk_chinese_ci_data.go"
GB18030_DATA = ROOT / "pkg/util/collate/gb18030_weight.data"
TIKV_ROOT = ROOT.parent / "tikv"
TIKV_COLLATORS = Path("components/tidb_query_datatype/src/codec/collation/collator")
INTEGER = r"(?:0[xX][0-9a-fA-F_]+|[0-9][0-9_]*)"
GENERAL_PLANES = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x1E, 0x1F, 0x21, 0x24, 0xFF}

# Preserve generated_images_have_source_pinned_lengths_and_hashes without
# retaining or constructing migrated production images. Hash one record at a time.
PINNED_RECORDS = {
    "general_ci": ("<H", 65536, "787ea411c0600e485ae7dd52ce4b609848b5b832c179f2aed6deaf1e3a173d61"),
    "unicode_0400": ("<Q", 65536, "87fbb2751d6afe9ff48b4f19136204846e778dd88a1ba8ef8b2d5398354852b6"),
    "unicode_0400_long": ("<IQQ", 22, "fc2ea60aa8caa70d615fcdffaf1d8e1d3d2438eae11847d719266be88bb5d776"),
    "unicode_0900": ("<Q", 183969, "5ff4831e13e7485cff183e4e9971fd17e2719da0d675f8b38db8f02e89aaee7b"),
    "unicode_0900_long": ("<IQQ", 27, "8329421bd84ef04ad3ff5650e6b946d2cb22934d1fded231b7938bb094155c6f"),
    "gbk_chinese_ci": ("<H", 65536, "f6f63c33fa57eeaffa5d46841694adab58bd9cddfac3f92389dec4564a6036d6"),
}
GBK_GO_SHA256 = "f4c81f9fbf27469f4dc2b7add68c315bbc399869f63efdf059142eddb2542dc1"
GBK_TIKV_SHA256 = "936a6495ad2f211980bfb80cd1a52efb0cfbf04f68bfdd75f898d0aeba5336df"
GB18030_SHA256 = "64faeaa726d3555479fa98b7d61add86bbdcb659235da3ffacbbae4fb45d340d"

# Separate from OBSOLETE_IMAGES: --prune-obsolete must retain its original scope.
OBSOLETE_GB_IMAGES = {
    "gbk_chinese_ci_u16_le.bin": (131072, PINNED_RECORDS["gbk_chinese_ci"][2]),
    "gb18030_chinese_ci_u32_le.bin": (4456448, GB18030_SHA256),
}

# Deliberately enumerate exact generated filenames: never glob or recurse when
# pruning. Their source-pinned records also protect unexpected local edits.
OBSOLETE_IMAGES = {
    "general_ci_u16_le.bin": "general_ci",
    "unicode_0400_u64_le.bin": "unicode_0400",
    "unicode_0400_long_u64_le.bin": "unicode_0400_long",
    "unicode_0900_u64_le.bin": "unicode_0900",
    "unicode_0900_long_u64_le.bin": "unicode_0900_long",
}


def strip_comments(source: str) -> str:
    return re.sub(r"/\*.*?\*/|//[^\n]*", "", source, flags=re.S)


def numeric_values(source: str) -> list[int]:
    # Fail closed on unsupported syntax instead of silently dropping tokens.
    body = strip_comments(source).strip().removesuffix(",")
    tokens = [token.strip() for token in body.split(",")]
    if any(re.fullmatch(INTEGER, token) is None for token in tokens):
        raise ValueError("weight table contains a non-integer entry")
    return [int(token, 0) for token in tokens]


def between(source: str, start: str, end: str) -> str:
    _, found_start, tail = source.partition(start)
    body, found_end, _ = tail.partition(end)
    if not found_start or not found_end:
        raise ValueError(f"cannot find source delimiters {start!r} .. {end!r}")
    return body


def require_match(source: str, pattern: str, label: str) -> re.Match[str]:
    match = re.search(pattern, strip_comments(source), re.S)
    if match is None:
        raise ValueError(f"missing or unsupported {label}")
    return match


def verify_table(label: str, expected: list[int], actual: list[int]) -> None:
    if len(expected) != len(actual):
        raise ValueError(f"{label}: {len(actual)} entries, expected {len(expected)}")
    for codepoint, (want, got) in enumerate(zip(expected, actual)):
        if want != got:
            raise ValueError(
                f"{label} differs at U+{codepoint:04X}: {got:#x}, expected {want:#x}"
            )


def verify_pinned_records(name: str, rows: Iterable[tuple[int, ...]]) -> None:
    fmt, expected_count, expected_hash = PINNED_RECORDS[name]
    digest = hashlib.sha256()
    count = 0
    for row in rows:
        digest.update(struct.pack(fmt, *row))
        count += 1
    if count != expected_count or digest.hexdigest() != expected_hash:
        raise ValueError(
            f"{name} source-pinned records differ: count={count}, "
            f"sha256={digest.hexdigest()}, expected {expected_count}/{expected_hash}"
        )


def parse_general_ci() -> list[int]:
    source = GENERAL_GO.read_text()
    planes: dict[int, list[int]] = {}
    for name, body in re.findall(r"plane([0-9A-F]{2}) = \[\]uint16\{(.*?)\}", source, re.S):
        values = numeric_values(body)
        if len(values) != 256:
            raise ValueError(f"general_ci plane {name} has {len(values)} values, expected 256")
        if int(name, 16) in planes:
            raise ValueError(f"duplicate general_ci plane {name}")
        planes[int(name, 16)] = values

    expected_planes = GENERAL_PLANES
    if set(planes) != expected_planes:
        raise ValueError(f"unexpected general_ci planes: {sorted(planes)}")

    table_body = between(source, "planeTable = [][]uint16{", "}")
    table_entries = re.findall(r"plane[0-9A-F]{2}|nil", table_body)
    if len(table_entries) != 256:
        raise ValueError(f"general_ci plane table has {len(table_entries)} entries, expected 256")
    for index, entry in enumerate(table_entries):
        expected = f"plane{index:02X}" if index in expected_planes else "nil"
        if entry != expected:
            raise ValueError(f"general_ci plane table entry {index:#x} is {entry}, expected {expected}")

    return [planes[codepoint >> 8][codepoint & 0xFF] if codepoint >> 8 in planes else codepoint for codepoint in range(65536)]


def parse_long_map(
    source: str, start: str, end: str, expected_count: int
) -> list[tuple[int, int, int]]:
    body = strip_comments(between(source, start, end))
    pattern = rf"({INTEGER})\s*:\s*\{{\s*({INTEGER}),\s*({INTEGER})\s*\}}\s*,?"
    rows = [
        (int(rune, 0), int(first, 0), int(second, 0))
        for rune, first, second in re.findall(pattern, body)
    ]
    if re.sub(pattern, "", body).strip():
        raise ValueError("unsupported Go long-rune map entry")
    if len(rows) != expected_count:
        raise ValueError(
            f"long-rune map has {len(rows)} rows, expected {expected_count}"
        )
    if len({row[0] for row in rows}) != len(rows):
        raise ValueError("UCA long-rune map has duplicate runes")
    if len({row[1:] for row in rows}) != len(rows):
        raise ValueError("UCA long-rune map has duplicate weights")
    return sorted(rows)


def parse_uca_generated() -> tuple[list[int], list[tuple[int, int, int]]]:
    source = UCA_GO.read_text()
    table = numeric_values(between(source, "MapTable4: [65536]uint64{", "},\n\tLongRuneMap:"))
    if len(table) != 65536:
        raise ValueError(f"generated UCA 4.0 table has {len(table)} values, expected 65536")
    long_map = parse_long_map(
        source, "LongRuneMap: map[rune][2]uint64{", "\n\t},\n}", 22
    )
    markers = {index for index, value in enumerate(table) if value == 0xFFFD}
    long_runes = {row[0] for row in long_map}
    if markers != long_runes:
        raise ValueError(
            "UCA 4.0 long-rune markers and expansion records differ: "
            f"missing={sorted(markers - long_runes)}, extra={sorted(long_runes - markers)}"
        )
    return table, long_map


def verify_original(generated: list[int], generated_long: list[tuple[int, int, int]]) -> None:
    source = UCA_ORIGINAL_GO.read_text()
    original = numeric_values(between(source, "mapTable = []uint64{", "\n\t}\n\tlongRuneMap"))
    verify_table("generated UCA 4.0/original Go table", original, generated)
    original_long = parse_long_map(
        source, "longRuneMap = map[rune][]uint64{", "\n\t}\n)", 22
    )
    if generated_long != original_long:
        raise ValueError("generated UCA 4.0 long-rune map differs from original")


def parse_uca_0900() -> tuple[list[int], list[tuple[int, int, int]]]:
    source = UCA_0900_GO.read_text()
    table = numeric_values(
        between(source, "MapTable4: [183969]uint64{", "},\n\tLongRuneMap:")
    )
    if len(table) != 183969:
        raise ValueError(
            f"generated UCA 9.0 table has {len(table)} values, expected 183969"
        )
    long_map = parse_long_map(
        source, "LongRuneMap: map[rune][2]uint64{", "\n\t},\n}", 27
    )
    markers = {index for index, value in enumerate(table) if value == 0xFFFD}
    long_runes = {row[0] for row in long_map}
    surrogate_markers = set(range(0xD800, 0xE000))
    if markers != long_runes | surrogate_markers:
        raise ValueError(
            "UCA 9.0 long-rune markers and expansion records differ: "
            f"missing={sorted(markers - long_runes - surrogate_markers)}, "
            f"extra={sorted(long_runes - markers)}"
        )
    return table, long_map


def parse_gbk() -> list[int]:
    source = GBK_GO.read_text()
    table = numeric_values(
        between(
            source,
            "gbkChineseCISortKeyTable = [0xFFFF + 1]uint16{",
            "\n\t}\n)",
        )
    )
    if len(table) != 65536:
        raise ValueError(f"GBK CI table has {len(table)} values, expected 65536")
    return table


def parse_tikv_general_ci(source: str) -> list[int]:
    """Read the actual static planes and Option references, not a copied table."""
    source = strip_comments(source)
    planes: dict[int, list[int]] = {}
    for name, size, body in re.findall(
        rf"static\s+GENERAL_CI_PLANE_([0-9A-F]{{2}})\s*:\s*"
        rf"\[u16;\s*({INTEGER})\]\s*=\s*\[(.*?)\];", source, re.S
    ):
        plane = int(name, 16)
        values = numeric_values(body)
        if plane in planes or int(size, 0) != 256 or len(values) != 256:
            raise ValueError(f"invalid shared general_ci plane {name}: duplicate plane or non-256 length")
        planes[plane] = values
    if set(planes) != GENERAL_PLANES:
        raise ValueError(f"unexpected shared general_ci planes: {sorted(planes)}")

    body = require_match(
        source,
        r"static\s+GENERAL_CI_PLANE_TABLE\s*:\s*"
        r"\[Option<&\[u16;\s*256\]>;\s*256\]\s*=\s*\[(.*?)\];",
        "shared general_ci plane table (256 references to 256-entry planes)",
    ).group(1)
    entries = body.strip().removesuffix(",").split(",")
    if len(entries) != 256:
        raise ValueError(f"shared general_ci plane table has {len(entries)} entries, expected 256")
    for index, entry in enumerate(entries):
        expected = f"Some(&GENERAL_CI_PLANE_{index:02X})" if index in planes else "None"
        if re.sub(r"\s+", "", entry) != expected:
            raise ValueError(f"shared general_ci plane reference {index:#x}: expected {expected}")
    return [planes[cp >> 8][cp & 0xFF] if cp >> 8 in planes else cp for cp in range(65536)]


def parse_tikv_uca(
    source: str, expected_count: int
) -> tuple[list[int], list[tuple[int, int, int]], int]:
    """Return all raw slots, split u128 match arms, and the explicit fallback."""
    source = strip_comments(source)
    size, body = require_match(
        source,
        rf"static\s+UNICODE_CI_TABLE\s*:\s*\[u64;\s*({INTEGER})\]\s*=\s*\[(.*?)\];",
        "shared UCA static table",
    ).groups()
    table = numeric_values(body)
    if int(size, 0) != expected_count or len(table) != expected_count:
        raise ValueError(
            f"shared UCA table declares {int(size, 0)} entries and contains {len(table)}, "
            f"expected {expected_count}"
        )
    marker = require_match(
        source, rf"static\s+LONG_RUNE\s*:\s*u64\s*=\s*({INTEGER})\s*;",
        "shared UCA long-rune marker",
    ).group(1)
    if int(marker, 0) != 0xFFFD:
        raise ValueError(f"shared UCA long-rune marker is {marker}, expected 0xFFFD")

    body = require_match(
        source,
        r"fn\s+map_long_rune\(r:\s*usize\)\s*->\s*u128\s*\{\s*match\s+r\s*\{(.*?)\}\s*\}",
        "shared UCA u128 long-rune match",
    ).group(1)
    arms = body.strip().removesuffix(",").split(",")
    rows = []
    fallback = None
    for index, arm in enumerate(arms):
        match = re.fullmatch(rf"\s*({INTEGER}|_)\s*=>\s*({INTEGER})\s*", arm)
        if match is None:
            raise ValueError(f"unsupported shared UCA long-rune match arm: {arm.strip()!r}")
        rune, literal = match.groups()
        weight = int(literal, 0)
        if weight >= 1 << 128:
            raise ValueError(f"shared UCA long-rune weight exceeds u128: {literal}")
        if rune == "_":
            if index != len(arms) - 1:
                raise ValueError("shared UCA fallback must be the final match arm")
            fallback = weight
        else:
            rows.append((int(rune, 0), weight & ((1 << 64) - 1), weight >> 64))
    if fallback is None:
        raise ValueError("missing shared UCA long-rune fallback")
    if len({row[0] for row in rows}) != len(rows):
        raise ValueError("shared UCA long-rune match has duplicate runes")
    if len({row[1:] for row in rows}) != len(rows):
        raise ValueError("shared UCA long-rune match has duplicate weights")
    return table, sorted(rows), fallback


def require_fragment(source: str, fragment: str, label: str) -> None:
    # These are source metadata checks, not execution of a second collation kernel.
    compact = re.sub(r"\s+", "", strip_comments(source))
    if re.sub(r"\s+", "", fragment) not in compact:
        raise ValueError(f"changed or missing {label}: expected {fragment!r}")


def verify_uca_metadata(source: str, version: str) -> None:
    weight_body = between(source, "fn char_weight(ch: char) -> u128 {", "\n    }")
    require_fragment(
        weight_body,
        "let u = UNICODE_CI_TABLE[r]; if u == LONG_RUNE { return map_long_rune(r); } u as u128",
        f"shared UCA {version} table/long-map dispatch",
    )
    go_impl = (ROOT / f"pkg/util/collate/unicode_{version}_"
               f"{'ai_ci' if version == '0900' else 'ci'}_impl.go").read_text()
    if version == "0400":
        require_fragment(weight_body, "if r > 0xFFFF { return 0xFFFD; }", "shared UCA 4.0 boundary")
        require_fragment(go_impl, "if r > 0xFFFF { return 0xFFFD, 0 }", "Go UCA 4.0 boundary")
    else:
        # Deliberately retain the historical strict >: U+2CEA1 equals the
        # 183969-entry table's length and panics on both sides, not implicit weight.
        require_fragment(
            weight_body,
            "if r > UNICODE_CI_TABLE.len() { return (r as u128 >> 15) + 0xFBC0 "
            "+ (((r as u128 & 0x7FFF) | 0x8000) << 16); }",
            "shared UCA 9.0 strict table boundary and implicit-weight metadata",
        )
        require_fragment(
            go_impl,
            "if int(r) > len(ucadata.DUCET0900Table.MapTable4) { "
            "return uint64(r>>15) + 0xFBC0 + (uint64((r&0x7FFF)|0x8000) << 16), 0 }",
            "Go UCA 9.0 strict table boundary and implicit-weight metadata",
        )
        require_fragment(
            go_impl,
            "first = ucadata.DUCET0900Table.MapTable4[r] "
            "if first == ucadata.LongRune8 { return ucadata.DUCET0900Table.LongRuneMap[r][0], "
            "ucadata.DUCET0900Table.LongRuneMap[r][1] } return first, 0",
            "Go UCA 9.0 missing-map zero-value lookup",
        )
        require_fragment(
            (UCA_GO.parent / "data.go").read_text(), "LongRune8 = 0xFFFD",
            "Go UCA long-rune marker",
        )


def read_shared_source(path: Path) -> str:
    try:
        return path.read_text()
    except OSError as error:
        raise ValueError(
            f"cannot read required shared collation source {path}: {error}; "
            "provide the TiKV checkout with --tikv-root (verification cannot be skipped)"
        ) from error


def verify_shared_weights(tikv_root: Path = TIKV_ROOT) -> None:
    collators = tikv_root / TIKV_COLLATORS
    general = parse_general_ci()
    source = read_shared_source(collators / "utf8mb4_general_ci.rs")
    verify_table("shared general_ci/Go table", general, parse_tikv_general_ci(source))
    require_fragment(source, "if r > 0xFFFF { return 0xFFFD; }", "shared general_ci boundary")
    require_fragment(GENERAL_GO.read_text(), "if r > 0xFFFF { return 0xFFFD }", "Go general_ci boundary")
    verify_pinned_records("general_ci", ((weight,) for weight in general))

    for version, parse_go in (("0400", parse_uca_generated), ("0900", parse_uca_0900)):
        table, long_map = parse_go()
        if version == "0400":
            verify_original(table, long_map)
        source = read_shared_source(collators / f"utf8mb4_uca/data_{version}.rs")
        shared_table, shared_long, fallback = parse_tikv_uca(source, len(table))
        verify_table(f"shared UCA {version}/Go table (including surrogates)", table, shared_table)
        if shared_long != long_map:
            expected = {cp: (first, second) for cp, first, second in long_map}
            actual = {cp: (first, second) for cp, first, second in shared_long}
            cp = next(cp for cp in sorted(expected.keys() | actual.keys()) if expected.get(cp) != actual.get(cp))
            raise ValueError(
                f"shared UCA {version} long expansion differs at U+{cp:04X}: "
                f"{actual.get(cp)}, expected {expected.get(cp)} (low/high u64)"
            )
        if fallback != 0xFFFD:
            raise ValueError(f"shared UCA {version} long-rune fallback is {fallback:#x}, expected 0xFFFD")
        verify_uca_metadata(source, version)
        verify_pinned_records(f"unicode_{version}", ((weight,) for weight in table))
        verify_pinned_records(f"unicode_{version}_long", long_map)

        if version == "0900":
            # Preserve TestHangulJamoHasOnlyOneWeight and TestFirstIsNotZero.
            for cp in range(0x1100, 0x11FF):
                if shared_table[cp] & 0xFFFFFFFFFFFF0000:
                    raise ValueError(f"UCA 9.0 Hangul Jamo U+{cp:04X} has multiple weights")
            if any(first == 0 for _, first, _ in shared_long):
                raise ValueError("UCA 9.0 long expansion has a zero first u64")

            # Preserve the Go-only surrogate helper test without constructing
            # invalid Rust chars or treating TiKV's fallback as Go's map zero.
            go_map = {cp: (first, second) for cp, first, second in long_map}
            shared_map = {cp: first | (second << 64) for cp, first, second in shared_long}
            for cp in range(0xD800, 0xE000):
                if (table[cp] != 0xFFFD or shared_table[cp] != 0xFFFD
                        or cp in go_map or cp in shared_map
                        or go_map.get(cp, (0, 0)) != (0, 0)
                        or shared_map.get(cp, fallback) != 0xFFFD):
                    raise ValueError(f"changed UCA 9.0 non-scalar surrogate contract at U+{cp:04X}")


def verify_shared_gb_weights(tikv_root: Path = TIKV_ROOT) -> None:
    collators = tikv_root / TIKV_COLLATORS
    if hashlib.sha256(GBK_GO.read_bytes()).hexdigest() != GBK_GO_SHA256:
        raise ValueError("GBK CI Go source-pinned SHA256 differs")
    gbk = parse_gbk()
    # Preserve the former native LE image oracle without constructing an image.
    verify_pinned_records("gbk_chinese_ci", ((weight,) for weight in gbk))
    gbk_path = collators / "gbk_chinese_ci.data"
    shared_gbk = gbk_path.read_bytes()
    if len(shared_gbk) != 65536 * 2:
        raise ValueError(f"{gbk_path}: expected 65536 big-endian u16 weights")
    verify_table(
        "shared GBK CI/Go table", gbk,
        [weight for (weight,) in struct.iter_unpack(">H", shared_gbk)],
    )
    if hashlib.sha256(shared_gbk).hexdigest() != GBK_TIKV_SHA256:
        raise ValueError(f"{gbk_path}: canonical GBK CI SHA256 differs")

    gb18030 = GB18030_DATA.read_bytes()
    if len(gb18030) != 0x110000 * 4:
        raise ValueError(
            f"GB18030 CI table has {len(gb18030)} bytes, expected {0x110000 * 4}"
        )
    if hashlib.sha256(gb18030).hexdigest() != GB18030_SHA256:
        raise ValueError("GB18030 CI source-pinned SHA256 differs")
    gb18030_path = collators / "gb18030_chinese_ci.data"
    if gb18030_path.read_bytes() != gb18030:
        raise ValueError(f"{gb18030_path}: canonical GB18030 CI bytes differ from Go")


def reject_obsolete_gb_images() -> None:
    present = [
        name for name in OBSOLETE_GB_IMAGES
        if (OUTPUT / name).exists() or (OUTPUT / name).is_symlink()
    ]
    if present:
        raise ValueError(
            "duplicate native GB CI images must be absent: " + ", ".join(present)
            + "; use --prune-gb-images to remove verified original images"
        )


def prune_obsolete_gb_images() -> None:
    # Check both exact old images before deleting either; never rewrite TiKV data.
    pending: list[Path] = []
    for name, (expected_size, expected_hash) in OBSOLETE_GB_IMAGES.items():
        path = OUTPUT / name
        if path.is_symlink():
            raise ValueError(f"refusing to prune GB image symlink: {path}")
        if not path.exists():
            continue
        if not path.is_file():
            raise ValueError(f"refusing to prune non-file GB image: {path}")
        contents = path.read_bytes()
        if len(contents) != expected_size or hashlib.sha256(contents).hexdigest() != expected_hash:
            raise ValueError(f"refusing to prune modified GB image: {path}")
        pending.append(path)
    for path in pending:
        path.unlink()
        print(f"pruned {path.relative_to(ROOT)}")
    reject_obsolete_gb_images()
    print(f"pruned {len(pending)} duplicate native GB CI images")


def reject_obsolete_images() -> None:
    present = [
        name for name in OBSOLETE_IMAGES
        if (OUTPUT / name).exists() or (OUTPUT / name).is_symlink()
    ]
    if present:
        raise ValueError(
            "obsolete General/UCA images must be absent: " + ", ".join(present)
            + "; use --prune-obsolete to remove verified original images"
        )


def prune_obsolete_images() -> None:
    # Validate the entire exact set before unlinking even the first file.
    pending: list[tuple[Path, int]] = []
    for name, record in OBSOLETE_IMAGES.items():
        path = OUTPUT / name
        if path.is_symlink():
            raise ValueError(f"refusing to prune obsolete image symlink: {path}")
        if not path.exists():
            continue
        if not path.is_file():
            raise ValueError(f"refusing to prune non-file obsolete image: {path}")
        fmt, count, expected_hash = PINNED_RECORDS[record]
        expected_size = struct.calcsize(fmt) * count
        contents = path.read_bytes()
        if len(contents) != expected_size or hashlib.sha256(contents).hexdigest() != expected_hash:
            raise ValueError(f"refusing to prune modified obsolete image: {path}")
        pending.append((path, expected_size))
    for path, size in pending:
        path.unlink()
        print(f"pruned {path.relative_to(ROOT)} ({size} bytes)")
    reject_obsolete_images()
    print(f"pruned {len(pending)} obsolete images ({sum(size for _, size in pending)} bytes)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--check", action="store_true",
        help="verify shared General/UCA/GB sources and absence of native images without writing",
    )
    mode.add_argument(
        "--prune-obsolete", action="store_true",
        help="after verification, remove only the five unmodified obsolete General/UCA images",
    )
    mode.add_argument(
        "--prune-gb-images", action="store_true",
        help="after verification, remove only the two unmodified duplicate native GB CI images",
    )
    parser.add_argument(
        "--tikv-root", type=Path, default=TIKV_ROOT,
        help=f"required shared-table checkout (default: {TIKV_ROOT})",
    )
    args = parser.parse_args()
    try:
        # Finish all authority checks before either narrowly scoped cleanup.
        verify_shared_weights(args.tikv_root)
        verify_shared_gb_weights(args.tikv_root)
        if args.prune_obsolete:
            prune_obsolete_images()
        elif args.prune_gb_images:
            prune_obsolete_gb_images()
        else:
            reject_obsolete_images()
            reject_obsolete_gb_images()
    except (OSError, ValueError, struct.error) as error:
        print(f"collation source verification failed: {error}", file=sys.stderr)
        return 1

    print(
        "shared General/UCA static weights match Go sources: 65536 general_ci, "
        "65536 UCA 4.0 + 22 long expansions, 183969 UCA 9.0 + 27 long expansions"
    )
    print(
        "verified: original UCA 4.0 fixture; source-pinned lengths/SHA256; "
        "plane references; long markers/uniqueness; Hangul Jamo; nonzero long prefixes; "
        "table boundaries/implicit-weight metadata (U+2CEA1 retains strict > boundary)"
    )
    print(
        "verified non-scalar distinction: all 2048 U+D800..U+DFFF UCA 9.0 slots are "
        "0xFFFD markers; Go missing-map weight=(0,0), TiKV unreachable fallback=0xFFFD"
    )
    print(
        "TiKV-owned GB CI data match Go sources: 65536 GBK big-endian u16 weights, "
        "1114112 GB18030 little-endian u32 weights; original native SHA256 oracles retained"
    )
    print(f"verified canonical GB data in {args.tikv_root / TIKV_COLLATORS}")
    if not args.prune_gb_images:
        print("verified: all five obsolete General/UCA images are absent")
    if not args.prune_obsolete:
        print("verified: both duplicate native GB CI images are absent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
