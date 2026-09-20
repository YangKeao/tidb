# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Small source fixtures for the deletion inventory's scope classification."""
import importlib.util
from pathlib import Path
import tempfile
import textwrap
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).with_name("classify-native-eval-sites.py")
SPEC = importlib.util.spec_from_file_location("native_sites", SCRIPT)
CLASSIFIER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CLASSIFIER)


class ScopeTests(unittest.TestCase):
    def check_scopes(self, source, **expected):
        source = textwrap.dedent(source)
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "sample.rs").write_text(source, encoding="utf-8")
            with patch.object(CLASSIFIER, "ROOT", directory):
                for marker, wanted in expected.items():
                    with self.subTest(marker=marker):
                        line = next(i for i, text in enumerate(source.splitlines(), 1)
                                    if f"// {marker}" in text)
                        self.assertEqual(CLASSIFIER.is_test_only("sample.rs", line), wanted)

    def test_helper_attribute_does_not_taint_later_method(self):
        self.check_scopes("""
            impl Executor {
                #[cfg(test)]
                fn helper() { expr.eval(ctx, row); } // helper
                fn production() { expr.eval(ctx, row); } // live
            }
        """, helper=True, live=False)

    def test_test_only_field_does_not_taint_production(self):
        self.check_scopes("""
            struct Executor {
                #[cfg(test)]
                counter: usize,
                live: usize,
            }
            fn production() { expr.eval(ctx, row); } // live
        """, live=False)

    def test_last_field_without_comma_does_not_taint_next_item(self):
        self.check_scopes("""
            struct Executor { #[cfg(test)] counter: usize }
            fn production() { expr.eval(ctx, row); } // live
        """, live=False)

    def test_test_module_ends_before_production_impl(self):
        self.check_scopes("""
            #[cfg(test)]
            mod checks {
                fn nested() { expr.eval(ctx, row); } // gated
            }
            impl Executor { fn next() { expr.eval(ctx, row); } } // live
        """, gated=True, live=False)

    def test_file_wide_inner_attribute(self):
        self.check_scopes("""
            #![cfg(test)]
            fn fixture() { expr.eval(ctx, row); } // gated
        """, gated=True)

    def test_module_inner_attribute_stays_in_its_module(self):
        self.check_scopes("""
            mod checks {
                #![cfg(test)]
                fn fixture() { expr.eval(ctx, row); } // gated
            }
            fn live() { expr.eval(ctx, row); } // live
        """, gated=True, live=False)

    def test_inline_attribute_and_shared_source_line(self):
        self.check_scopes("""
            #[cfg(test)] fn a() { expr.eval(ctx, row); } // gated
            #[cfg(test)] fn b() { expr.eval(ctx, row); } fn c() { expr.eval(ctx, row); } // mixed
        """, gated=True, mixed=False)

    def test_only_cfg_conditions_that_exclude_non_test_builds(self):
        self.check_scopes("""
            #[cfg(all(test, feature = "engine"))]
            fn a() { expr.eval(ctx, row); } // gated
            #[cfg(any(test, feature = "engine"))]
            fn b() { expr.eval(ctx, row); } // maybe_live
            #[cfg(not(test))]
            fn c() { expr.eval(ctx, row); } // live
            #[cfg(not(not(test)))]
            fn d() { expr.eval(ctx, row); } // double_not
        """, gated=True, maybe_live=False, live=False, double_not=True)

    def test_cfg_attr_is_conservative(self):
        self.check_scopes("""
            #[cfg_attr(feature = "engine", cfg(test))]
            fn a() { expr.eval(ctx, row); } // unknown
        """, unknown=False)

    def test_comments_are_not_attributes_or_delimiters(self):
        self.check_scopes("""
            // #[cfg(test)] mod phantom {
            /* outer /* nested #[cfg(test)] } */ } */
            fn live() { expr.eval(ctx, row); } // live
        """, live=False)

    def test_literals_and_nested_comments_do_not_change_item_bounds(self):
        self.check_scopes(r'''
            #[cfg(test)]
            fn fixture<'a>(input: &'a str) {
                let _ = "} escaped quote \" {";
                let _ = b"} /*";
                let _ = r##"} \" #[cfg(test)] {"##;
                let _ = br#"} //"#;
                let _ = cr#"} /*"#;
                let _ = '}';
                let _ = b'{';
                let _ = '\'';
                let _ = '\u{7d}';
                /* } /* } */ } */
                expr.eval(ctx, row); // gated
            }
            fn live() { expr.eval(ctx, row); } // live
        ''', gated=True, live=False)

    def test_literal_cfg_text_is_not_an_attribute(self):
        self.check_scopes(r'''
            const TEXT: &str = r#"#[cfg(test)] mod phantom {"#;
            fn live() { expr.eval(ctx, row); } // live
        ''', live=False)

    def test_multiple_attributes_and_external_module_declaration(self):
        self.check_scopes('''
            #[cfg(test)]
            #[allow(dead_code)]
            fn fixture() { expr.eval(ctx, row); } // gated
            #[cfg(test)]
            #[path = "support.rs"]
            mod support;
            fn live() { expr.eval(ctx, row); } // live
        ''', gated=True, live=False)

    def test_unknown_item_boundary_is_not_file_wide_exclusion(self):
        self.check_scopes("""
            #[cfg(test)]
            unknown_macro!(tokens);
            fn live() { expr.eval(ctx, row); } // live
        """, live=False)

    def test_unbalanced_item_is_conservative(self):
        self.check_scopes("""
            #[cfg(test)]
            fn incomplete() {
                expr.eval(ctx, row); // uncertain
        """, uncertain=False)

    def test_test_attribute_bounds_only_its_function(self):
        self.check_scopes("""
            #[test]
            fn fixture() { expr.eval(ctx, row); } // gated
            fn live() { expr.eval(ctx, row); } // live
        """, gated=True, live=False)

    def test_unsupported_generic_item_is_retained_conservatively(self):
        self.check_scopes("""
            #[cfg(test)]
            fn fixture<T, U>() { expr.eval(ctx, row); } // uncertain
            fn live() { expr.eval(ctx, row); } // live
        """, uncertain=False, live=False)

    def test_existing_test_file_conventions(self):
        for path in ["./crate/tests/all.rs", "crate/src/tests.rs", "crate/src/join_tests.rs"]:
            with self.subTest(path=path):
                self.assertTrue(CLASSIFIER.is_test_only(path, 1))


if __name__ == "__main__":
    unittest.main()
