## Decision: retain the identity contract with confirmed tradeoffs

Signed ROUND8.369ns/row versus true native100.852 (12.05x), unsigned ROUND8.410
versus101.790 (12.10x), unsigned ANY_VALUE10.726 versus122.044 (11.38x).
Same-current fallback ablations946–1093ns remain separately labeled; their
roughly102–113x ratios are not historical-native gains. Retain the complete
integer identity contract and its exact result flags, with scaled/noninteger
ROUND excluded. No f64 conversion or value-range gate is added.

Old controls regress: Duration13.495→15.048ns (~12%), Datetime10.265→10.863 and
Timestamp10.194→10.942 (~6–7%), JSON122.028→130.890 (~7%), Int12.898→13.421,
Real13.089→13.872, LOWER32.836→34.490 (~4–6%), Vector69.911→71.791 (~3%).
DecimalPLUS130.581→131.733 and comparisons74.1→75.9 are also slower. Longer
reverse-order confirmation finds Datetime10.186→10.854, Duration13.533→14.997,
JSON122.082→130.664; all630checksums agree. These are repeatable tradeoffs,
not dismissed as noise. The cause is not isolated. New functionality and its
11–12x native gains justify retention, not a universal improvement claim.

Independent M15 controls also retain old losses: Int12.852→13.406, Datetime
10.131→10.910, Duration14.421→15.191 and LOWER33.061→34.531. Every shape and
process range remains visible. Duration profiles point at constructor/import
and fixed-output append as major costs, not proof of a regression cause. Next
measure a shared output-buffer experiment; do not remove value normalization
checks to chase a profile percentage. No application QPS claim is made.

Retained TiKVd5817d0bfa15ccfad1fe93bea82e779dadf5e4c9 / this TiDB report commit.
Private result facts replace the old Boolean; no duplicate contract/driver or
migration fallback remains. Existing signed/unsigned fixed-result families
keep their rules. Benchmark schema selection is test-only; all original control
schemas/data are unchanged.
