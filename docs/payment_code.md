# Payment codes

A payment code identifies a payment notice (`wsjrdp_payment_notices`) in
the remittance information of a transfer: a refund we pay, or a claim a
person pays. The bank statement import finds the code in the remittance
text and links the payment to its notice, also when a person typed the
code by hand and made a mistake.

Code: `packages/wsjrdp2027/src/wsjrdp2027/_payment_code.py` (exported from
`wsjrdp2027`), tests in `unit-tests/wsjrdp2027_tests/test_payment_code.py`.
The Hitobito wagon issues codes with the same rules.

## Format

```
RFpp DDDD DDDD CCCC        printed, e.g. RF75 K7M3 QX9P NFK3
RFppDDDDDDDDCCCC           electronic form, 16 characters
```

- `DDDDDDDD`: 8 random symbols of Crockford base32,
  `0123456789ABCDEFGHJKMNPQRSTVWXYZ` (no `I L O U`); a symbol's value is
  its index. 32^8 ≈ 1.1 · 10^12 codes.
- `CCCC`: 4 Reed-Solomon parity symbols over the 8 data symbols.
- `RFpp`: ISO 11649 creditor reference; `pp` are the ISO 7064 MOD 97-10
  check digits over the 12-symbol codeword.

All symbols are allowed in SEPA remittance information. Moss offers only
the unstructured remittance information, so the code is always part of the
remittance text, e.g. `TN <id> AB<n> RZ<n> RF75 K7M3 QX9P NFK3`.

## Reed-Solomon parameters

| Parameter | Value |
|---|---|
| Field | GF(32), symbols 0–31 |
| Primitive polynomial | x^5 + x^2 + 1 (`0x25`) |
| Generator | α = 2 |
| First consecutive root (fcr) | 1 |
| Code | RS(12,8), shortened from RS(31,27), systematic (data first) |
| Minimum distance | 5: any two codewords differ in at least 5 symbols |

Python uses `reedsolo` 1.7.0:
`RSCodec(nsym=4, c_exp=5, prim=0x25, generator=2, fcr=1)`.
`reedsolo` keeps its field tables in module globals, so the process uses
this one codec only and no other field size.

Test vectors (data, codeword, code, printed form) are in
`unit-tests/wsjrdp2027_tests/data/payment_code_vectors.json`, written by
`payment_code_test_vectors()`. Another implementation must reproduce
them exactly.

## Issuing a code

1. Draw 8 random data symbols.
2. Append the 4 parity symbols and prefix the RF check digits.
3. Compare the codeword with the codewords of all issued codes. The draw
   is kept only when every issued codeword is at
   - a Hamming distance of at least 8 (`min_hamming_distance`), and
   - a Levenshtein distance of at least 4 (`min_levenshtein_distance`).
4. Otherwise draw again, at most 10 times (`max_attempts`). After the last
   draw `generate_distinct_payment_code` returns the draw farthest from the issued
   codes (largest Hamming distance first, then largest Levenshtein
   distance); with `raise_when_too_close=True` it raises
   `PaymentCodeError` instead. A draw equal to an issued code is never
   returned.

Reed-Solomon alone guarantees a Hamming distance of 5. Requiring 8 between
issued codes lets matching correct up to 3 wrong symbols uniquely and
recognise 4 as ambiguous instead of correcting them wrongly. The
Levenshtein distance keeps a missing or extra symbol from leading to
another issued code.

The probability that a draw is rejected grows with the number of issued
codes: about 2 % at 1,000 codes, 18 % at 10,000 (1.2 draws on average;
more than 10 rejected draws in a row about 3 · 10^-8). Checking one draw
against 10,000 codes takes a few milliseconds.

### Codes without the RF prefix

The codeword is usable on its own, apart from payments:

| Function | Makes |
|---|---|
| `encode_rs_codeword(data, parity_length=4)` | the data symbols followed by the parity symbols |
| `generate_rs_codeword(data_length=8, parity_length=4)` | a random code |
| `generate_distinct_rs_codeword(issued_codes, data_length=8, parity_length=4, …)` | a random code far enough from the issued ones, with the same distance options as `generate_distinct_payment_code` |
| `is_valid_rs_codeword(code, data_length=8, parity_length=4)` | whether a code is a codeword of these lengths |

Data and parity together are at most 31 symbols (GF(32)). A code with p
parity symbols has a minimum distance of p + 1; the default distances for
issuing suit the default lengths and are to be chosen anew for others.
`generate_distinct_payment_code` takes its codeword from
`generate_distinct_rs_codeword`; it takes the same `data_length` and
`parity_length` (default 8 and 4), together at most 21 symbols, the most an
RF reference holds. Finding, checking and matching typed codes take the
same two arguments; a code is always matched with the lengths it was
issued with.

## Matching a typed code

`find_payment_codes(text)` finds `RF` followed by two check digits and the
codeword, ignoring case, spaces and hyphens and reading `O` as `0` and `I`/`L`
as `1`. It offers the 12-symbol codeword and, for missing or extra symbols,
codewords up to 2 symbols shorter or longer (`max_edit_correction`).

`match_payment_code(typed, issued_codes)` then tries in this order:

| Step | Accepts | `method` |
|---|---|---|
| The codeword is an issued codeword | — | `exact` |
| Reed-Solomon decoding gives an issued codeword | up to `parity_length // 2` wrong symbols, 2 by default (a swap of two neighbours counts as 2) | `reed_solomon` |
| Exactly one issued codeword at the smallest Hamming distance | up to `max_hamming_correction` wrong symbols, 3 by default | `hamming` |
| Exactly one issued codeword at the smallest Damerau-Levenshtein distance (codewords of other lengths included) | up to `max_edit_correction` edits, 2 by default, a missing or extra symbol included | `edit` |

`find_payment_codes`, `is_valid_payment_code` and `match_payment_code` take
`data_length` and `parity_length` (default 8 and 4) like the generators.
How many corrections are unique depends on the distances kept when
issuing: with a minimum Hamming distance h, up to (h − 1) // 2 wrong
symbols lead to one issued code only.

The RF check digits do not decide a match; `check_digits_ok` says whether
the typed ones were right. A match found by correction has `corrected`
set and is to be reviewed by a person. Where several issued codes fit
equally well, `ambiguous` lists them and nothing is matched.

Distances use `rapidfuzz`; the RF check digits use `python-stdnum`.
