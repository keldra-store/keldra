# Historical Keldra 0.9 index dependency record

> Superseded by the format-v6 memory-first index architecture. Keldra 0.16 no
> longer depends on `sux`; this file is retained only as the audit record for
> the removed format-v4 implementation.

This record covers the two dependencies deliberately adopted for the Keldra
0.9 native-segment index implementation. `Cargo.lock` remains the authoritative
source for exact checksums and the complete workspace graph.

## Keldra 0.9 direct choices

| Dependency | Locked release | Enabled features | License choice | Purpose |
| --- | --- | --- | --- | --- |
| [`sux`](https://crates.io/crates/sux) | `0.14.0` | default features disabled | Apache-2.0 from `Apache-2.0 OR LGPL-2.1-or-later` | Query-time Rank9/Select9 navigation over Keldra's portable dense-posting bitmap bytes |
| [`rayon`](https://crates.io/crates/rayon) | `1.12.0` | its normal library surface, reached through `sux`/`rdst` and used directly by Keldra's fixed index worker pool | Apache-2.0 from `MIT OR Apache-2.0` | Process-owned execution for admitted, bounded source-projection CPU work |

Keldra does not enable `sux`'s default `flate2` or `zstd` features, nor its
`serde`, `epserde`, `mmap`, `cli` or `deko` features. This keeps native
layout persistence and unused compression stacks out of the index format.
Keldra serializes its own architecture-independent format and reconstructs
checked `sux` structures from it.

The normal Linux release graph was recorded with:

```sh
cargo tree --locked -p sux --edges normal
cargo tree --locked -p keldra-index --edges features
```

The resolved `sux` closure is listed below. License expressions are copied from
the crates' Cargo metadata. Every disjunction has a permissive option; Keldra
uses Apache-2.0 where it is offered.

| Package | Version | Declared license |
| --- | --- | --- |
| `aho-corasick` | `1.1.4` | `Unlicense OR MIT` |
| `aliasable` | `0.1.3` | `MIT` |
| `allocator-api2` | `0.2.21` | `MIT OR Apache-2.0` |
| `ambassador` | `0.5.1` | `MIT OR Apache-2.0` |
| `anstream` | `1.0.0` | `MIT OR Apache-2.0` |
| `anstyle` | `1.0.14` | `MIT OR Apache-2.0` |
| `anstyle-parse` | `1.0.0` | `MIT OR Apache-2.0` |
| `anstyle-query` | `1.1.5` | `MIT OR Apache-2.0` |
| `anyhow` | `1.0.104` | `MIT OR Apache-2.0` |
| `arbitrary-chunks` | `0.4.1` | `Apache-2.0 OR MIT` |
| `arrayvec` | `0.7.8` | `MIT OR Apache-2.0` |
| `atomic-primitive` | `0.1.4` | `MIT OR Apache-2.0` |
| `bitflags` | `2.13.1` | `MIT OR Apache-2.0` |
| `bytemuck` | `1.25.2` | `Zlib OR Apache-2.0 OR MIT` |
| `bytemuck_derive` | `1.12.0` | `Zlib OR Apache-2.0 OR MIT` |
| `cfg-if` | `1.0.4` | `MIT OR Apache-2.0` |
| `chacha20` | `0.10.1` | `MIT OR Apache-2.0` |
| `colorchoice` | `1.0.5` | `MIT OR Apache-2.0` |
| `crossbeam-channel` | `0.5.16` | `MIT OR Apache-2.0` |
| `crossbeam-deque` | `0.8.7` | `MIT OR Apache-2.0` |
| `crossbeam-epoch` | `0.9.20` | `MIT OR Apache-2.0` |
| `crossbeam-utils` | `0.8.22` | `MIT OR Apache-2.0` |
| `darling` | `0.21.3` | `MIT` |
| `darling_core` | `0.21.3` | `MIT` |
| `darling_macro` | `0.21.3` | `MIT` |
| `derivative` | `2.2.0` | `MIT/Apache-2.0` |
| `derive_setters` | `0.1.9` | `MIT/Apache-2.0` |
| `dsi-progress-logger` | `0.8.9` | `Apache-2.0 OR MIT` |
| `either` | `1.17.0` | `MIT OR Apache-2.0` |
| `env_filter` | `2.0.0` | `MIT OR Apache-2.0` |
| `env_logger` | `0.11.11` | `MIT OR Apache-2.0` |
| `equivalent` | `1.0.2` | `Apache-2.0 OR MIT` |
| `fallible-iterator` | `0.3.0` | `MIT/Apache-2.0` |
| `fastrand` | `2.5.0` | `Apache-2.0 OR MIT` |
| `fnv` | `1.0.7` | `Apache-2.0 / MIT` |
| `foldhash` | `0.2.0` | `Zlib` |
| `getrandom` | `0.4.3` | `MIT OR Apache-2.0` |
| `hashbrown` | `0.16.1` | `MIT OR Apache-2.0` |
| `hashbrown` | `0.17.1` | `MIT OR Apache-2.0` |
| `ident_case` | `1.0.1` | `MIT/Apache-2.0` |
| `impl-tools` | `0.11.4` | `MIT/Apache-2.0` |
| `impl-tools-lib` | `0.11.4` | `MIT/Apache-2.0` |
| `indexmap` | `2.14.0` | `Apache-2.0 OR MIT` |
| `is_terminal_polyfill` | `1.70.2` | `MIT OR Apache-2.0` |
| `itertools` | `0.10.5` | `MIT/Apache-2.0` |
| `itertools` | `0.14.0` | `MIT OR Apache-2.0` |
| `itoa` | `1.0.18` | `MIT OR Apache-2.0` |
| `jiff` | `0.2.35` | `Unlicense OR MIT` |
| `jiff-core` | `0.1.0` | `Unlicense OR MIT` |
| `lambert_w` | `1.2.34` | `MIT OR Apache-2.0` |
| `lender` | `0.6.2` | `Apache-2.0 OR LGPL-2.1-or-later OR MIT` |
| `lender-derive` | `0.1.3` | `Apache-2.0 OR LGPL-2.1-or-later OR MIT` |
| `libc` | `0.2.189` | `MIT OR Apache-2.0` |
| `libm` | `0.2.16` | `MIT` |
| `linux-raw-sys` | `0.12.1` | `Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT` |
| `log` | `0.4.33` | `MIT OR Apache-2.0` |
| `maybe-dangling` | `0.1.2` | `Zlib OR MIT OR Apache-2.0` |
| `mem_dbg` | `0.4.4` | `Apache-2.0 OR MIT` |
| `mem_dbg-derive` | `0.3.4` | `Apache-2.0 OR MIT` |
| `memchr` | `2.8.3` | `Unlicense OR MIT` |
| `num-complex` | `0.4.6` | `MIT OR Apache-2.0` |
| `num-format` | `0.4.4` | `MIT/Apache-2.0` |
| `num-primitive` | `0.2.1` | `MIT OR Apache-2.0` |
| `num-traits` | `0.2.19` | `MIT OR Apache-2.0` |
| `once_cell` | `1.21.4` | `MIT OR Apache-2.0` |
| `partition` | `0.1.2` | `MIT OR Apache-2.0` |
| `pluralizer` | `0.5.0` | `MIT/Apache-2.0` |
| `proc-macro-error-attr2` | `2.0.0` | `MIT OR Apache-2.0` |
| `proc-macro-error2` | `2.0.1` | `MIT OR Apache-2.0` |
| `proc-macro2` | `1.0.107` | `MIT OR Apache-2.0` |
| `quote` | `1.0.47` | `MIT OR Apache-2.0` |
| `rand` | `0.10.2` | `MIT OR Apache-2.0` |
| `rand_core` | `0.10.1` | `MIT OR Apache-2.0` |
| `rayon` | `1.12.0` | `MIT OR Apache-2.0` |
| `rayon-core` | `1.13.0` | `MIT OR Apache-2.0` |
| `rdst` | `0.20.14` | `Apache-2.0 OR MIT` |
| `regex` | `1.13.1` | `MIT OR Apache-2.0` |
| `regex-automata` | `0.4.16` | `MIT OR Apache-2.0` |
| `regex-syntax` | `0.8.11` | `MIT OR Apache-2.0` |
| `rustix` | `1.1.4` | `Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT` |
| `rustversion` | `1.0.23` | `MIT OR Apache-2.0` |
| `stable_try_trait_v2` | `1.75.1` | `Apache-2.0 OR MIT` |
| `strsim` | `0.11.1` | `MIT` |
| `succinctly` | `0.6.0` | `MIT` |
| `sux` | `0.14.0` | `Apache-2.0 OR LGPL-2.1-or-later` |
| `syn` | `1.0.109` | `MIT OR Apache-2.0` |
| `syn` | `2.0.119` | `MIT OR Apache-2.0` |
| `syn` | `3.0.3` | `MIT OR Apache-2.0` |
| `sync-cell-slice` | `0.9.14` | `Apache-2.0 OR MIT` |
| `sysinfo` | `0.36.1` | `MIT` |
| `tempfile` | `3.27.0` | `MIT OR Apache-2.0` |
| `thiserror` | `2.0.19` | `MIT OR Apache-2.0` |
| `thiserror-impl` | `2.0.19` | `MIT OR Apache-2.0` |
| `unicode-ident` | `1.0.24` | `(MIT OR Apache-2.0) AND Unicode-3.0` |
| `utf8parse` | `0.2.2` | `Apache-2.0 OR MIT` |
| `value-traits` | `0.2.1` | `Apache-2.0 OR LGPL-2.1-or-later` |
| `value-traits-derive` | `0.2.1` | `Apache-2.0 OR LGPL-2.1-or-later` |
| `xxhash-rust` | `0.8.18` | `BSL-1.0` |
| `zerocopy` | `0.8.55` | `BSD-2-Clause OR Apache-2.0 OR MIT` |
| `zerocopy-derive` | `0.8.55` | `BSD-2-Clause OR Apache-2.0 OR MIT` |
