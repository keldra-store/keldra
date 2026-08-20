# This crate has moved

`anvil-storage` is now [`keldra`](https://crates.io/crates/keldra).

Use the new crate name for current releases:

```sh
cargo remove anvil-storage
cargo add keldra
```

The Rust module path changes from `anvil_storage` to `keldra`. Keldra's current
client guide and examples are available in the
[`keldra` documentation](https://docs.rs/keldra).

This final `anvil-storage` release only preserves source compatibility while
users update their dependency name. New development and documentation continue
under Keldra.
