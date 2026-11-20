Title: `run` exits on the first materializer cycle that can't reach the catalog

Hi, I ran into this while testing catalog outages against 0.12.0.

With single-process `pg2iceberg run`, stopping the REST catalog (Lakekeeper 0.15.1) for 30 s makes the process exit with code 1 about 14 s later, on the first materializer cycle:

```text
Error: logical lifecycle

Caused by:
    0: materializer.cycle: catalog: other: catalog: other: Unexpected => Failed to execute http request, source: error sending request for url (http://lakekeeper:8181/catalog/v1/<warehouse>/namespaces/public/tables/items)
```

The slot goes inactive and nothing resumes when the catalog comes back. A manual restart recovers cleanly, with no lost or duplicated rows.

The same config in split mode (`stream-only` + `materializer-only`) rides through the outage: the materializer logs "materializer cycle failed; will retry next interval" and commits once the catalog is back.

Looks like the difference is `Ticks::finish` in `crates/pg2iceberg-validate/src/runtime.rs` ("`cycle` failure is fatal", still there on main) vs the retry loop in `run_materializer_only` (`crates/pg2iceberg/src/run.rs`). `docs/architecture/materializer.md` reads as if a failed table is retried next cycle, so I'd have expected `run` to retry too.

Repro (Postgres 17, Lakekeeper 0.15.1, Silo, pg2iceberg 0.12.0, all official images): https://github.com/AndreaBozzo/convergence-rig/tree/main/repro/pg2iceberg-catalog-outage. It fails the same way 3 of 3 runs; `./repro.sh split` is the comparison.

Happy to send a PR if retrying in `run` is the behaviour you want.

Filed 2026-11-20 as https://github.com/polynya-dev/pg2iceberg/issues/154
