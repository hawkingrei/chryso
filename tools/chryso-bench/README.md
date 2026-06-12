# Chryso Benchmark Tool

`chryso-bench` is an opt-in benchmark runner for Chryso. It is intentionally
kept outside the root Cargo workspace so the default developer checks do not
pull in DuckDB or benchmark data generator dependencies.

## TPC-H

Generate a small TPC-H data set:

```sh
cd tools/chryso-bench
cargo run -- tpch generate --scale-factor 0.01 --out ../../target/chryso-bench/tpch --threads 4
```

Run the Chryso-compatible TPC-H query suite against the generated data:

```sh
cd tools/chryso-bench
cargo run -- tpch run --data-dir ../../target/chryso-bench/tpch --iterations 3
```

Generate data and run the suite in one command:

```sh
cd tools/chryso-bench
cargo run -- tpch all --scale-factor 0.01 --work-dir ../../target/chryso-bench/tpch --iterations 3
```

The current suite is a Chryso parser/planner smoke suite over TPC-H tables, not
the full official 22-query benchmark. It is meant to measure the integrated
SQL-to-physical-plan path and executor adapter while the parser and planner
continue to grow.

Data generation uses the `tpchgen` Rust library directly and writes CSV files.
The `--threads` option is accepted for CLI stability, but the current direct
generator writes tables sequentially.

## TPC-C

TPC-C is reserved as a first-class subcommand, but it is not implemented yet.
There is no mature embedded Rust TPC-C generator crate in the current Cargo
ecosystem comparable to `tpchgen-cli`; the runner returns an explicit error
until Chryso grows either a small native TPC-C workload generator or a stable
adapter around an external implementation.
