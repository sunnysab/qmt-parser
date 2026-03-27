use criterion::{criterion_group, criterion_main, Criterion};

fn bench_tick(_c: &mut Criterion) {}

criterion_group!(benches, bench_tick);
criterion_main!(benches);
