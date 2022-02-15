# concurrent-round-robin
> A concurrent wrr implementation for rust


## quick start

```toml
[dependencies]
concurrent-round-robin="0.1"
```

```rust
use concurrent_round_robin::WeightedRoundRobinSelector;

fn main() {
    let balancer = Arc::new(WeightedRoundRobinSelector::new(
        vec![("1", 1), ("2", 2)]
    ));
    for _i in 0..100 {
        let arc = balancer.clone();
        std::thread::spawn(move || {
            println!("{}", arc.select().unwrap().value());
        });
    }
}
```