
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use chrono::{Local};
use dashmap::{DashMap};

use dashmap::mapref::one::RefMut;


struct WeightedRoundRobin{
    weight: i64,
    current: AtomicI64,
    last_update: i64,
}

impl WeightedRoundRobin {
    pub fn new_with_weight(weight: i64) ->  WeightedRoundRobin{
        WeightedRoundRobin{
            weight,
            current: AtomicI64::new(0),
            last_update: 0
        }
    }
    fn increase_current(&self) -> i64{
        self.current.fetch_add(self.weight, Ordering::SeqCst)
    }
    fn sel(&self, total: i64) {
        let _ = self.current.fetch_add(-1 * total, Ordering::SeqCst);
    }
}

/// # Examples
/// ```
/// use concurrent_round_robin::WeightedRoundRobinSelector;
/// let balancer = Arc::new(WeightedRoundRobinSelector::new(
///     vec![("1", 1), ("2", 2)]
/// ));
/// for _i in 0..100 {
///     let arc = balancer.clone();
///     std::thread::spawn(move ||{
///         println!("{}", arc.select().unwrap().value());
///     });
/// }
/// ```
pub struct WeightedRoundRobinSelector<T> {
    size: AtomicUsize,
    elements_map: DashMap<usize, T>,
    weight_map: DashMap<usize, usize>,
    weighted_round_robin_map: DashMap<usize, WeightedRoundRobin>,
}

unsafe impl <T> Send for WeightedRoundRobinSelector<T> {}

impl <T> Drop for WeightedRoundRobinSelector<T>{
    fn drop(&mut self) {
        self.size.store(0, Ordering::Release);
    }
}


impl  <T> WeightedRoundRobinSelector<T>{

    pub fn new(elements_with_weight: Vec<(T, usize)>) ->  WeightedRoundRobinSelector<T>{
        let elements_map = DashMap::new();
        let weight_map = DashMap::new();
        let mut i = 0;
        for  (e, w) in elements_with_weight {
            elements_map.insert( i, e);
            weight_map.insert(i, w);
            i+=1;
        }
        WeightedRoundRobinSelector {
            size: AtomicUsize::new(i),
            elements_map, weight_map, weighted_round_robin_map: DashMap::new()
        }
    }

    pub fn close(self) -> Vec<(T, usize)> {
        let size = self.size.load(Ordering::Acquire);
        self.size.store(0, Ordering::Release);
        let mut vec = Vec::new();
        for i in 0..size {
            let x = self.elements_map.remove(&i).unwrap();
            vec.push((x.1, x.0));
        }
        return vec;
    }

    pub fn select(&self) -> Option<RefMut<usize, T>> {
        let mut total_weight: i64 = 0;
        let mut max_current = -1 << 63;
        let now = Local::now().timestamp();
        let mut selected_index: isize = -1;
        for i in 0..self.size.load(Ordering::Acquire) {
            let weight = self.weight_map.get(&i).unwrap().value().to_owned() as i64;
            let mut entry = self.weighted_round_robin_map.entry(i)
                .or_insert(WeightedRoundRobin::new_with_weight(
                    weight
                ));
            let value_mut = entry.value_mut();
            let cur = value_mut.increase_current();
            value_mut.last_update = now;
            if cur > max_current {
                max_current = cur;
                selected_index = i as isize;
            }
            total_weight += weight;
        }
        return if selected_index > -1 {
            let i = selected_index as usize;
            self.weighted_round_robin_map.get_mut(&i).unwrap().sel(total_weight);
            self.elements_map.get_mut(&i)
        } else {
            Option::None
        }
    }
}

#[test]
pub fn t(){
    use std::sync::Arc;
    let balancer = Arc::new(WeightedRoundRobinSelector::new(
        vec![("1", 1), ("2", 2)]
    ));
    for _i in 0..100000 {
        let arc = balancer.clone();
        std::thread::spawn(move ||{
            println!("{}", arc.select().unwrap().value());
        });
    }
}