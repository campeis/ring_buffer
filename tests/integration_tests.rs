use ring_buffer::{Consumer, Producer, RingBuffer};
use std::sync::{Arc, RwLock};
use std::thread;

#[derive(Debug, Default, Clone)]
struct TestStruct {
    value: usize,
}

#[test]
fn use_deref() {
    let buffer = RingBuffer::<TestStruct>::new(1024);
    let to_produce = buffer.size() * 100;

    let mut concurency: usize = num_cpus::get() / 2;
    if concurency == 0 {
        concurency = 2;
    }

    for i in 0..concurency {
        let buffer_copy = buffer.clone();
        thread::spawn(move || {
            for a in 0..to_produce {
                let mut producer = buffer_copy.reserve_produce();
                producer.value = a + to_produce * i;
            }
        });
    }
    let mut handlers = Vec::with_capacity(concurency);
    let total_received = Arc::new(RwLock::new(Vec::with_capacity(concurency * to_produce)));
    for _ in 0..concurency {
        let buffer_copy = buffer.clone();
        let total_received_copy = total_received.clone();
        handlers.push(thread::spawn(move || {
            let mut received = Vec::with_capacity(to_produce);
            for _ in 0..to_produce {
                received.push(buffer_copy.reserve_consume().value);
            }
            let guard = total_received_copy.write();
            guard.unwrap().append(&mut received);
        }));
    }
    for handler in handlers {
        let _ = handler.join();
    }
    let guard = total_received.write();
    let mut processed = guard.unwrap();
    assert_eq!(concurency * to_produce, processed.len());
    processed.sort();
    for i in 0..(concurency * to_produce) {
        assert_eq!(i, *processed.get(i).unwrap());
    }
}

#[test]
fn can_reserve_for_production_if_not_all_slots_are_full() {
    let buffer = RingBuffer::<TestStruct>::new(1024);

    let res = buffer.try_reserve_produce();
    assert!(res.is_ok())
}

#[test]
fn cant_reserve_for_production_if_all_slots_are_full() {
    let buffer = RingBuffer::<TestStruct>::new(1024);

    for i in 0..buffer.size() {
        buffer.reserve_produce().value = i;
    }

    let res = buffer.try_reserve_produce();
    assert!(res.is_err())
}

#[test]
fn can_reserve_for_consuming_if_there_is_an_available_slot() {
    let buffer = RingBuffer::<TestStruct>::new(1024);
    {
        let mut guard = buffer.reserve_produce();
        guard.value = 1;
    }
    let res = buffer.try_reserve_consume();
    assert!(res.is_ok())
}

#[test]
fn cant_reserve_for_consuming_if_all_slots_are_empty() {
    let buffer = RingBuffer::<TestStruct>::new(1024);
    let res = buffer.try_reserve_consume();
    assert!(res.is_err())
}
