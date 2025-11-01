use ring_buffer::RingBuffer;
use rstest::rstest;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

#[derive(Debug, Default, Clone)]
struct TestStruct {
    value: usize,
}

#[rstest]
#[case::zero_below_min_size(0)]
#[case::one_below_min_size(1)]
#[case::min_size(2)]
#[case::medium(32)]
#[case::bigger(1024)]
#[timeout(Duration::from_secs(10))]
fn concurrent_test(#[case] num_slots: usize) {
    let (producer, consumer) = RingBuffer::create::<TestStruct>(num_slots, TestStruct::default);
    let to_produce = producer.size() * 100;

    let mut concurency: usize = num_cpus::get() / 2;
    if concurency == 0 {
        concurency = 2;
    }

    for i in 0..concurency {
        let producer_copy = producer.clone();
        thread::spawn(move || {
            for a in 0..to_produce {
                let mut producer = producer_copy.reserve_produce();
                producer.value = a + to_produce * i;
            }
        });
    }
    let mut handlers = Vec::with_capacity(concurency);
    let total_received = Arc::new(RwLock::new(Vec::with_capacity(concurency * to_produce)));
    for _ in 0..concurency {
        let consumer_copy = consumer.clone();
        let total_received_copy = total_received.clone();
        handlers.push(thread::spawn(move || {
            let mut received = Vec::with_capacity(to_produce);
            for _ in 0..to_produce {
                received.push(consumer_copy.reserve_consume().value);
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
    let (producer, _) = RingBuffer::create::<TestStruct>(1024, TestStruct::default);

    let res = producer.try_reserve_produce();
    assert!(res.is_ok())
}

#[test]
fn cant_reserve_for_production_if_all_slots_are_full() {
    let (producer, _) = RingBuffer::create::<TestStruct>(1024, TestStruct::default);

    for i in 0..producer.size() {
        producer.reserve_produce().value = i;
    }

    let res = producer.try_reserve_produce();
    assert!(res.is_err())
}

#[test]
fn cant_reserve_for_production_if_all_slots_are_full_after_timeut() {
    let (producer, _) = RingBuffer::create::<TestStruct>(1024, TestStruct::default);

    for i in 0..producer.size() {
        producer.reserve_produce().value = i;
    }

    let res = producer.try_reserve_produce_with_timeout(Duration::from_millis(1));
    assert!(res.is_err())
}

#[test]
fn can_reserve_for_consuming_if_there_is_an_available_slot() {
    let (producer, consumer) = RingBuffer::create::<TestStruct>(1024, TestStruct::default);
    {
        let mut guard = producer.reserve_produce();
        guard.value = 1;
    }
    let res = consumer.try_reserve_consume();
    assert!(res.is_ok())
}

#[test]
fn cant_reserve_for_consuming_if_all_slots_are_empty() {
    let (_, consumer) = RingBuffer::create::<TestStruct>(1024, TestStruct::default);
    let res = consumer.try_reserve_consume();
    assert!(res.is_err())
}

#[test]
fn cant_reserve_for_consuming_if_all_slots_are_empty_before_timeout_expires() {
    let (_, consumer) = RingBuffer::create::<TestStruct>(1024, TestStruct::default);
    let res = consumer.try_reserve_consume_with_timeout(Duration::from_millis(1));
    assert!(res.is_err())
}

#[test]
fn cant_reserve_for_consuming_if_intermediate_stage_did_not_consume() {
    let (producer, _, consumer) =
        RingBuffer::create_with_intermediate_stage::<TestStruct>(1024, TestStruct::default);

    {
        producer.try_reserve_produce().unwrap().value = 1;
    }

    assert!(consumer.try_reserve_consume().is_err())
}

#[test]
fn can_reserve_for_consuming_if_intermediate_stage_consumed() {
    let (producer, intermediate, consumer) =
        RingBuffer::create_with_intermediate_stage::<TestStruct>(1024, TestStruct::default);
    {
        producer.try_reserve_produce().unwrap().value = 1;
    }

    {
        assert_eq!(1, intermediate.try_reserve_consume().unwrap().value);
    }

    assert!(consumer.try_reserve_consume().is_ok());
}
