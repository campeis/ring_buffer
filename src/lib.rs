//! Concurrent multi producer - multi consumer ring buffer based bounded queue
//!
//! Provides a concurrent queue based on a lock free ring buffer.
//! It allows to reserve an item in the queue to produce/consume values.
//!
//! The implementation is:
//! - fast
//! - blocking

mod tracking_cursor;

use crossbeam_utils::CachePadded;
use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use tracking_cursor::{ReservedForCursor, TrackingCursor};

///The queue access point
pub struct RingBuffer {}

impl RingBuffer {
    /// Returns a new a tuple containing a Producer and a Consumer
    ///
    /// # Arguments
    ///
    /// * `size` - The maximum number of elements in the queue. it will be pre-allocated. The size will be rounded to the next power of 2 to allow for faster math.
    pub fn create<T: Default>(size: usize) -> (RingBufferProducer<T>, RingBufferConsumer<T>)
    where
        T: Default + Clone,
    {
        let produce_tracker = Arc::new(TrackingCursor::leader(size));
        let consume_tracker = Arc::new(TrackingCursor::follower(size));

        let size = produce_tracker.size();

        let mut buffer = Vec::with_capacity(size);
        for _ in 0..size {
            buffer.push(UnsafeCell::default());
        }
        let buffer = Arc::new(buffer);

        (
            RingBufferProducer::new(
                buffer.clone(),
                produce_tracker.clone(),
                consume_tracker.clone(),
            ),
            RingBufferConsumer::new(buffer, produce_tracker, consume_tracker),
        )
    }
}

pub enum ReservationErr {
    NoAvailableSlot,
}

impl From<tracking_cursor::ReservationErr> for ReservationErr {
    fn from(value: tracking_cursor::ReservationErr) -> Self {
        match value {
            tracking_cursor::ReservationErr::NoAvailableSlot => Self::NoAvailableSlot,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RingBufferProducer<T>
where
    T: Default,
{
    buffer: Arc<Vec<UnsafeCell<CachePadded<T>>>>,
    produce_tracker: Arc<TrackingCursor>,
    consume_tracker: Arc<TrackingCursor>,
}
unsafe impl<T: Default> Send for RingBufferProducer<T> {}
unsafe impl<T: Default> Sync for RingBufferProducer<T> {}

impl<T> RingBufferProducer<T>
where
    T: Default,
{
    fn new(
        buffer: Arc<Vec<UnsafeCell<CachePadded<T>>>>,
        produce_tracker: Arc<TrackingCursor>,
        consume_tracker: Arc<TrackingCursor>,
    ) -> Self {
        Self {
            buffer,
            produce_tracker,
            consume_tracker,
        }
    }

    /// Returns a ProduceGuard that could be used to push a new value in the queue. If the queue is full will block until a new item has been consumed and freed.
    pub fn reserve_produce(&self) -> ProduceGuard<T> {
        let reservation = self.produce_tracker.advance_cursor();
        ProduceGuard::new(self, reservation)
    }

    /// Returns a ProduceGuard that could be used to push a new value in the queue. If the queue is full will return ReservationErr::NoAvailableSlot.
    pub fn try_reserve_produce(&self) -> Result<ProduceGuard<T>, ReservationErr> {
        let reservation = self.produce_tracker.try_advance_cursor()?;
        Ok(ProduceGuard::new(self, reservation))
    }

    /// Returns the number of available entities in the producer
    pub fn size(&self) -> usize {
        self.buffer.len()
    }
}

#[derive(Debug, Clone)]
pub struct RingBufferConsumer<T>
where
    T: Default,
{
    buffer: Arc<Vec<UnsafeCell<CachePadded<T>>>>,
    produce_tracker: Arc<TrackingCursor>,
    consume_tracker: Arc<TrackingCursor>,
}

unsafe impl<T: Default> Send for RingBufferConsumer<T> {}
unsafe impl<T: Default> Sync for RingBufferConsumer<T> {}

impl<T> RingBufferConsumer<T>
where
    T: Default,
{
    fn new(
        buffer: Arc<Vec<UnsafeCell<CachePadded<T>>>>,
        produce_tracker: Arc<TrackingCursor>,
        consume_tracker: Arc<TrackingCursor>,
    ) -> Self {
        Self {
            buffer,
            produce_tracker,
            consume_tracker,
        }
    }

    /// Returns a ConsumeGuard that could be used to pull a new value from the queue. If the queue is empty will block until a new item has been added to the queue.
    pub fn reserve_consume(&self) -> ConsumeGuard<T> {
        let reservation = self.consume_tracker.advance_cursor();
        ConsumeGuard::new(self, reservation)
    }

    /// Returns ConsumeGuard that could be used to pull a new value from the queue. If the queue is empty will return ReservationErr::NoAvailableSlot.
    pub fn try_reserve_consume(&self) -> Result<ConsumeGuard<T>, ReservationErr> {
        let reservation = self.consume_tracker.try_advance_cursor()?;
        Ok(ConsumeGuard::new(self, reservation))
    }
}

/// A guard that will keep the reservation valid until the guard is in scope.
/// When the reservation goes out of scope the iterm is made available for consumption.
/// # Examples
///
/// ```
/// use ring_buffer::RingBuffer;
///
/// let (producer, _) = RingBuffer::create::<usize>(1);
/// {
///     let mut reservation = producer.reserve_produce();
///     *reservation = 1usize;
/// }
/// ```
#[derive(Debug)]
pub struct ProduceGuard<'a, T>
where
    T: Default,
{
    producer: &'a RingBufferProducer<T>,
    reservation: ReservedForCursor,
}

impl<'a, T> ProduceGuard<'a, T>
where
    T: Default,
{
    fn new(
        state: &'a RingBufferProducer<T>,
        reservation: ReservedForCursor,
    ) -> ProduceGuard<'a, T> {
        ProduceGuard {
            producer: state,
            reservation,
        }
    }
}

impl<'a, T> Deref for ProduceGuard<'a, T>
where
    T: Default,
{
    type Target = T;

    fn deref(&self) -> &T {
        unsafe {
            self.producer.buffer[self.reservation.reserved_slot()]
                .get()
                .as_ref()
                .unwrap()
        }
    }
}

impl<'a, T> DerefMut for ProduceGuard<'a, T>
where
    T: Default,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            self.producer.buffer[self.reservation.reserved_slot()]
                .get()
                .as_mut()
                .unwrap()
        }
    }
}

impl<'a, T> Drop for ProduceGuard<'a, T>
where
    T: Default,
{
    fn drop(&mut self) {
        self.producer
            .consume_tracker
            .advance_target(&self.reservation);
    }
}

/// A guard that will keep the reservation valid until the guard is in scope.
/// When the reservation goes out of scope the iterm is freed for producers to use again.
/// # Examples
///
/// ```
/// use ring_buffer::RingBuffer;
///
/// let (producer, consumer) = RingBuffer::create::<usize>(1);
/// {
///     let mut produce = producer.reserve_produce();
///     *produce = 1usize;
/// }
///
/// {
///     let consume = consumer.reserve_consume();
///     assert_eq!(1usize, *consume);
/// }   
/// ```
#[derive(Debug)]
pub struct ConsumeGuard<'a, T>
where
    T: Default,
{
    consumer: &'a RingBufferConsumer<T>,
    reservation: ReservedForCursor,
}

impl<'a, T> ConsumeGuard<'a, T>
where
    T: Default,
{
    fn new(
        state: &'a RingBufferConsumer<T>,
        reservation: ReservedForCursor,
    ) -> ConsumeGuard<'a, T> {
        ConsumeGuard {
            consumer: state,
            reservation,
        }
    }
}

impl<'a, T> Deref for ConsumeGuard<'a, T>
where
    T: Default,
{
    type Target = T;

    fn deref(&self) -> &T {
        unsafe {
            self.consumer.buffer[self.reservation.reserved_slot()]
                .get()
                .as_ref()
                .unwrap()
        }
    }
}

impl<'a, T> Drop for ConsumeGuard<'a, T>
where
    T: Default,
{
    fn drop(&mut self) {
        self.consumer
            .produce_tracker
            .advance_target(&self.reservation)
    }
}
