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
    pub fn create<T: Default + Sized>(
        size: usize,
    ) -> (
        impl Producer<T> + Send + Sync,
        impl Consumer<T> + Send + Sync,
    )
    where
        T: Default + Clone,
    {
        let state = Arc::new(RingBufferState::new(size));
        (
            RingBufferProducer::new(state.clone()),
            RingBufferConsumer::new(state),
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
pub trait Producer<T>: Clone {
    /// Returns a ProduceGuard that could be used to push a new value in the queue. If the queue is full will block until a new item has been consumed and freed.
    fn reserve_produce(&self) -> ProduceGuard<T>;

    /// Returns a ProduceGuard that could be used to push a new value in the queue. If the queue is full will return ReservationErr::NoAvailableSlot.
    fn try_reserve_produce(&self) -> Result<ProduceGuard<T>, ReservationErr>;

    /// Returns the number of available entities in the producer
    fn size(&self) -> usize;
}

pub trait Consumer<T>: Clone {
    /// Returns a ConsumeGuard that could be used to pull a new value from the queue. If the queue is empty will block until a new item has been added to the queue.
    fn reserve_consume(&self) -> ConsumeGuard<T>;

    /// Returns ConsumeGuard that could be used to pull a new value from the queue. If the queue is empty will return ReservationErr::NoAvailableSlot.
    fn try_reserve_consume(&self) -> Result<ConsumeGuard<T>, ReservationErr>;
}

#[derive(Clone)]
struct RingBufferProducer<T> {
    state: Arc<RingBufferState<T>>,
}
unsafe impl<T> Send for RingBufferProducer<T> {}
unsafe impl<T> Sync for RingBufferProducer<T> {}

impl<T> RingBufferProducer<T> {
    fn new(state: Arc<RingBufferState<T>>) -> Self {
        Self { state }
    }
}
impl<T> Producer<T> for RingBufferProducer<T>
where
    T: Default + Clone,
{
    fn reserve_produce(&self) -> ProduceGuard<T> {
        self.state.reserve_produce()
    }

    fn try_reserve_produce(&self) -> Result<ProduceGuard<T>, ReservationErr> {
        self.state.try_reserve_produce()
    }

    fn size(&self) -> usize {
        self.state.buffer.len()
    }
}

#[derive(Clone)]
struct RingBufferConsumer<T> {
    state: Arc<RingBufferState<T>>,
}

unsafe impl<T> Send for RingBufferConsumer<T> {}
unsafe impl<T> Sync for RingBufferConsumer<T> {}

impl<T> RingBufferConsumer<T> {
    fn new(state: Arc<RingBufferState<T>>) -> Self {
        Self { state }
    }
}
impl<T> Consumer<T> for RingBufferConsumer<T>
where
    T: Default + Clone,
{
    /// Returns a ConsumeGuard that could be used to pull a new value from the queue. If the queue is empty will block until a new item has been added to the queue.
    fn reserve_consume(&self) -> ConsumeGuard<T> {
        self.state.reserve_consume()
    }

    /// Returns ConsumeGuard that could be used to pull a new value from the queue. If the queue is empty will return ReservationErr::NoAvailableSlot.
    fn try_reserve_consume(&self) -> Result<ConsumeGuard<T>, ReservationErr> {
        self.state.try_reserve_consume()
    }
}

#[derive(Debug)]
struct RingBufferState<T> {
    buffer: Vec<UnsafeCell<CachePadded<T>>>,
    produce_tracker: TrackingCursor,
    consume_tracker: TrackingCursor,
}

impl<T> RingBufferState<T>
where
    T: Default,
{
    fn new(size: usize) -> RingBufferState<T> {
        let produce_tracker = TrackingCursor::leader(size);
        let consume_tracker = TrackingCursor::follower(size);

        let size = produce_tracker.size();

        let mut vec = Vec::with_capacity(size);
        for _ in 0..size {
            vec.push(UnsafeCell::default());
        }

        Self {
            buffer: vec,
            produce_tracker,
            consume_tracker,
        }
    }

    pub fn reserve_produce(&self) -> ProduceGuard<T> {
        let reservation = self.produce_tracker.advance_cursor();
        ProduceGuard::new(self, reservation)
    }

    pub fn try_reserve_produce(&self) -> Result<ProduceGuard<T>, ReservationErr> {
        let reservation = self.produce_tracker.try_advance_cursor()?;
        Ok(ProduceGuard::new(self, reservation))
    }

    pub fn reserve_consume(&self) -> ConsumeGuard<T> {
        let reservation = self.consume_tracker.advance_cursor();
        ConsumeGuard::new(self, reservation)
    }

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
/// use ring_buffer::{RingBuffer, Producer};
///
/// let (producer, _) = RingBuffer::create::<usize>(1);
/// {
///     let mut reservation = producer.reserve_produce();
///     *reservation = 1usize;
/// }
/// ```
#[derive(Debug)]
pub struct ProduceGuard<'a, T> {
    state: &'a RingBufferState<T>,
    reservation: ReservedForCursor,
}

impl<'a, T> ProduceGuard<'a, T> {
    fn new(state: &'a RingBufferState<T>, reservation: ReservedForCursor) -> ProduceGuard<'a, T> {
        ProduceGuard { state, reservation }
    }
}

impl<'a, T> Deref for ProduceGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe {
            self.state.buffer[self.reservation.reserved_slot()]
                .get()
                .as_ref()
                .unwrap()
        }
    }
}

impl<'a, T> DerefMut for ProduceGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            self.state.buffer[self.reservation.reserved_slot()]
                .get()
                .as_mut()
                .unwrap()
        }
    }
}

impl<'a, T> Drop for ProduceGuard<'a, T> {
    fn drop(&mut self) {
        self.state.consume_tracker.advance_target(&self.reservation);
    }
}

/// A guard that will keep the reservation valid until the guard is in scope.
/// When the reservation goes out of scope the iterm is freed for producers to use again.
/// # Examples
///
/// ```
/// use ring_buffer::{RingBuffer,Producer,Consumer};
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
pub struct ConsumeGuard<'a, T> {
    state: &'a RingBufferState<T>,
    reservation: ReservedForCursor,
}

impl<'a, T> ConsumeGuard<'a, T> {
    fn new(state: &'a RingBufferState<T>, reservation: ReservedForCursor) -> ConsumeGuard<'a, T> {
        ConsumeGuard { state, reservation }
    }
}

impl<'a, T> Deref for ConsumeGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe {
            self.state.buffer[self.reservation.reserved_slot()]
                .get()
                .as_ref()
                .unwrap()
        }
    }
}

impl<'a, T> Drop for ConsumeGuard<'a, T> {
    fn drop(&mut self) {
        self.state.produce_tracker.advance_target(&self.reservation)
    }
}
