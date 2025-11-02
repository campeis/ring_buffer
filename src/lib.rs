//! Concurrent multi producer - multi consumer ring buffer based bounded queue
//!
//! Provides a concurrent queue based on a lock free ring buffer.
//! It allows to reserve an item in the queue to produce/consume values.
//!
//! The implementation is:
//! - fast
//! - blocking

mod tracking_cursor;

use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Duration;

use tracking_cursor::{ReservedForCursor, TrackingCursor};

///The queue access point
pub struct RingBuffer {}

impl RingBuffer {
    /// Returns a new a tuple containing a Producer and a Consumer
    ///
    /// # Arguments
    ///
    /// * `size` - The maximum number of elements in the queue. it will be pre-allocated. The size will be rounded to the next power of 2 to allow for faster math.
    /// * `factory` - A function providing objects to put in the buffer..
    pub fn create<T>(
        size: usize,
        factory: fn() -> T,
    ) -> (RingBufferProducer<T>, RingBufferConsumer<T>) {
        let produce_tracker = Arc::new(TrackingCursor::leader(size));
        let consume_tracker = Arc::new(TrackingCursor::follower(size));

        let size = produce_tracker.size();

        let buffer = Self::init_vec_with_size(size, factory);

        (
            RingBufferProducer::new(
                buffer.clone(),
                produce_tracker.clone(),
                consume_tracker.clone(),
            ),
            RingBufferConsumer::new(buffer, produce_tracker, consume_tracker),
        )
    }

    pub fn create_with_intermediate_stage<T>(
        size: usize,
        factory: fn() -> T,
    ) -> (
        RingBufferProducer<T>,
        RingBufferConsumer<T>,
        RingBufferConsumer<T>,
    ) {
        let produce_tracker = Arc::new(TrackingCursor::leader(size));
        let intermediate_tracker = Arc::new(TrackingCursor::follower(size));
        let consume_tracker = Arc::new(TrackingCursor::follower(size));

        let size = produce_tracker.size();

        let buffer = Self::init_vec_with_size(size, factory);

        (
            RingBufferProducer::new(
                buffer.clone(),
                produce_tracker.clone(),
                intermediate_tracker.clone(),
            ),
            RingBufferConsumer::new(
                buffer.clone(),
                consume_tracker.clone(),
                intermediate_tracker.clone(),
            ),
            RingBufferConsumer::new(buffer, produce_tracker, consume_tracker),
        )
    }

    fn init_vec_with_size<T>(size: usize, factory: fn() -> T) -> Arc<UnsafeCell<Vec<T>>> {
        let mut buffer = Vec::with_capacity(size);
        for _ in 0..size {
            buffer.push(factory());
        }
        Arc::new(UnsafeCell::new(buffer))
    }
}

#[derive(Debug)]
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
pub struct RingBufferProducer<T> {
    buffer: Arc<UnsafeCell<Vec<T>>>,
    produce_tracker: Arc<TrackingCursor>,
    consume_tracker: Arc<TrackingCursor>,
}
unsafe impl<T> Send for RingBufferProducer<T> {}
unsafe impl<T> Sync for RingBufferProducer<T> {}

impl<T> RingBufferProducer<T> {
    fn new(
        buffer: Arc<UnsafeCell<Vec<T>>>,
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
    pub fn reserve_produce(&self) -> ProduceGuard<'_, T> {
        let reservation = self.produce_tracker.advance_cursor();
        unsafe {
            let reserved =
                &mut self.buffer.deref().get().as_mut().unwrap()[reservation.reserved_slot()];
            ProduceGuard::new(self, reservation, reserved)
        }
    }

    /// Returns a ProduceGuard that could be used to push a new value in the queue. If the queue is full will return ReservationErr::NoAvailableSlot.
    pub fn try_reserve_produce(&self) -> Result<ProduceGuard<'_, T>, ReservationErr> {
        let reservation = self.produce_tracker.try_advance_cursor()?;
        unsafe {
            let reserved =
                &mut self.buffer.deref().get().as_mut().unwrap()[reservation.reserved_slot()];
            Ok(ProduceGuard::new(self, reservation, reserved))
        }
    }

    /// Returns a ProduceGuard that could be used to push a new value in the queue. If the queue is full it will wait for a free spot
    /// until timeout expires. If no spot frees before timeout expires will return ReservationErr::NoAvailableSlot.
    /// The timeout is not an hard timeout, but just represents the minimum time this function is going to wait.
    pub fn try_reserve_produce_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<ProduceGuard<'_, T>, ReservationErr> {
        let reservation = self
            .produce_tracker
            .try_advance_cursor_with_timeout(timeout)?;
        unsafe {
            let reserved =
                &mut self.buffer.deref().get().as_mut().unwrap()[reservation.reserved_slot()];
            Ok(ProduceGuard::new(self, reservation, reserved))
        }
    }

    /// Returns the number of available entities in the producer
    pub fn size(&self) -> usize {
        self.produce_tracker.size()
    }
}

#[derive(Debug, Clone)]
pub struct RingBufferConsumer<T> {
    buffer: Arc<UnsafeCell<Vec<T>>>,
    produce_tracker: Arc<TrackingCursor>,
    consume_tracker: Arc<TrackingCursor>,
}

unsafe impl<T> Send for RingBufferConsumer<T> {}
unsafe impl<T> Sync for RingBufferConsumer<T> {}

impl<T> RingBufferConsumer<T> {
    fn new(
        buffer: Arc<UnsafeCell<Vec<T>>>,
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
    pub fn reserve_consume(&self) -> ConsumeGuard<'_, T> {
        let reservation = self.consume_tracker.advance_cursor();

        unsafe {
            let reserved =
                &self.buffer.deref().get().as_ref().unwrap()[reservation.reserved_slot()];
            ConsumeGuard::new(self, reservation, reserved)
        }
    }

    /// Returns ConsumeGuard that could be used to pull a new value from the queue. If the queue is empty will return ReservationErr::NoAvailableSlot.
    pub fn try_reserve_consume(&self) -> Result<ConsumeGuard<'_, T>, ReservationErr> {
        let reservation = self.consume_tracker.try_advance_cursor()?;

        unsafe {
            let reserved =
                &self.buffer.deref().get().as_ref().unwrap()[reservation.reserved_slot()];
            Ok(ConsumeGuard::new(self, reservation, reserved))
        }
    }

    /// Returns ConsumeGuard that could be used to pull a new value from the queue. It will wait for an available entity until timeout expires.
    /// If the queue is empty when timeout expires will return ReservationErr::NoAvailableSlot.
    /// /// The timeout is not an hard timeout, but just represents the minimum time this function is going to wait.
    pub fn try_reserve_consume_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<ConsumeGuard<'_, T>, ReservationErr> {
        let reservation = self
            .consume_tracker
            .try_advance_cursor_with_timeout(timeout)?;

        unsafe {
            let reserved =
                &self.buffer.deref().get().as_ref().unwrap()[reservation.reserved_slot()];
            Ok(ConsumeGuard::new(self, reservation, reserved))
        }
    }
}

/// A guard that will keep the reservation valid until the guard is in scope.
/// When the reservation goes out of scope the iterm is made available for consumption.
/// # Examples
///
/// ```
/// use ring_buffer::RingBuffer;
///
/// let (producer, _) = RingBuffer::create::<usize>(1, || 0);
/// {
///     let mut reservation = producer.reserve_produce();
///     *reservation = 1usize;
/// }
/// ```
#[derive(Debug)]
pub struct ProduceGuard<'a, T> {
    producer: &'a RingBufferProducer<T>,
    reservation: ReservedForCursor,
    reserved: &'a mut T,
}

impl<'a, T> ProduceGuard<'a, T> {
    fn new(
        state: &'a RingBufferProducer<T>,
        reservation: ReservedForCursor,
        reserved: &'a mut T,
    ) -> ProduceGuard<'a, T> {
        ProduceGuard {
            producer: state,
            reservation,
            reserved,
        }
    }
}

impl<'a, T> Deref for ProduceGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.reserved
    }
}

impl<'a, T> DerefMut for ProduceGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.reserved
    }
}

impl<'a, T> Drop for ProduceGuard<'a, T> {
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
/// let (producer, consumer) = RingBuffer::create::<usize>(1, || 0);
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
    consumer: &'a RingBufferConsumer<T>,
    reservation: ReservedForCursor,
    reserved: &'a T,
}

impl<'a, T> ConsumeGuard<'a, T> {
    fn new(
        state: &'a RingBufferConsumer<T>,
        reservation: ReservedForCursor,
        reserved: &'a T,
    ) -> ConsumeGuard<'a, T> {
        ConsumeGuard {
            consumer: state,
            reservation,
            reserved,
        }
    }
}

impl<'a, T> Deref for ConsumeGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.reserved
    }
}

impl<'a, T> Drop for ConsumeGuard<'a, T> {
    fn drop(&mut self) {
        self.consumer
            .produce_tracker
            .advance_target(&self.reservation)
    }
}
