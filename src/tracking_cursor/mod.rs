use crossbeam_utils::CachePadded;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

type AvailabilityStrategy<T> = dyn Fn(&T, usize, usize) -> bool;

pub(crate) struct TrackingCursor {
    size_mask: usize,
    page_flag: usize,
    overflow_mask: usize,
    cursor: CachePadded<AtomicUsize>,
    target: CachePadded<AtomicUsize>,
    availability_strategy: Box<AvailabilityStrategy<Self>>,
}

unsafe impl Send for TrackingCursor {}
unsafe impl Sync for TrackingCursor {}

#[derive(Debug)]
pub(crate) struct ReservedForCursor {
    reserved_slot: usize,
    from: usize,
    to: usize,
}

impl ReservedForCursor {
    pub(crate) fn reserved_slot(&self) -> usize {
        self.reserved_slot
    }
}

#[derive(Debug)]
pub(crate) enum ReservationErr {
    NoAvailableSlot,
}

impl TrackingCursor {
    pub(crate) fn leader(size: usize) -> Self {
        Self::new(size, Self::leader_availability_strategy)
    }
    pub(crate) fn follower(size: usize) -> Self {
        Self::new(size, Self::follower_availability_strategy)
    }

    fn new(size: usize, retry_strategy: impl Fn(&Self, usize, usize) -> bool + 'static) -> Self {
        const MINIMUM_BUFFER_SIZE: usize = 1024;
        let size = if size > (usize::MAX >> 1) {
            usize::MAX >> 1
        } else if size > MINIMUM_BUFFER_SIZE {
            size
        } else {
            MINIMUM_BUFFER_SIZE
        };

        let size_mask = usize::MAX >> (size - 1).leading_zeros();
        let page_flag = size_mask + 1;
        let overflow_mask = size_mask + page_flag;

        Self {
            size_mask,
            overflow_mask,
            page_flag,
            cursor: CachePadded::new(AtomicUsize::new(overflow_mask)),
            target: CachePadded::new(AtomicUsize::new(overflow_mask)),
            availability_strategy: Box::new(retry_strategy),
        }
    }

    pub(crate) fn advance_cursor(&self) -> ReservedForCursor {
        loop {
            if let Ok(reserved) = self.try_advance_cursor() {
                return reserved;
            }
        }
    }

    #[inline(always)]
    pub(crate) fn try_advance_cursor(&self) -> Result<ReservedForCursor, ReservationErr> {
        loop {
            let from = self.cursor.load(Ordering::Acquire);
            let actual_target = self.target.load(Ordering::Relaxed);

            let to = (from + 1) & self.overflow_mask;
            let first_available_target = (actual_target + 1) & self.overflow_mask;

            if (self.availability_strategy)(self, to, first_available_target) {
                return Err(ReservationErr::NoAvailableSlot);
            };

            if self
                .cursor
                .compare_exchange(from, to, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return Ok(ReservedForCursor {
                    reserved_slot: from & self.size_mask,
                    from,
                    to,
                });
            }
        }
    }

    pub(crate) fn try_advance_cursor_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<ReservedForCursor, ReservationErr> {
        let start_time = Instant::now();
        loop {
            match self.try_advance_cursor() {
                Ok(reserved) => return Ok(reserved),
                Err(err) => match err {
                    ReservationErr::NoAvailableSlot => {
                        if start_time.elapsed() > timeout {
                            return Err(ReservationErr::NoAvailableSlot);
                        }
                    }
                },
            }
        }
    }

    pub(crate) fn advance_target(&self, reserved: &ReservedForCursor) {
        loop {
            if self
                .target
                .compare_exchange(
                    reserved.from,
                    reserved.to,
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return;
            }
        }
    }

    pub(crate) fn size(&self) -> usize {
        self.size_mask + 1
    }

    fn leader_availability_strategy(&self, to: usize, first_available_target: usize) -> bool {
        (to & self.size_mask) == (first_available_target & self.size_mask)
            && (to & self.page_flag) != (first_available_target & self.page_flag)
    }

    fn follower_availability_strategy(&self, to: usize, first_available_target: usize) -> bool {
        to == first_available_target
    }
}

impl Debug for TrackingCursor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TrackingCursor (cursor={:?},target={:?},size_mask={:?},overflow_mask={:?},page_flag={:?})",
            self.cursor, self.target, self.size_mask, self.overflow_mask, self.page_flag,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::tracking_cursor::TrackingCursor;
    use rstest::{fixture, rstest};

    #[fixture]
    fn leader() -> TrackingCursor {
        TrackingCursor::leader(1)
    }

    #[fixture]
    fn follower() -> TrackingCursor {
        TrackingCursor::follower(1)
    }

    #[rstest]
    fn can_advance_cursor_if_empty(leader: TrackingCursor) {
        let res = leader.try_advance_cursor();
        assert!(res.is_ok());
    }

    #[rstest]
    fn cant_advance_cursor_if_full(leader: TrackingCursor) {
        for _ in 0..leader.size() {
            let res = leader.try_advance_cursor();
            assert!(res.is_ok());
        }

        let res = leader.try_advance_cursor();
        assert!(res.is_err());
    }

    #[rstest]
    fn advance_target_free_slot(leader: TrackingCursor) {
        let first_reservation = leader.advance_cursor();
        for _ in 1..leader.size() {
            let res = leader.try_advance_cursor();
            assert!(res.is_ok());
        }

        leader.advance_target(&first_reservation);

        let res = leader.try_advance_cursor();
        assert!(res.is_ok());
    }

    #[rstest]
    fn cant_advance_target_if_empty(follower: TrackingCursor) {
        let res = follower.try_advance_cursor();
        assert!(res.is_err());
    }

    #[rstest]
    fn can_advance_target_using_leader_reservation(
        leader: TrackingCursor,
        follower: TrackingCursor,
    ) {
        for _ in 0..leader.size() {
            let reservation = leader.try_advance_cursor().unwrap();
            follower.advance_target(&reservation);
        }

        let res = follower.try_advance_cursor();
        assert!(res.is_ok());
    }

    #[rstest]
    fn cant_advance_cursor_on_follower_if_target_has_not_been_advanced(follower: TrackingCursor) {
        let res = follower.try_advance_cursor();
        assert!(res.is_err());
    }

    #[rstest]
    fn advance_target_on_follower_allows_follower_reservation(
        leader: TrackingCursor,
        follower: TrackingCursor,
    ) {
        let leader_reservation = leader.try_advance_cursor().unwrap();
        follower.advance_target(&leader_reservation);

        let follower_res = follower.try_advance_cursor();
        assert!(follower_res.is_ok());
    }
}
