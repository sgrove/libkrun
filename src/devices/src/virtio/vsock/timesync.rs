use std::sync::{Arc, Mutex};
use std::thread;
use std::time;

use super::super::Queue as VirtQueue;
use super::defs::uapi;
use super::packet::VsockPacket;
use super::snapshot_gate::SnapshotGate;

use crate::virtio::InterruptTransport;
use vm_memory::GuestMemoryMmap;

const UPDATE_INTERVAL: u64 = 60 * 1000 * 1000 * 1000;
const SLEEP_NSECS: u64 = 2 * 1000 * 1000 * 1000;
const TSYNC_PORT: u32 = 123;

/// Whether a push is due: the thread overslept (the host napped), or the
/// update interval passed since the last push that was actually queued.
fn push_due(now: u64, last_awake: u64, last_update: u64) -> bool {
    now.saturating_sub(last_awake) >= SLEEP_NSECS * 3
        || now.saturating_sub(last_update) >= UPDATE_INTERVAL
}

/// The time of the last push after an attempt at `now`. A push that could not
/// be queued (the guest had no receive buffer at that instant) leaves it
/// unchanged, so the push stays due and is retried at the thread's next wake
/// instead of a whole update interval later. This matters most right after a
/// restore, when the first push is what sets the guest's wall clock.
fn after_push(last_update: u64, now: u64, queued: bool) -> u64 {
    if queued { now } else { last_update }
}

pub struct TimesyncThread {
    cid: u64,
    mem: GuestMemoryMmap,
    queue_mutex: Arc<Mutex<VirtQueue>>,
    interrupt: InterruptTransport,
    snapshot_gate: Arc<SnapshotGate>,
}

impl TimesyncThread {
    pub fn new(
        cid: u64,
        mem: GuestMemoryMmap,
        queue_mutex: Arc<Mutex<VirtQueue>>,
        interrupt: InterruptTransport,
        snapshot_gate: Arc<SnapshotGate>,
    ) -> Self {
        Self {
            cid,
            mem,
            queue_mutex,
            interrupt,
            snapshot_gate,
        }
    }

    /// Queue one time-sync datagram for the guest. Returns whether it was
    /// queued: with no receive buffer available (or an unusable one) nothing
    /// is sent, and the caller must not count the push as made.
    fn send_time(&self, time: u64) -> bool {
        let _snapshot_activity = self.snapshot_gate.enter();
        let mut queue = self.queue_mutex.lock().unwrap();
        if let Some(head) = queue.pop(&self.mem)
            && let Ok(mut pkt) = VsockPacket::from_rx_virtq_head(&head)
        {
            pkt.set_op(uapi::VSOCK_OP_RW)
                .set_src_cid(uapi::VSOCK_HOST_CID)
                .set_dst_cid(self.cid)
                .set_src_port(TSYNC_PORT)
                .set_dst_port(TSYNC_PORT)
                .set_type(uapi::VSOCK_TYPE_DGRAM);

            pkt.write_time_sync(time);
            pkt.set_len(pkt.buf().unwrap().len() as u32);
            if let Err(e) =
                queue.add_used(&self.mem, head.index, pkt.hdr().len() as u32 + pkt.len())
            {
                error!("failed to add used elements to the queue: {e:?}");
            }
            self.interrupt.signal_used_queue();
            return true;
        }
        false
    }

    fn work(&mut self) {
        let mut last_update = 0u64;
        let mut last_awake = utils::time::get_time(utils::time::ClockType::Real);
        loop {
            let now = utils::time::get_time(utils::time::ClockType::Real);
            /*
             * We send a time sync packet if we slept for 3 times more
             * nanoseconds than expected (which is an indication the
             * system forced us to take a long nap), or if UPDATE_INTERVAL
             * has been reached.
             */
            if push_due(now, last_awake, last_update) {
                last_update = after_push(last_update, now, self.send_time(now));
            }

            last_awake = utils::time::get_time(utils::time::ClockType::Real);
            thread::sleep(time::Duration::from_nanos(SLEEP_NSECS));
        }
    }

    pub fn run(mut self) {
        thread::Builder::new()
            .name("vsock timesync".into())
            .spawn(move || self.work())
            .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A push that was not queued stays due at every later wake until one is
    // queued; a queued one is not repeated before the update interval.
    #[test]
    fn a_skipped_push_is_retried_at_the_next_wake() {
        let start = 1_000 * UPDATE_INTERVAL;
        // First wake: never pushed, so it is due.
        assert!(push_due(start, start, 0));
        // It could not be queued: still due two seconds later, and again.
        let mut last_update = after_push(0, start, false);
        for wake in 1..=3 {
            let now = start + wake * SLEEP_NSECS;
            assert!(push_due(now, now - SLEEP_NSECS, last_update));
            last_update = after_push(last_update, now, false);
        }
        // Queued at the next wake: not due again until the interval passes.
        let sent = start + 4 * SLEEP_NSECS;
        last_update = after_push(last_update, sent, true);
        assert_eq!(last_update, sent);
        assert!(!push_due(sent + SLEEP_NSECS, sent, last_update));
        assert!(push_due(
            sent + UPDATE_INTERVAL,
            sent + UPDATE_INTERVAL - SLEEP_NSECS,
            last_update
        ));
    }

    // An overslept wake (three sleeps or more) is due whatever was pushed.
    #[test]
    fn an_overslept_wake_pushes_at_once() {
        let now = 1_000 * UPDATE_INTERVAL;
        assert!(push_due(now, now - 3 * SLEEP_NSECS, now - SLEEP_NSECS));
        assert!(!push_due(now, now - SLEEP_NSECS, now - SLEEP_NSECS));
    }
}
