use crate::config::local_commit;
use crate::listen::{LanMouseListener, ListenEvent, ListenerCreationError};
use futures::StreamExt;
use input_emulation::{EmulationHandle, InputEmulation, InputEmulationError};
use input_event::{Event, PointerEvent};
use lan_mouse_proto::{Position, ProtoEvent};
use local_channel::mpsc::{Receiver, Sender, channel};
use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    rc::Rc,
    time::{Duration, Instant},
};
use tokio::{
    select,
    sync::Notify,
    task::{JoinHandle, spawn_local},
};

/// Maximum number of input events retained while the emulation backend is busy.
/// Beyond this, stateless traffic is dropped and essential traffic fails its
/// source closed.
const MAX_QUEUED_INPUTS: usize = 1024;

/// Maximum number of per-source cleanup controls queued before the pipeline
/// falls back to a single global reset.
const MAX_QUEUED_REMOVALS: usize = 64;

/// How long a source that was failed closed stays muted before it may drive
/// input again.
const BLOCKED_SOURCE_TIMEOUT: Duration = Duration::from_secs(5);

/// emulation handling events received from a listener
pub(crate) struct Emulation {
    task: JoinHandle<()>,
    request_tx: Sender<EmulationRequest>,
    event_rx: Receiver<EmulationEvent>,
}

pub(crate) enum EmulationEvent {
    Connected {
        addr: SocketAddr,
        fingerprint: String,
    },
    ConnectionAttempt {
        fingerprint: String,
    },
    /// new connection
    Entered {
        /// address of the connection
        addr: SocketAddr,
        /// position of the connection
        pos: lan_mouse_ipc::Position,
        /// certificate fingerprint of the connection
        fingerprint: String,
    },
    /// connection closed
    Disconnected {
        addr: SocketAddr,
    },
    /// the port of the listener has changed
    PortChanged(Result<u16, ListenerCreationError>),
    /// emulation was disabled
    EmulationDisabled,
    /// emulation was enabled
    EmulationEnabled,
    /// capture should be released
    ReleaseNotify,
    /// peer sent us a Hello with its build commit hash. Used to
    /// populate `client_manager.peer_commit` from the listen side
    /// too — without this, peer-version visibility silently fails
    /// whenever the outgoing connection in the *other* direction is
    /// broken (one-way setups, asymmetric NAT, peer's TCP listener
    /// down). The connect-side path stays as the primary source;
    /// this is the defensive fallback.
    PeerHello {
        addr: SocketAddr,
        commit: [u8; 8],
    },
}

enum EmulationRequest {
    Reenable,
    Release(SocketAddr),
    ChangePort(u16),
    Terminate,
}

impl Emulation {
    pub(crate) fn new(
        backend: Option<input_emulation::Backend>,
        listener: LanMouseListener,
    ) -> Self {
        let emulation_proxy = EmulationProxy::new(backend);
        let (request_tx, request_rx) = channel();
        let (event_tx, event_rx) = channel();
        let emulation_task = ListenTask {
            listener,
            emulation_proxy,
            request_rx,
            event_tx,
        };
        let task = spawn_local(emulation_task.run());
        Self {
            task,
            request_tx,
            event_rx,
        }
    }

    pub(crate) fn send_leave_event(&self, addr: SocketAddr) {
        self.request_tx
            .send(EmulationRequest::Release(addr))
            .expect("channel closed");
    }

    pub(crate) fn reenable(&self) {
        self.request_tx
            .send(EmulationRequest::Reenable)
            .expect("channel closed");
    }

    pub(crate) fn request_port_change(&self, port: u16) {
        self.request_tx
            .send(EmulationRequest::ChangePort(port))
            .expect("channel closed")
    }

    pub(crate) async fn event(&mut self) -> EmulationEvent {
        self.event_rx.recv().await.expect("channel closed")
    }

    /// wait for termination
    pub(crate) async fn terminate(&mut self) {
        log::debug!("terminating emulation");
        self.request_tx
            .send(EmulationRequest::Terminate)
            .expect("channel closed");
        if let Err(e) = (&mut self.task).await {
            log::warn!("{e}");
        }
    }
}

struct ListenTask {
    listener: LanMouseListener,
    emulation_proxy: EmulationProxy,
    request_rx: Receiver<EmulationRequest>,
    event_tx: Sender<EmulationEvent>,
}

impl ListenTask {
    async fn run(mut self) {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        let mut last_response = HashMap::new();
        let mut rejected_connections = HashMap::new();
        loop {
            select! {
                e = self.listener.next() => {match e {
                    Some(ListenEvent::Msg { event, addr }) => {
                        log::trace!("{event} <-<-<-<-<- {addr}");
                        last_response.insert(addr, Instant::now());
                        match event {
                            ProtoEvent::Enter(pos) => {
                                // Re-entering after an overflow clears the block:
                                // the peer is allowed to drive input again.
                                self.emulation_proxy.clear_overflow_block(addr);
                                if let Some(fingerprint) = self.listener.get_certificate_fingerprint(addr).await {
                                    log::info!("releasing capture: {addr} entered this device");
                                    self.event_tx.send(EmulationEvent::ReleaseNotify).expect("channel closed");
                                    self.listener.reply(addr, ProtoEvent::Ack(0)).await;
                                    self.event_tx.send(EmulationEvent::Entered{addr, pos: to_ipc_pos(pos), fingerprint}).expect("channel closed");
                                }
                            }
                            ProtoEvent::Leave(_) => {
                                self.emulation_proxy.remove(addr);
                                self.listener.reply(addr, ProtoEvent::Ack(0)).await;
                            }
                            ProtoEvent::Input(event) => self.emulation_proxy.consume(event, addr),
                            ProtoEvent::Ping => self.listener.reply(addr, ProtoEvent::Pong(self.emulation_proxy.emulation_active.get())).await,
                            // Peer's version handshake. Echo our own
                            // commit back so the peer's connect-side
                            // receive_loop populates its `peer_commit`,
                            // AND publish a PeerHello upward so our
                            // service can populate ours from the listen
                            // side too — the connect side is the primary
                            // path, but if the outbound direction is
                            // broken (one-way setup, NAT, peer's TCP
                            // listener down) the version display would
                            // otherwise silently say "unknown" while
                            // the peer is in fact happily talking to us.
                            ProtoEvent::Hello { commit } => {
                                self.listener.reply(addr, ProtoEvent::Hello { commit: local_commit() }).await;
                                self.event_tx.send(EmulationEvent::PeerHello { addr, commit }).expect("channel closed");
                            }
                            _ => {}
                        }
                    }
                    Some(ListenEvent::Accept { addr, fingerprint }) => {
                        // A reconnect also clears any overflow block.
                        self.emulation_proxy.clear_overflow_block(addr);
                        self.event_tx.send(EmulationEvent::Connected { addr, fingerprint }).expect("channel closed");
                    }
                    Some(ListenEvent::Rejected { fingerprint }) => {
                        if rejected_connections.insert(fingerprint.clone(), Instant::now())
                            .is_none_or(|i| i.elapsed() >= Duration::from_secs(2)) {
                                self.event_tx.send(EmulationEvent::ConnectionAttempt { fingerprint }).expect("channel closed");
                        }
                    }
                    Some(ListenEvent::Closed { addr }) => {
                        last_response.remove(&addr);
                        self.emulation_proxy.note_source_inactive(addr);
                    }
                    None => break
                }}
                event = self.emulation_proxy.event() => {
                    self.event_tx.send(event).expect("channel closed");
                }
                request = self.request_rx.recv() => match request.expect("channel closed") {
                    // reenable emulation
                    EmulationRequest::Reenable => self.emulation_proxy.reenable(),
                    // notify the other end that we hit a barrier (should release capture)
                    EmulationRequest::Release(addr) => self.listener.reply(addr, ProtoEvent::Leave(0)).await,
                    EmulationRequest::ChangePort(port) => {
                        self.listener.request_port_change(port);
                        let result = self.listener.port_changed().await;
                        self.event_tx.send(EmulationEvent::PortChanged(result)).expect("channel closed");
                    }
                    EmulationRequest::Terminate => break,
                },
                _ = interval.tick() => {
                    last_response.retain(|&addr,instant| {
                        if instant.elapsed() > Duration::from_secs(1) {
                            log::warn!("releasing keys: {addr} not responding!");
                            // This source has been declared inactive, so remove
                            // any stale overflow block as well as its handle.
                            self.emulation_proxy.note_source_inactive(addr);
                            self.event_tx.send(EmulationEvent::Disconnected { addr }).expect("channel closed");
                            false
                        } else {
                            true
                        }
                    });
                }
            }
        }
        self.listener.terminate().await;
        self.emulation_proxy.terminate().await;
    }
}

/// proxy handling the actual input emulation,
/// discarding events when it is disabled
pub(crate) struct EmulationProxy {
    emulation_active: Rc<Cell<bool>>,
    admission: Rc<RefCell<Admission>>,
    event_rx: Receiver<EmulationEvent>,
    task: JoinHandle<()>,
}

impl EmulationProxy {
    fn new(backend: Option<input_emulation::Backend>) -> Self {
        let (event_tx, event_rx) = channel();
        let emulation_active = Rc::new(Cell::new(false));
        let admission = Rc::new(RefCell::new(Admission::new()));
        let emulation_task = EmulationTask {
            backend,
            admission: Rc::clone(&admission),
            event_tx,
            handles: Default::default(),
            next_id: 0,
        };
        let task = spawn_local(emulation_task.run());
        Self {
            emulation_active,
            admission,
            event_rx,
            task,
        }
    }

    async fn event(&mut self) -> EmulationEvent {
        let event = self.event_rx.recv().await.expect("channel closed");
        if let EmulationEvent::EmulationEnabled = event {
            self.emulation_active.replace(true);
        }
        if let EmulationEvent::EmulationDisabled = event {
            self.emulation_active.replace(false);
        }
        event
    }

    fn consume(&self, event: Event, addr: SocketAddr) {
        // ignore events if emulation is currently disabled
        if self.emulation_active.get() {
            if let Admitted::FailedClosed = self.admission.borrow_mut().admit(addr, event) {
                log::warn!("input queue full: failing {addr} closed and scheduling cleanup");
            }
        }
    }

    fn remove(&self, addr: SocketAddr) {
        self.admission.borrow_mut().request_remove(addr);
    }

    fn reenable(&self) {
        self.admission.borrow_mut().request_reenable();
    }

    /// A peer is admitted again after re-entry, reconnect, or an inactive timeout.
    fn clear_overflow_block(&self, addr: SocketAddr) {
        self.admission.borrow_mut().clear_blocked(addr);
    }

    /// Clear stale overflow state and schedule cleanup for an inactive source.
    fn note_source_inactive(&self, addr: SocketAddr) {
        self.admission.borrow_mut().note_source_inactive(addr);
    }

    async fn terminate(&mut self) {
        self.admission.borrow_mut().request_terminate();
        let _ = (&mut self.task).await;
    }
}

struct EmulationTask {
    backend: Option<input_emulation::Backend>,
    admission: Rc<RefCell<Admission>>,
    event_tx: Sender<EmulationEvent>,
    handles: HashMap<SocketAddr, EmulationHandle>,
    next_id: EmulationHandle,
}

/// How a single emulation session ended.
enum SessionEnd {
    /// A terminate request ended the session.
    Terminated,
    /// The backend must be reset/recreated before resuming.
    Recreate,
}

impl EmulationTask {
    async fn run(mut self) {
        loop {
            if let Err(e) = self.do_emulation().await {
                log::warn!("input emulation exited: {e}");
            }
            // wait for reenable or terminate
            loop {
                if self.admission.borrow_mut().take_terminate() {
                    return;
                }
                if self.admission.borrow_mut().take_reenable() {
                    break;
                }
                let wake = { self.admission.borrow().control_wake.clone() };
                wake.notified().await;
            }
        }
    }

    async fn do_emulation(&mut self) -> Result<(), InputEmulationError> {
        log::info!("creating input emulation ...");
        let mut emulation = tokio::select! {
            r = InputEmulation::new(self.backend) => r?,
            // allow termination event while requesting input emulation
            _ = wait_for_terminate(Rc::clone(&self.admission)) => return Ok(()),
        };

        // used to send enabled and disabled events
        let _emulation_guard = DropGuard::new(
            self.event_tx.clone(),
            EmulationEvent::EmulationEnabled,
            EmulationEvent::EmulationDisabled,
        );

        loop {
            // Pending events belong to the previous backend/session. Its matching
            // releases may have been discarded while emulation was disabled; replaying
            // only the queued presses could leave input held in this session.
            self.admission.borrow_mut().discard_pending_inputs();
            self.create_clients(&mut emulation).await;
            match self.do_emulation_session(&mut emulation).await {
                Ok(SessionEnd::Terminated) => break,
                Ok(SessionEnd::Recreate) => {
                    // A bounded cleanup reported that it could not release the
                    // source synchronously, or the removal backlog overflowed:
                    // tear the backend down and recreate it cleanly.
                    log::warn!("resetting input emulation backend");
                    emulation.terminate().await;
                    self.handles.clear();
                    emulation = tokio::select! {
                        r = InputEmulation::new(self.backend) => r?,
                        _ = wait_for_terminate(Rc::clone(&self.admission)) => return Ok(()),
                    };
                }
                Err(e) => {
                    emulation.terminate().await;
                    return Err(e);
                }
            }
        }
        // FIXME replace with async drop when stabilized
        emulation.terminate().await;
        Ok(())
    }

    /// Create the backend handles known before the session started. A terminate
    /// request interrupts handle creation; other controls are serviced by the
    /// session afterwards.
    async fn create_clients(&mut self, emulation: &mut InputEmulation) {
        let handles = self.handles.values().copied().collect::<Vec<_>>();
        let terminate = wait_for_terminate(Rc::clone(&self.admission));
        tokio::pin!(terminate);
        for handle in handles {
            tokio::select! {
                biased;
                _ = emulation.create(handle) => {},
                _ = &mut terminate => return,
            }
        }
    }

    async fn do_emulation_session(
        &mut self,
        emulation: &mut InputEmulation,
    ) -> Result<SessionEnd, InputEmulationError> {
        loop {
            // Priority: termination always wins.
            if self.admission.borrow().terminating() {
                return Ok(SessionEnd::Terminated);
            }
            // Re-enabling an already active session is a no-op.
            if self.admission.borrow_mut().take_reenable() {
                continue;
            }
            // Priority: removal/reset controls are serviced before pending input.
            let control = self.admission.borrow_mut().take_control();
            match control {
                Some(Control::Remove(addr)) => {
                    if let Some(handle) = self.handles.remove(&addr) {
                        // Reset the backend if this handle could not be cleaned up.
                        if !emulation.destroy_bounded(handle).await {
                            return Ok(SessionEnd::Recreate);
                        }
                    }
                }
                Some(Control::ResetAll) => return Ok(SessionEnd::Recreate),
                None => {}
            }

            let Some(item) = self.admission.borrow_mut().take_input() else {
                // Idle: wait for either queued input or a priority control.
                let (input_wake, control_wake) = {
                    let admission = self.admission.borrow();
                    (admission.input_wake.clone(), admission.control_wake.clone())
                };
                tokio::select! {
                    _ = input_wake.notified() => {}
                    _ = control_wake.notified() => {}
                }
                continue;
            };

            // A priority control must be able to interrupt a blocked backend
            // `consume`. The input is re-queued unless the control supersedes it,
            // so ordering barriers for old vs later input are preserved.
            let admission = Rc::clone(&self.admission);
            let result = consume_until_control(admission.clone(), item, async {
                let handle = match self.handles.get(&item.addr) {
                    Some(&handle) => handle,
                    None => {
                        let handle = self.next_id;
                        self.next_id += 1;
                        // Register ownership before awaiting creation so a
                        // priority control can still find and clean up a
                        // handle if a future backend makes `create` pending.
                        self.handles.insert(item.addr, handle);
                        emulation.create(handle).await;
                        handle
                    }
                };
                emulation.consume(item.event, handle).await
            })
            .await;
            if let Ok(Err(error)) = result {
                // A transient backend failure (for example a full output buffer)
                // fails only this source closed; other errors terminate the session.
                handle_consume_error(&mut admission.borrow_mut(), item.addr, error)?;
            }
        }
    }
}

fn to_ipc_pos(pos: Position) -> lan_mouse_ipc::Position {
    match pos {
        Position::Left => lan_mouse_ipc::Position::Left,
        Position::Right => lan_mouse_ipc::Position::Right,
        Position::Top => lan_mouse_ipc::Position::Top,
        Position::Bottom => lan_mouse_ipc::Position::Bottom,
    }
}

async fn wait_for_terminate(admission: Rc<RefCell<Admission>>) {
    loop {
        if admission.borrow().terminating() {
            return;
        }
        let wake = { admission.borrow().control_wake.clone() };
        wake.notified().await;
    }
}

struct DropGuard<T> {
    tx: Sender<T>,
    on_drop: Option<T>,
}

impl<T> DropGuard<T> {
    fn new(tx: Sender<T>, on_new: T, on_drop: T) -> Self {
        tx.send(on_new).expect("channel closed");
        let on_drop = Some(on_drop);
        Self { tx, on_drop }
    }
}

impl<T> Drop for DropGuard<T> {
    fn drop(&mut self) {
        self.tx
            .send(self.on_drop.take().expect("item"))
            .expect("channel closed");
    }
}

/// Outcome of admitting a single input event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admitted {
    /// The event was appended to the queue.
    Queued,
    /// The event was merged into an adjacent relative-motion event.
    Coalesced,
    /// The event was discarded without affecting the source: stateless traffic
    /// at capacity, or traffic from a source that is currently blocked.
    Dropped,
    /// The queue was at capacity and the event was essential, so its source was
    /// failed closed: its queued inputs were removed and cleanup was scheduled.
    FailedClosed,
}

/// A single admitted input event, tagged with its originating source.
#[derive(Debug, Clone, Copy, PartialEq)]
struct QueuedInput {
    addr: SocketAddr,
    event: Event,
}

/// A priority operation produced for the emulation session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Control {
    /// Drop and clean up one source.
    Remove(SocketAddr),
    /// Reset/recreate the whole backend.
    ResetAll,
}

/// Bounded, ordered admission state shared between the producing listener
/// (`EmulationProxy`) and the consuming emulation task.
///
/// The input queue is capped at [`MAX_QUEUED_INPUTS`]. Relative pointer motion
/// is coalesced when it is adjacent to the tail and comes from the same source,
/// so a fast pointer cannot fill the queue. Stateless traffic (motion/axis) is
/// dropped at capacity; essential traffic (keys/buttons/modifiers) fails its
/// source closed instead, because dropping it would strand key state.
///
/// A pending removal/reset is an ordering barrier for that source: its already
/// queued inputs are dropped immediately, so inputs admitted afterwards belong
/// to the re-created handle and are never coalesced across the barrier.
struct Admission {
    inputs: VecDeque<QueuedInput>,
    /// An input has been popped from `inputs` and is currently being consumed.
    /// Its queue slot stays reserved so the session can always re-queue an
    /// interrupted item without exceeding [`MAX_QUEUED_INPUTS`].
    in_flight: bool,
    removals: VecDeque<SocketAddr>,
    reset_all: bool,
    terminate: bool,
    reenable: bool,
    /// Sources blocked after overflow, with the time each block began. A block is
    /// also cleared on re-entry, reconnect, or inactivity.
    blocked: HashMap<SocketAddr, Instant>,
    /// Woken when new input is queued.
    input_wake: Rc<Notify>,
    /// Woken when a priority control is queued.
    control_wake: Rc<Notify>,
}

async fn consume_until_control<F: std::future::Future>(
    admission: Rc<RefCell<Admission>>,
    item: QueuedInput,
    consume: F,
) -> Result<F::Output, ()> {
    let control_wake = { admission.borrow().control_wake.clone() };
    tokio::select! {
        biased;
        result = consume => {
            admission.borrow_mut().input_consumed();
            Ok(result)
        }
        _ = control_wake.notified() => {
            admission.borrow_mut().resume_after_interrupt(item);
            Err(())
        }
    }
}

fn handle_consume_error(
    admission: &mut Admission,
    addr: SocketAddr,
    error: input_emulation::EmulationError,
) -> Result<(), input_emulation::EmulationError> {
    if error.is_transient() {
        admission.fail_source_closed(addr);
        Ok(())
    } else {
        Err(error)
    }
}

impl Admission {
    fn new() -> Self {
        Self {
            inputs: VecDeque::new(),
            in_flight: false,
            removals: VecDeque::new(),
            reset_all: false,
            terminate: false,
            reenable: false,
            blocked: HashMap::new(),
            input_wake: Rc::new(Notify::new()),
            control_wake: Rc::new(Notify::new()),
        }
    }

    fn admit(&mut self, addr: SocketAddr, event: Event) -> Admitted {
        // A source that overflowed stays blocked until it re-enters, reconnects,
        // is declared inactive, or its bounded mute period expires.
        if self.is_muted(addr) {
            return Admitted::Dropped;
        }
        if coalesce_relative_motion(&mut self.inputs, addr, &event) {
            self.input_wake.notify_one();
            return Admitted::Coalesced;
        }
        // The slot of an item currently being consumed stays reserved, so an
        // interrupted item can always be re-queued without breaking the bound.
        if self.inputs.len() + usize::from(self.in_flight) < MAX_QUEUED_INPUTS {
            self.inputs.push_back(QueuedInput { addr, event });
            self.input_wake.notify_one();
            return Admitted::Queued;
        }
        // At capacity: stateless traffic is simply dropped.
        if is_stateless(&event) {
            return Admitted::Dropped;
        }
        // Essential traffic must not be dropped silently, or the source would
        // be left with stranded key/button state: fail it closed instead.
        self.fail_source_closed(addr);
        Admitted::FailedClosed
    }

    /// Whether a source's overflow block is still active. Expired entries are
    /// removed when the source next sends input.
    fn is_muted(&mut self, addr: SocketAddr) -> bool {
        let Some(blocked_since) = self.blocked.get(&addr) else {
            return false;
        };
        if blocked_since.elapsed() < BLOCKED_SOURCE_TIMEOUT {
            return true;
        }
        self.blocked.remove(&addr);
        false
    }

    /// Fail a source closed after an essential overflow or a transient backend
    /// failure, and schedule its ordered cleanup.
    fn fail_source_closed(&mut self, addr: SocketAddr) {
        self.drop_source_inputs(addr);
        self.blocked.insert(addr, Instant::now());
        self.schedule_removal(addr);
        self.control_wake.notify_one();
    }

    /// Queue a per-source cleanup. Dropping the source's queued inputs now makes
    /// the removal an ordering barrier: later inputs belong to a fresh handle.
    fn request_remove(&mut self, addr: SocketAddr) {
        self.drop_source_inputs(addr);
        self.schedule_removal(addr);
        self.control_wake.notify_one();
    }

    fn request_reset_all(&mut self) {
        self.reset_all = true;
        self.removals.clear();
        self.inputs.clear();
        self.control_wake.notify_one();
    }

    fn request_terminate(&mut self) {
        self.terminate = true;
        self.control_wake.notify_one();
    }

    fn request_reenable(&mut self) {
        self.reenable = true;
        self.control_wake.notify_one();
    }

    fn terminating(&self) -> bool {
        self.terminate
    }

    fn take_terminate(&mut self) -> bool {
        std::mem::replace(&mut self.terminate, false)
    }

    fn take_reenable(&mut self) -> bool {
        std::mem::replace(&mut self.reenable, false)
    }

    /// Take the next priority control, if any.
    fn take_control(&mut self) -> Option<Control> {
        if self.reset_all {
            self.reset_all = false;
            self.removals.clear();
            return Some(Control::ResetAll);
        }
        self.removals.pop_front().map(Control::Remove)
    }

    fn take_input(&mut self) -> Option<QueuedInput> {
        let input = self.inputs.pop_front();
        self.in_flight = input.is_some();
        input
    }

    /// Mark the in-flight input as resolved, freeing its reserved queue slot.
    fn input_consumed(&mut self) {
        self.in_flight = false;
    }

    /// Re-insert an input whose `consume` was interrupted by a priority control,
    /// unless that control already superseded it.
    fn resume_after_interrupt(&mut self, input: QueuedInput) {
        // The item is no longer being consumed; free its reserved slot.
        self.in_flight = false;
        if self.terminate
            || self.reset_all
            || self.blocked.contains_key(&input.addr)
            || self.removals.contains(&input.addr)
        {
            // The pending barrier supersedes this pre-barrier input.
            return;
        }
        // Interrupted stateless traffic is not replayed: the backend may already
        // have buffered the request before the interrupt (libei buffers the write
        // and only then awaits writable readiness), so replaying it could apply
        // the same motion twice. Dropping it is safe. Essential transitions are
        // re-queued, and per-handle tracking asks the backend how to resume them.
        if is_stateless(&input.event) {
            return;
        }
        // `admit` leaves this slot reserved while an item is in flight, so the
        // re-queue always fits within `MAX_QUEUED_INPUTS`.
        self.inputs.push_front(input);
    }

    fn clear_blocked(&mut self, addr: SocketAddr) {
        self.blocked.remove(&addr);
    }

    fn note_source_inactive(&mut self, addr: SocketAddr) {
        self.clear_blocked(addr);
        self.request_remove(addr);
    }

    fn schedule_removal(&mut self, addr: SocketAddr) {
        if self.reset_all {
            // Already falling back to a global reset; removals are subsumed.
            self.control_wake.notify_one();
            return;
        }
        if self.removals.contains(&addr) {
            self.control_wake.notify_one();
            return;
        }
        if self.removals.len() >= MAX_QUEUED_REMOVALS {
            // Removal backlog overflowed: drop the stale work and enqueue a
            // single bounded global reset instead.
            log::warn!(
                "input cleanup backlog exceeded {MAX_QUEUED_REMOVALS}; scheduling global reset"
            );
            self.request_reset_all();
            return;
        }
        self.removals.push_back(addr);
        self.control_wake.notify_one();
    }

    fn drop_source_inputs(&mut self, addr: SocketAddr) {
        self.inputs.retain(|queued| queued.addr != addr);
    }

    /// Drop queued input at a session boundary, including any reserved in-flight
    /// slot left by an interrupted input.
    fn discard_pending_inputs(&mut self) {
        self.inputs.clear();
        self.in_flight = false;
    }
}

/// Relative pointer motion is the only traffic that can be merged losslessly:
/// adjacent events from the same source sum up to the same motion.
fn coalesce_relative_motion(
    inputs: &mut VecDeque<QueuedInput>,
    addr: SocketAddr,
    event: &Event,
) -> bool {
    let Event::Pointer(PointerEvent::Motion { time, dx, dy }) = *event else {
        return false;
    };
    let Some(tail) = inputs.back_mut() else {
        return false;
    };
    if tail.addr != addr {
        return false;
    }
    let Event::Pointer(PointerEvent::Motion {
        dx: tdx, dy: tdy, ..
    }) = tail.event
    else {
        return false;
    };
    tail.event = Event::Pointer(PointerEvent::Motion {
        time,
        dx: tdx + dx,
        dy: tdy + dy,
    });
    true
}

/// Traffic that carries no cross-event state and can therefore be dropped at
/// capacity without stranding device state.
fn is_stateless(event: &Event) -> bool {
    matches!(
        event,
        Event::Pointer(
            PointerEvent::Motion { .. }
                | PointerEvent::Axis { .. }
                | PointerEvent::AxisDiscrete120 { .. }
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use input_event::KeyboardEvent;
    use std::net::{Ipv4Addr, SocketAddrV4};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    fn motion(dx: f64, dy: f64) -> Event {
        Event::Pointer(PointerEvent::Motion { time: 0, dx, dy })
    }

    fn axis(value: f64) -> Event {
        Event::Pointer(PointerEvent::Axis {
            time: 0,
            axis: 0,
            value,
        })
    }

    fn key(key: u32) -> Event {
        Event::Keyboard(KeyboardEvent::Key {
            time: 0,
            key,
            state: 1,
        })
    }

    fn button() -> Event {
        Event::Pointer(PointerEvent::Button {
            time: 0,
            button: input_event::BTN_LEFT,
            state: 1,
        })
    }

    fn modifiers() -> Event {
        Event::Keyboard(KeyboardEvent::Modifiers {
            depressed: 1,
            latched: 0,
            locked: 0,
            group: 0,
        })
    }

    /// Fill the queue with alternating sources so that nothing coalesces.
    fn fill_alternating(admission: &mut Admission) {
        for i in 0..MAX_QUEUED_INPUTS {
            let source = if i % 2 == 0 { addr(1) } else { addr(2) };
            assert_ne!(admission.admit(source, motion(1.0, 0.0)), Admitted::Dropped);
        }
        assert_eq!(admission.inputs.len(), MAX_QUEUED_INPUTS);
    }

    #[test]
    fn stateless_and_essential_traffic_are_classified() {
        assert!(is_stateless(&motion(1.0, 1.0)));
        assert!(is_stateless(&axis(1.0)));
        assert!(is_stateless(&Event::Pointer(
            PointerEvent::AxisDiscrete120 {
                axis: 0,
                value: 120
            }
        )));
        assert!(!is_stateless(&key(30)));
        assert!(!is_stateless(&modifiers()));
        assert!(!is_stateless(&button()));
    }

    #[test]
    fn queue_is_bounded_at_capacity() {
        let mut admission = Admission::new();
        fill_alternating(&mut admission);
        // Stateless traffic is dropped once the queue is full.
        assert_eq!(
            admission.admit(addr(1), motion(1.0, 0.0)),
            Admitted::Dropped
        );
        assert_eq!(admission.admit(addr(1), axis(1.0)), Admitted::Dropped);
        assert_eq!(admission.inputs.len(), MAX_QUEUED_INPUTS);
    }

    #[test]
    fn adjacent_same_source_motion_is_coalesced() {
        let a = addr(1);
        let b = addr(2);
        let mut admission = Admission::new();
        assert_eq!(admission.admit(a, motion(1.0, 2.0)), Admitted::Queued);
        assert_eq!(admission.admit(a, motion(3.0, 4.0)), Admitted::Coalesced);
        // Another source breaks adjacency.
        assert_eq!(admission.admit(b, motion(5.0, 6.0)), Admitted::Queued);
        assert_eq!(admission.admit(a, motion(7.0, 8.0)), Admitted::Queued);

        assert_eq!(admission.inputs.len(), 3);
        assert_eq!(admission.inputs[0].addr, a);
        assert_eq!(admission.inputs[0].event, motion(4.0, 6.0));
        assert_eq!(admission.inputs[1].addr, b);
        assert_eq!(admission.inputs[1].event, motion(5.0, 6.0));
        assert_eq!(admission.inputs[2].addr, a);
        assert_eq!(admission.inputs[2].event, motion(7.0, 8.0));

        // Order is preserved on drain.
        assert_eq!(
            admission.take_input(),
            Some(QueuedInput {
                addr: a,
                event: motion(4.0, 6.0)
            })
        );
        assert_eq!(
            admission.take_input(),
            Some(QueuedInput {
                addr: b,
                event: motion(5.0, 6.0)
            })
        );
        assert_eq!(
            admission.take_input(),
            Some(QueuedInput {
                addr: a,
                event: motion(7.0, 8.0)
            })
        );
        assert_eq!(admission.take_input(), None);
    }

    #[test]
    fn essential_overflow_fails_only_that_source_closed() {
        let a = addr(1);
        let b = addr(2);
        let mut admission = Admission::new();
        fill_alternating(&mut admission);
        let before_b = admission.inputs.iter().filter(|q| q.addr == b).count();
        assert!(before_b > 0);

        assert_eq!(admission.admit(a, key(30)), Admitted::FailedClosed);
        assert!(admission.blocked.contains_key(&a));
        // Only the overflowing source is discarded; the other source survives.
        assert_eq!(admission.inputs.iter().filter(|q| q.addr == a).count(), 0);
        assert_eq!(
            admission.inputs.iter().filter(|q| q.addr == b).count(),
            before_b
        );
        assert_eq!(admission.removals, VecDeque::from([a]));
        // The blocked source is muted until it re-enters/reconnects.
        assert_eq!(admission.admit(a, motion(1.0, 0.0)), Admitted::Dropped);
    }

    #[test]
    fn transient_backend_failure_fails_only_its_source_closed() {
        let a = addr(1);
        let b = addr(2);
        let c = addr(3);
        let mut admission = Admission::new();
        assert_eq!(admission.admit(b, motion(1.0, 0.0)), Admitted::Queued);

        // Backpressure (WouldBlock) and an OS-refused injection (InputRefused) are both
        // transient: only the failing source is dropped so that it can be retried later.
        let would_block = input_emulation::EmulationError::Io(std::io::Error::from(
            std::io::ErrorKind::WouldBlock,
        ));
        assert!(handle_consume_error(&mut admission, a, would_block).is_ok());
        assert!(admission.blocked.contains_key(&a));
        assert_eq!(admission.removals, VecDeque::from([a]));
        assert_eq!(admission.inputs.len(), 1);
        assert_eq!(admission.inputs[0].addr, b);

        let refused = input_emulation::EmulationError::InputRefused(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied,
        ));
        assert!(handle_consume_error(&mut admission, c, refused).is_ok());
        assert!(admission.blocked.contains_key(&c));
        assert_eq!(admission.removals, VecDeque::from([a, c]));

        let fatal = input_emulation::EmulationError::EndOfStream;
        assert!(handle_consume_error(&mut admission, b, fatal).is_err());
    }

    #[test]
    fn button_and_modifier_overflow_fail_closed() {
        for essential in [button(), modifiers()] {
            let mut admission = Admission::new();
            fill_alternating(&mut admission);
            assert_eq!(admission.admit(addr(1), essential), Admitted::FailedClosed);
            assert!(admission.blocked.contains_key(&addr(1)));
        }
    }

    #[test]
    fn removal_control_overflow_falls_back_to_bounded_reset_all() {
        let mut admission = Admission::new();
        // Seed input so the fallback can be seen clearing stale work.
        admission.admit(addr(1), motion(1.0, 1.0));
        for port in 0..MAX_QUEUED_REMOVALS as u16 {
            admission.request_remove(addr(port + 1));
        }
        assert_eq!(admission.removals.len(), MAX_QUEUED_REMOVALS);

        // One removal too many overflows the control budget.
        admission.request_remove(addr(1000));
        assert!(
            admission.reset_all,
            "control overflow must schedule ResetAll"
        );
        assert!(
            admission.removals.is_empty(),
            "stale removals must be cleared"
        );
        assert!(admission.inputs.is_empty(), "ResetAll clears pending input");

        // Exactly one bounded ResetAll control is produced.
        assert_eq!(admission.take_control(), Some(Control::ResetAll));
        assert_eq!(admission.take_control(), None);
    }

    #[test]
    fn duplicate_removals_do_not_consume_control_capacity() {
        let mut admission = Admission::new();
        let source = addr(1);
        for _ in 0..=MAX_QUEUED_REMOVALS {
            admission.request_remove(source);
        }

        assert_eq!(admission.removals, VecDeque::from([source]));
        assert!(!admission.reset_all);
    }

    #[test]
    fn removal_is_an_ordering_barrier_for_coalescing() {
        let a = addr(1);
        let mut admission = Admission::new();
        assert_eq!(admission.admit(a, motion(1.0, 1.0)), Admitted::Queued);
        admission.request_remove(a);
        // The pre-barrier input was dropped, so the next motion cannot coalesce.
        assert_eq!(admission.admit(a, motion(2.0, 2.0)), Admitted::Queued);
        assert_eq!(admission.inputs.len(), 1);
        assert_eq!(admission.inputs[0].event, motion(2.0, 2.0));

        // The control is serviced before the post-barrier input.
        assert_eq!(admission.take_control(), Some(Control::Remove(a)));
        assert_eq!(
            admission.take_input(),
            Some(QueuedInput {
                addr: a,
                event: motion(2.0, 2.0)
            })
        );
    }

    #[test]
    fn interrupted_input_is_dropped_when_superseded_by_barrier() {
        let a = addr(1);
        let b = addr(2);
        let mut admission = Admission::new();
        admission.request_remove(b);
        // b's in-flight input is superseded by its pending removal.
        admission.resume_after_interrupt(QueuedInput {
            addr: b,
            event: motion(1.0, 1.0),
        });
        assert!(admission.inputs.is_empty());
        // An unrelated source is re-queued in order (essential traffic only:
        // interrupted stateless motion is dropped instead of replayed).
        admission.resume_after_interrupt(QueuedInput {
            addr: a,
            event: key(30),
        });
        assert_eq!(admission.inputs.len(), 1);
        assert_eq!(admission.inputs[0].addr, a);
    }

    #[test]
    fn interrupted_stateless_input_is_not_replayed() {
        let a = addr(1);
        let mut admission = Admission::new();
        assert_eq!(admission.admit(a, motion(1.0, 2.0)), Admitted::Queued);
        let in_flight = admission.take_input().expect("head input");
        admission.request_remove(addr(99));
        admission.resume_after_interrupt(in_flight);
        assert!(
            admission.inputs.is_empty(),
            "replaying buffered stateless output could apply it twice"
        );
    }

    #[test]
    fn in_flight_input_reserves_a_queue_slot() {
        let a = addr(1);
        let b = addr(2);
        let c = addr(3);
        let mut admission = Admission::new();
        // The front item is essential so an interrupt re-queues it.
        assert_eq!(admission.admit(a, key(30)), Admitted::Queued);
        for i in 0..MAX_QUEUED_INPUTS - 1 {
            let source = if i % 2 == 0 { b } else { c };
            assert_eq!(admission.admit(source, motion(1.0, 0.0)), Admitted::Queued);
        }
        assert_eq!(admission.inputs.len(), MAX_QUEUED_INPUTS);

        // The session starts consuming the head item; its slot stays reserved.
        let in_flight = admission.take_input().expect("head input");
        assert_eq!(in_flight.event, key(30));
        assert_eq!(admission.inputs.len(), MAX_QUEUED_INPUTS - 1);

        // A concurrent producer cannot reuse the freed slot.
        assert_eq!(admission.admit(a, motion(1.0, 0.0)), Admitted::Dropped);
        assert_eq!(admission.inputs.len(), MAX_QUEUED_INPUTS - 1);

        // An unrelated removal interrupts the consume; re-queuing stays bounded.
        admission.request_remove(addr(99));
        admission.resume_after_interrupt(in_flight);
        assert_eq!(admission.inputs.len(), MAX_QUEUED_INPUTS);
        assert_eq!(
            admission.inputs.front().map(|queued| queued.event),
            Some(key(30))
        );
    }

    #[test]
    fn overflow_block_lifecycle() {
        let a = addr(1);
        let mut admission = Admission::new();
        fill_alternating(&mut admission);

        // Overflow blocks a.
        admission.admit(a, key(30));
        assert!(admission.blocked.contains_key(&a));

        // A generic removal is an ordering barrier, not proof that the source
        // is inactive, so it must not clear the block.
        admission.request_remove(a);
        assert!(admission.blocked.contains_key(&a));

        // A liveness timeout or transport close clears stale block metadata.
        // Re-entry/reconnect also clears the block and admits input again.
        admission.note_source_inactive(a);
        assert!(!admission.blocked.contains_key(&a));
        assert_eq!(admission.removals, VecDeque::from([a]));
        assert_eq!(admission.admit(a, motion(1.0, 2.0)), Admitted::Queued);
        assert_eq!(admission.inputs.back().map(|q| q.addr), Some(a));
    }

    #[test]
    fn overflow_block_expires_after_timeout() {
        let a = addr(1);
        let mut admission = Admission::new();
        fill_alternating(&mut admission);

        assert_eq!(admission.admit(a, key(30)), Admitted::FailedClosed);
        assert_eq!(admission.admit(a, motion(1.0, 0.0)), Admitted::Dropped);

        // A source that stays connected remains muted before the timeout.
        admission.blocked.insert(
            a,
            Instant::now() - (BLOCKED_SOURCE_TIMEOUT - Duration::from_secs(1)),
        );
        assert_eq!(admission.admit(a, motion(1.0, 0.0)), Admitted::Dropped);
        assert!(admission.blocked.contains_key(&a));

        // Once the full mute period has elapsed, the next event may be admitted.
        admission
            .blocked
            .insert(a, Instant::now() - BLOCKED_SOURCE_TIMEOUT);
        assert_eq!(admission.admit(a, motion(1.0, 0.0)), Admitted::Queued);
        assert!(!admission.blocked.contains_key(&a));
    }

    #[test]
    fn session_start_discards_stale_input() {
        let source = addr(1);
        let press = key(30);
        let release = Event::Keyboard(KeyboardEvent::Key {
            time: 0,
            key: 30,
            state: 0,
        });
        let mut admission = Admission::new();
        assert_eq!(admission.admit(source, press), Admitted::Queued);
        assert_eq!(admission.admit(source, release), Admitted::Queued);
        assert_eq!(
            admission.take_input(),
            Some(QueuedInput {
                addr: source,
                event: press,
            })
        );
        assert_eq!(
            admission.inputs.front().map(|queued| queued.event),
            Some(release)
        );

        admission.discard_pending_inputs();

        assert!(admission.inputs.is_empty());
        assert!(!admission.in_flight);
        assert_eq!(admission.admit(source, key(31)), Admitted::Queued);
    }

    #[tokio::test]
    async fn priority_control_interrupts_blocked_consume_and_requeues_essential_input() {
        let admission = Rc::new(RefCell::new(Admission::new()));
        let item = QueuedInput {
            addr: addr(1),
            event: key(30),
        };
        assert_eq!(
            admission.borrow_mut().admit(item.addr, item.event),
            Admitted::Queued
        );
        assert_eq!(admission.borrow_mut().take_input(), Some(item));

        let interrupted =
            consume_until_control(Rc::clone(&admission), item, std::future::pending::<()>());
        admission.borrow_mut().request_remove(addr(2));
        assert!(interrupted.await.is_err());
        assert!(!admission.borrow().in_flight);
        assert_eq!(
            admission.borrow_mut().take_control(),
            Some(Control::Remove(addr(2)))
        );
        assert_eq!(admission.borrow_mut().take_input(), Some(item));
    }

    #[tokio::test]
    async fn terminate_preempts_blocked_consume_without_requeue() {
        let admission = Rc::new(RefCell::new(Admission::new()));
        let item = QueuedInput {
            addr: addr(1),
            event: key(30),
        };
        admission.borrow_mut().admit(item.addr, item.event);
        assert_eq!(admission.borrow_mut().take_input(), Some(item));

        let interrupted =
            consume_until_control(Rc::clone(&admission), item, std::future::pending::<()>());
        admission.borrow_mut().request_terminate();
        assert!(interrupted.await.is_err());
        assert!(admission.borrow().inputs.is_empty());
        assert!(!admission.borrow().in_flight);
        assert!(admission.borrow_mut().take_terminate());
    }
}
