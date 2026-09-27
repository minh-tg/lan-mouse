use async_trait::async_trait;
use std::{collections::HashMap, fmt::Display, time::Duration};

use input_event::{Event, KeyboardEvent, PointerEvent};

pub use self::error::{EmulationCreationError, EmulationError, InputEmulationError};

#[cfg(windows)]
mod windows;

#[cfg(x11)]
mod x11;

#[cfg(wlroots)]
mod wlroots;

#[cfg(rdp)]
mod xdg_desktop_portal;

#[cfg(libei)]
mod libei;

#[cfg(target_os = "macos")]
mod macos;

/// fallback input emulation (logs events)
mod dummy;
mod error;

pub type EmulationHandle = u64;

/// Upper bound applied to each cleanup step performed by
/// [`InputEmulation::destroy_bounded`] and [`InputEmulation::terminate`].
///
/// Cleanup must not block the emulation task forever, e.g. when a backend connection is
/// backed up and flushing the key/button releases keeps returning `WouldBlock`.
const DEFAULT_CLEANUP_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Backend {
    #[cfg(wlroots)]
    Wlroots,
    #[cfg(libei)]
    Libei,
    #[cfg(rdp)]
    Xdp,
    #[cfg(x11)]
    X11,
    #[cfg(windows)]
    Windows,
    #[cfg(target_os = "macos")]
    MacOs,
    Dummy,
}

impl Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(wlroots)]
            Backend::Wlroots => write!(f, "wlroots"),
            #[cfg(libei)]
            Backend::Libei => write!(f, "libei"),
            #[cfg(rdp)]
            Backend::Xdp => write!(f, "xdg-desktop-portal"),
            #[cfg(x11)]
            Backend::X11 => write!(f, "X11"),
            #[cfg(windows)]
            Backend::Windows => write!(f, "windows"),
            #[cfg(target_os = "macos")]
            Backend::MacOs => write!(f, "macos"),
            Backend::Dummy => write!(f, "dummy"),
        }
    }
}

pub struct InputEmulation {
    emulation: Box<dyn Emulation>,
    handles: HashMap<EmulationHandle, TrackedInput>,
    /// Bound applied to each backend operation during cleanup.
    cleanup_timeout: Duration,
}

/// Delivery state for one key or pointer-button transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TrackedTransition {
    /// A press was submitted but the backend future was cancelled before confirmation.
    PressPending,
    /// The backend confirmed the press.
    Pressed,
    /// A release was submitted but the backend future was cancelled before confirmation.
    ReleasePending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InputAction {
    Ignore,
    Send,
    Retry,
}

/// Input that has been handed to the backend for a single [`EmulationHandle`].
///
/// Pending transitions are retained across cancellation so the caller can resume them.
/// A backend may flush an already-buffered event instead of sending it twice. Confirmed
/// presses remain tracked until a confirmed release or cleanup.
#[derive(Default)]
struct TrackedInput {
    keys: HashMap<u32, TrackedTransition>,
    buttons: HashMap<u32, TrackedTransition>,
}

impl InputEmulation {
    async fn with_backend(backend: Backend) -> Result<InputEmulation, EmulationCreationError> {
        let emulation: Box<dyn Emulation> = match backend {
            #[cfg(wlroots)]
            Backend::Wlroots => Box::new(wlroots::WlrootsEmulation::new()?),
            #[cfg(libei)]
            Backend::Libei => Box::new(libei::LibeiEmulation::new().await?),
            #[cfg(x11)]
            Backend::X11 => Box::new(x11::X11Emulation::new()?),
            #[cfg(rdp)]
            Backend::Xdp => Box::new(xdg_desktop_portal::DesktopPortalEmulation::new().await?),
            #[cfg(windows)]
            Backend::Windows => Box::new(windows::WindowsEmulation::new()?),
            #[cfg(target_os = "macos")]
            Backend::MacOs => Box::new(macos::MacOSEmulation::new()?),
            Backend::Dummy => Box::new(dummy::DummyEmulation::new()),
        };
        Ok(Self {
            emulation,
            handles: HashMap::new(),
            cleanup_timeout: DEFAULT_CLEANUP_TIMEOUT,
        })
    }

    pub async fn new(backend: Option<Backend>) -> Result<InputEmulation, EmulationCreationError> {
        if let Some(backend) = backend {
            let b = Self::with_backend(backend).await;
            if b.is_ok() {
                log::info!("using emulation backend: {backend}");
            }
            return b;
        }

        for backend in [
            #[cfg(wlroots)]
            Backend::Wlroots,
            #[cfg(libei)]
            Backend::Libei,
            #[cfg(rdp)]
            Backend::Xdp,
            #[cfg(x11)]
            Backend::X11,
            #[cfg(windows)]
            Backend::Windows,
            #[cfg(target_os = "macos")]
            Backend::MacOs,
            Backend::Dummy,
        ] {
            match Self::with_backend(backend).await {
                Ok(b) => {
                    log::info!("using emulation backend: {backend}");
                    return Ok(b);
                }
                Err(e) if e.cancelled_by_user() => return Err(e),
                Err(e) => log::warn!("{e}"),
            }
        }

        Err(EmulationCreationError::NoAvailableBackend)
    }

    pub async fn consume(
        &mut self,
        event: Event,
        handle: EmulationHandle,
    ) -> Result<(), EmulationError> {
        match event {
            Event::Keyboard(KeyboardEvent::Key { key, state, .. }) => {
                // Ignore confirmed duplicate presses and unmatched releases. If a previous
                // backend call was cancelled, let that backend resume it without resending
                // the event when it already buffers writes (as libei does).
                let action = self.track_key(handle, key, state);
                let result = match action {
                    InputAction::Ignore => return Ok(()),
                    InputAction::Send => self.emulation.consume(event, handle).await,
                    InputAction::Retry => self.emulation.retry_pending(event, handle).await,
                };
                result?;
                self.complete_key_transition(handle, key, state);
                Ok(())
            }
            Event::Pointer(PointerEvent::Button { button, state, .. }) => {
                // Use the same cancellation handling for pointer buttons as for keys.
                let action = self.track_button(handle, button, state);
                let result = match action {
                    InputAction::Ignore => return Ok(()),
                    InputAction::Send => self.emulation.consume(event, handle).await,
                    InputAction::Retry => self.emulation.retry_pending(event, handle).await,
                };
                result?;
                self.complete_button_transition(handle, button, state);
                Ok(())
            }
            _ => self.emulation.consume(event, handle).await,
        }
    }

    pub async fn create(&mut self, handle: EmulationHandle) -> bool {
        if self.handles.contains_key(&handle) {
            return false;
        }
        self.handles.insert(handle, TrackedInput::default());
        self.emulation.create(handle).await;
        true
    }

    /// Release everything tracked for `handle` and destroy the backend state.
    ///
    /// Convenience wrapper around [`destroy_bounded`](Self::destroy_bounded) that discards
    /// the result; used for the common case where no failure handling is possible.
    pub async fn destroy(&mut self, handle: EmulationHandle) {
        let _ = self.destroy_bounded(handle).await;
    }

    /// Release all keys and pointer buttons tracked for `handle` and then destroy the
    /// backend state for it.
    ///
    /// Both steps are bounded by an internal timeout so that a stalled backend cannot block
    /// cleanup indefinitely. Returns `false` when releasing the tracked input or destroying
    /// the backend did not complete successfully (including a timeout); in that case the
    /// handle and its tracked input remain registered so a later
    /// [`terminate`](Self::terminate) (or another `destroy_bounded`) can retry.
    pub async fn destroy_bounded(&mut self, handle: EmulationHandle) -> bool {
        let Some(tracked) = self.handles.get(&handle) else {
            return true;
        };
        let keys = tracked.keys.keys().copied().collect::<Vec<_>>();
        let buttons = tracked.buttons.keys().copied().collect::<Vec<_>>();

        let released = tokio::time::timeout(
            self.cleanup_timeout,
            Self::release_tracked(&mut *self.emulation, handle, &keys, &buttons),
        )
        .await;

        if !matches!(released, Ok(Ok(()))) {
            log::warn!("releasing input for handle {handle} did not complete successfully");
            return false;
        }

        let destroyed =
            tokio::time::timeout(self.cleanup_timeout, self.emulation.destroy(handle)).await;
        if destroyed.is_err() {
            log::warn!("destroying emulation for handle {handle} did not complete in time");
            return false;
        }

        self.handles.remove(&handle);
        true
    }

    /// Release all tracked input for every handle and terminate the backend.
    ///
    /// Each per-handle cleanup and the final backend terminate are bounded by an internal
    /// timeout, so a stalled backend cannot block termination indefinitely. The total time is
    /// therefore bounded by `cleanup_timeout * (2 * handles + 1)`, i.e. linear in the handle
    /// count, which is itself bounded by the number of known peer addresses. The handle set is
    /// snapshotted once, so a failing cleanup cannot extend the loop.
    pub async fn terminate(&mut self) {
        for handle in self.handles.keys().copied().collect::<Vec<_>>() {
            let _ = self.destroy_bounded(handle).await;
        }
        if tokio::time::timeout(self.cleanup_timeout, self.emulation.terminate())
            .await
            .is_err()
        {
            log::warn!("terminating emulation did not complete in time");
        }
    }

    /// Release all keys currently tracked as pressed for `handle`.
    ///
    /// The handle stays registered and keys only leave the tracked set once the backend
    /// confirmed their release.
    pub async fn release_keys(&mut self, handle: EmulationHandle) -> Result<(), EmulationError> {
        let keys = self
            .handles
            .get(&handle)
            .map(|tracked| tracked.keys.keys().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        Self::release_tracked(&mut *self.emulation, handle, &keys, &[]).await?;
        if let Some(tracked) = self.handles.get_mut(&handle) {
            for key in keys {
                tracked.keys.remove(&key);
            }
        }
        Ok(())
    }

    pub fn has_pressed_keys(&self, handle: EmulationHandle) -> bool {
        self.handles
            .get(&handle)
            .is_some_and(|tracked| !tracked.keys.is_empty())
    }

    /// Track an incoming key transition and choose whether to send or retry it.
    ///
    /// Confirmed duplicate presses and unmatched releases are suppressed. Pending
    /// transitions use the backend's retry behavior after cancellation.
    fn track_key(&mut self, handle: EmulationHandle, key: u32, state: u8) -> InputAction {
        let Some(tracked) = self.handles.get_mut(&handle) else {
            return InputAction::Ignore;
        };
        if state == 0 {
            let Some(transition) = tracked.keys.get_mut(&key) else {
                return InputAction::Ignore;
            };
            let action = if *transition == TrackedTransition::ReleasePending {
                InputAction::Retry
            } else {
                InputAction::Send
            };
            *transition = TrackedTransition::ReleasePending;
            action
        } else {
            match tracked.keys.entry(key) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(TrackedTransition::PressPending);
                    InputAction::Send
                }
                std::collections::hash_map::Entry::Occupied(entry) => {
                    if *entry.get() == TrackedTransition::PressPending {
                        InputAction::Retry
                    } else {
                        InputAction::Ignore
                    }
                }
            }
        }
    }

    fn complete_key_transition(&mut self, handle: EmulationHandle, key: u32, state: u8) {
        let Some(tracked) = self.handles.get_mut(&handle) else {
            return;
        };
        if state == 0 {
            tracked.keys.remove(&key);
        } else if let Some(transition) = tracked.keys.get_mut(&key) {
            *transition = TrackedTransition::Pressed;
        }
    }

    /// Same contract as [`track_key`](Self::track_key) for pointer buttons.
    fn track_button(&mut self, handle: EmulationHandle, button: u32, state: u32) -> InputAction {
        let Some(tracked) = self.handles.get_mut(&handle) else {
            return InputAction::Ignore;
        };
        if state == 0 {
            let Some(transition) = tracked.buttons.get_mut(&button) else {
                return InputAction::Ignore;
            };
            let action = if *transition == TrackedTransition::ReleasePending {
                InputAction::Retry
            } else {
                InputAction::Send
            };
            *transition = TrackedTransition::ReleasePending;
            action
        } else {
            match tracked.buttons.entry(button) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(TrackedTransition::PressPending);
                    InputAction::Send
                }
                std::collections::hash_map::Entry::Occupied(entry) => {
                    if *entry.get() == TrackedTransition::PressPending {
                        InputAction::Retry
                    } else {
                        InputAction::Ignore
                    }
                }
            }
        }
    }

    fn complete_button_transition(&mut self, handle: EmulationHandle, button: u32, state: u32) {
        let Some(tracked) = self.handles.get_mut(&handle) else {
            return;
        };
        if state == 0 {
            tracked.buttons.remove(&button);
        } else if let Some(transition) = tracked.buttons.get_mut(&button) {
            *transition = TrackedTransition::Pressed;
        }
    }

    /// Release the given keys and buttons plus the modifier state for `handle`.
    ///
    /// Sending a release for input the backend never applied is harmless, so a superset of
    /// the actual backend state is released.
    async fn release_tracked(
        emulation: &mut dyn Emulation,
        handle: EmulationHandle,
        keys: &[u32],
        buttons: &[u32],
    ) -> Result<(), EmulationError> {
        for &key in keys {
            if let Ok(scancode) = input_event::scancode::Linux::try_from(key) {
                log::warn!("releasing stuck key: {scancode:?}");
            }
            let event = Event::Keyboard(KeyboardEvent::Key {
                time: 0,
                key,
                state: 0,
            });
            emulation.consume(event, handle).await?;
        }

        for &button in buttons {
            log::warn!("releasing stuck button: {button}");
            let event = Event::Pointer(PointerEvent::Button {
                time: 0,
                button,
                state: 0,
            });
            emulation.consume(event, handle).await?;
        }

        let event = Event::Keyboard(KeyboardEvent::Modifiers {
            depressed: 0,
            latched: 0,
            locked: 0,
            group: 0,
        });
        emulation.consume(event, handle).await
    }
}

#[async_trait]
trait Emulation: Send {
    async fn consume(
        &mut self,
        event: Event,
        handle: EmulationHandle,
    ) -> Result<(), EmulationError>;
    /// Resume a cancelled transition. Backends that buffer output before awaiting can
    /// flush the existing buffer instead of submitting the same transition twice.
    async fn retry_pending(
        &mut self,
        event: Event,
        handle: EmulationHandle,
    ) -> Result<(), EmulationError> {
        self.consume(event, handle).await
    }
    async fn create(&mut self, handle: EmulationHandle);
    async fn destroy(&mut self, handle: EmulationHandle);
    async fn terminate(&mut self);
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use input_event::{BTN_LEFT, KeyboardEvent, PointerEvent};
    use std::{
        future::pending,
        sync::{Arc, Mutex},
        time::Instant,
    };

    /// Backend used by the tests. It records everything it is asked to do and can be told
    /// to stall or fail on specific events, so cancellation and timeout paths are
    /// observable.
    #[derive(Default)]
    struct MockControl {
        consumed: Vec<(EmulationHandle, Event)>,
        retried: Vec<(EmulationHandle, Event)>,
        destroyed: Vec<EmulationHandle>,
        terminated: bool,
        /// `consume` waits forever for this event instead of recording it
        stall_on: Option<Event>,
        /// `consume` records this event and then waits forever
        stall_after_record: Option<Event>,
        /// Model a backend whose pending event is already buffered on retry
        retry_without_resend: bool,
        /// `consume` records this event and then returns an error
        fail_on: Option<Event>,
        /// `destroy`/`terminate` wait forever
        stall_destroy: bool,
        stall_terminate: bool,
    }

    struct MockEmulation {
        control: Arc<Mutex<MockControl>>,
    }

    impl MockEmulation {
        fn new() -> (Self, Arc<Mutex<MockControl>>) {
            let control = Arc::new(Mutex::new(MockControl::default()));
            (
                Self {
                    control: control.clone(),
                },
                control,
            )
        }
    }

    #[async_trait]
    impl Emulation for MockEmulation {
        async fn consume(
            &mut self,
            event: Event,
            handle: EmulationHandle,
        ) -> Result<(), EmulationError> {
            let (stall, fail) = {
                let mut control = self.control.lock().unwrap();
                if control.stall_on == Some(event) {
                    (true, false)
                } else {
                    control.consumed.push((handle, event));
                    (
                        control.stall_after_record == Some(event),
                        control.fail_on == Some(event),
                    )
                }
            };
            if stall {
                pending::<()>().await;
            }
            if fail {
                return Err(EmulationError::EndOfStream);
            }
            Ok(())
        }

        async fn retry_pending(
            &mut self,
            event: Event,
            handle: EmulationHandle,
        ) -> Result<(), EmulationError> {
            let retry_without_resend = {
                let mut control = self.control.lock().unwrap();
                control.retried.push((handle, event));
                control.retry_without_resend
            };
            if retry_without_resend {
                Ok(())
            } else {
                self.consume(event, handle).await
            }
        }

        async fn create(&mut self, _: EmulationHandle) {}

        async fn destroy(&mut self, handle: EmulationHandle) {
            let stall = {
                let mut control = self.control.lock().unwrap();
                control.destroyed.push(handle);
                control.stall_destroy
            };
            if stall {
                pending::<()>().await;
            }
        }

        async fn terminate(&mut self) {
            let stall = {
                let mut control = self.control.lock().unwrap();
                control.terminated = true;
                control.stall_terminate
            };
            if stall {
                pending::<()>().await;
            }
        }
    }

    fn emulation_with(control: &Arc<Mutex<MockControl>>) -> InputEmulation {
        InputEmulation {
            emulation: Box::new(MockEmulation {
                control: control.clone(),
            }),
            handles: HashMap::new(),
            cleanup_timeout: Duration::from_millis(20),
        }
    }

    fn key_event(key: u32, state: u8) -> Event {
        Event::Keyboard(KeyboardEvent::Key {
            time: 0,
            key,
            state,
        })
    }

    fn button_event(button: u32, state: u32) -> Event {
        Event::Pointer(PointerEvent::Button {
            time: 0,
            button,
            state,
        })
    }

    fn consumed(control: &Arc<Mutex<MockControl>>) -> Vec<(EmulationHandle, Event)> {
        control.lock().unwrap().consumed.clone()
    }

    #[tokio::test]
    async fn suppresses_duplicate_presses_and_unmatched_releases() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;
        emulation.create(1).await;

        // duplicate press is suppressed
        emulation.consume(key_event(30, 1), 0).await.unwrap();
        emulation.consume(key_event(30, 1), 0).await.unwrap();
        // release on the wrong handle is unmatched
        emulation.consume(key_event(30, 0), 1).await.unwrap();
        // release on the owning handle is forwarded
        emulation.consume(key_event(30, 0), 0).await.unwrap();
        // a second release is unmatched again
        emulation.consume(key_event(30, 0), 0).await.unwrap();

        // same for pointer buttons
        emulation
            .consume(button_event(BTN_LEFT, 1), 1)
            .await
            .unwrap();
        emulation
            .consume(button_event(BTN_LEFT, 1), 1)
            .await
            .unwrap();
        emulation
            .consume(button_event(BTN_LEFT, 0), 0)
            .await
            .unwrap();
        emulation
            .consume(button_event(BTN_LEFT, 0), 1)
            .await
            .unwrap();
        emulation
            .consume(button_event(BTN_LEFT, 0), 1)
            .await
            .unwrap();

        assert_eq!(
            consumed(&control),
            vec![
                (0, key_event(30, 1)),
                (0, key_event(30, 0)),
                (1, button_event(BTN_LEFT, 1)),
                (1, button_event(BTN_LEFT, 0)),
            ]
        );
        assert!(!emulation.has_pressed_keys(0));
        assert!(!emulation.has_pressed_keys(1));
    }

    #[tokio::test]
    async fn destroy_releases_tracked_input_per_handle() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;
        emulation.create(1).await;

        emulation.consume(key_event(30, 1), 0).await.unwrap();
        emulation
            .consume(button_event(BTN_LEFT, 1), 0)
            .await
            .unwrap();
        emulation.consume(key_event(31, 1), 1).await.unwrap();

        assert!(emulation.destroy_bounded(0).await);

        let events = consumed(&control);
        assert!(events.contains(&(0, key_event(30, 0))));
        assert!(events.contains(&(0, button_event(BTN_LEFT, 0))));
        // the other handle must not be released or destroyed
        assert!(!events.contains(&(1, key_event(31, 0))));
        assert!(!control.lock().unwrap().destroyed.contains(&1));

        assert!(emulation.destroy_bounded(1).await);
        assert!(consumed(&control).contains(&(1, key_event(31, 0))));
        assert!(control.lock().unwrap().destroyed.contains(&1));
    }

    #[tokio::test]
    async fn cancelled_consume_retains_cleanup_state() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;

        let press = key_event(30, 1);
        control.lock().unwrap().stall_on = Some(press);

        // cancel the consume future while the backend is still busy
        let cancelled =
            tokio::time::timeout(Duration::from_millis(5), emulation.consume(press, 0)).await;
        assert!(cancelled.is_err());
        assert!(
            emulation.has_pressed_keys(0),
            "a cancelled press must stay tracked for cleanup"
        );

        // cleanup must still release the key once the backend recovers
        control.lock().unwrap().stall_on = None;
        assert!(emulation.destroy_bounded(0).await);
        assert!(consumed(&control).contains(&(0, key_event(30, 0))));
        assert!(!emulation.has_pressed_keys(0));
    }

    #[tokio::test]
    async fn default_retry_resends_cancelled_press() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;

        let press = key_event(30, 1);
        control.lock().unwrap().stall_on = Some(press);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), emulation.consume(press, 0))
                .await
                .is_err()
        );

        control.lock().unwrap().stall_on = None;
        emulation.consume(press, 0).await.unwrap();
        assert_eq!(consumed(&control), vec![(0, press)]);
        assert_eq!(control.lock().unwrap().retried, vec![(0, press)]);
        assert!(emulation.has_pressed_keys(0));

        let button_press = button_event(BTN_LEFT, 1);
        control.lock().unwrap().stall_on = Some(button_press);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), emulation.consume(button_press, 0))
                .await
                .is_err()
        );

        control.lock().unwrap().stall_on = None;
        emulation.consume(button_press, 0).await.unwrap();
        assert_eq!(consumed(&control), vec![(0, press), (0, button_press)]);
        assert_eq!(
            control.lock().unwrap().retried,
            vec![(0, press), (0, button_press)]
        );
    }

    #[tokio::test]
    async fn buffered_transition_retry_does_not_send_a_duplicate() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;

        let press = key_event(30, 1);
        {
            let mut control = control.lock().unwrap();
            control.stall_after_record = Some(press);
            control.retry_without_resend = true;
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(5), emulation.consume(press, 0))
                .await
                .is_err()
        );

        control.lock().unwrap().stall_after_record = None;
        emulation.consume(press, 0).await.unwrap();
        assert_eq!(consumed(&control), vec![(0, press)]);
        assert_eq!(control.lock().unwrap().retried, vec![(0, press)]);
        assert!(emulation.has_pressed_keys(0));
    }

    #[tokio::test]
    async fn default_retry_resends_cancelled_button_release() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;

        let press = button_event(BTN_LEFT, 1);
        let release = button_event(BTN_LEFT, 0);
        emulation.consume(press, 0).await.unwrap();

        control.lock().unwrap().stall_on = Some(release);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), emulation.consume(release, 0))
                .await
                .is_err()
        );

        control.lock().unwrap().stall_on = None;
        emulation.consume(release, 0).await.unwrap();
        assert_eq!(consumed(&control), vec![(0, press), (0, release)]);
        assert_eq!(control.lock().unwrap().retried, vec![(0, release)]);
        assert!(emulation.destroy_bounded(0).await);
    }

    #[tokio::test]
    async fn failed_consume_retains_cleanup_state() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;

        let press = key_event(30, 1);
        control.lock().unwrap().fail_on = Some(press);
        assert!(emulation.consume(press, 0).await.is_err());
        assert!(
            emulation.has_pressed_keys(0),
            "a failed press must stay tracked for cleanup"
        );

        control.lock().unwrap().fail_on = None;
        assert!(emulation.destroy_bounded(0).await);
        assert!(consumed(&control).contains(&(0, key_event(30, 0))));
    }

    #[tokio::test]
    async fn destroy_bounded_reports_stalled_release_and_can_retry() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;
        emulation.consume(key_event(30, 1), 0).await.unwrap();

        // stalled release: cleanup cannot complete
        control.lock().unwrap().stall_on = Some(key_event(30, 0));
        assert!(!emulation.destroy_bounded(0).await);
        assert!(emulation.has_pressed_keys(0));
        assert!(!control.lock().unwrap().destroyed.contains(&0));

        // backend recovers, the retry succeeds
        control.lock().unwrap().stall_on = None;
        assert!(emulation.destroy_bounded(0).await);
        assert!(control.lock().unwrap().destroyed.contains(&0));
        assert!(!emulation.has_pressed_keys(0));
    }

    #[tokio::test]
    async fn terminate_is_bounded_across_many_stalled_handles() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);

        const HANDLES: u64 = 32;
        for handle in 0..HANDLES {
            emulation.create(handle).await;
            emulation.consume(key_event(30, 1), handle).await.unwrap();
        }

        // Every release stalls, so each handle adds one cleanup timeout to the
        // worst-case total; the snapshot loop is still finite and completes.
        control.lock().unwrap().stall_on = Some(key_event(30, 0));
        let start = Instant::now();
        tokio::time::timeout(Duration::from_secs(10), emulation.terminate())
            .await
            .expect("terminate must be bounded across all handles");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "terminate scaled beyond the per-handle bound"
        );
        // The stalled input stays tracked so a later attempt can still release it.
        assert!(emulation.has_pressed_keys(0));
    }

    #[tokio::test]
    async fn stalled_destroy_is_reported_and_terminate_is_bounded() {
        let (_, control) = MockEmulation::new();
        let mut emulation = emulation_with(&control);
        emulation.create(0).await;

        control.lock().unwrap().stall_destroy = true;
        assert!(!emulation.destroy_bounded(0).await);
        assert!(emulation.handles.contains_key(&0));
        control.lock().unwrap().stall_destroy = false;

        control.lock().unwrap().stall_terminate = true;
        // terminate must return even though the backend terminate never completes
        tokio::time::timeout(Duration::from_secs(1), emulation.terminate())
            .await
            .expect("terminate must be bounded");
        assert!(control.lock().unwrap().terminated);
        assert!(emulation.handles.is_empty());
    }
}
