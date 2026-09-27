use futures::{StreamExt, future};
use std::{
    env, fs, io,
    os::{
        fd::{AsFd, OwnedFd},
        unix::net::UnixStream,
    },
    path::PathBuf,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{Interest, unix::AsyncFd},
    task::JoinHandle,
};

use ashpd::desktop::{
    PersistMode, Session,
    remote_desktop::{DeviceType, RemoteDesktop, SelectDevicesOptions},
};
use async_trait::async_trait;

use reis::{
    ei::{
        self, Button, Keyboard, Pointer, Scroll, button::ButtonState, handshake::ContextType,
        keyboard::KeyState,
    },
    event::{self, Connection, DeviceCapability, DeviceEvent, EiEvent, SeatEvent},
    tokio::EiConvertEventStream,
};

use input_event::{Event, KeyboardEvent, PointerEvent};

use crate::error::EmulationError;

use super::{Emulation, EmulationHandle, error::LibeiEmulationCreationError};

#[derive(Clone, Default)]
struct Devices {
    pointer: Arc<RwLock<Option<(ei::Device, ei::Pointer)>>>,
    scroll: Arc<RwLock<Option<(ei::Device, ei::Scroll)>>>,
    button: Arc<RwLock<Option<(ei::Device, ei::Button)>>>,
    keyboard: Arc<RwLock<Option<(ei::Device, ei::Keyboard)>>>,
}

pub(crate) struct LibeiEmulation {
    context: ei::Context,
    /// Readiness for the libei socket, used to retry a flush that hit `WouldBlock`.
    write_ready: Arc<AsyncFd<OwnedFd>>,
    conn: event::Connection,
    devices: Devices,
    ei_task: JoinHandle<()>,
    error: Arc<Mutex<Option<EmulationError>>>,
    libei_error: Arc<AtomicBool>,
    _remote_desktop: RemoteDesktop,
    session: Session<RemoteDesktop>,
}

/// Get the path to the RemoteDesktop token file
fn get_token_file_path() -> PathBuf {
    let cache_dir = env::var("XDG_CACHE_HOME")
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = env::var("HOME").expect("HOME not set");
            PathBuf::from(home).join(".cache")
        });

    cache_dir.join("lan-mouse").join("remote-desktop.token")
}

/// Read the RemoteDesktop token from file
fn read_token() -> Option<String> {
    let token_path = get_token_file_path();
    match fs::read_to_string(&token_path) {
        Ok(token) => Some(token.trim().to_string()),
        Err(_) => None,
    }
}

/// Write the RemoteDesktop token to file
fn write_token(token: &str) -> io::Result<()> {
    let token_path = get_token_file_path();
    if let Some(parent) = token_path.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::write(&token_path, token)?;
    Ok(())
}

async fn get_ei_fd() -> Result<(RemoteDesktop, Session<RemoteDesktop>, OwnedFd), ashpd::Error> {
    let remote_desktop = RemoteDesktop::new().await?;

    let restore_token = read_token();

    log::debug!("creating session ...");
    let session = remote_desktop.create_session(Default::default()).await?;

    log::debug!("selecting devices ...");
    let options = SelectDevicesOptions::default()
        .set_devices(DeviceType::Keyboard | DeviceType::Pointer)
        .set_persist_mode(PersistMode::ExplicitlyRevoked)
        .set_restore_token(restore_token.as_deref());
    remote_desktop.select_devices(&session, options).await?;

    log::info!("requesting permission for input emulation");
    let start_response = remote_desktop
        .start(&session, None, Default::default())
        .await?
        .response()?;

    // The restore token is only valid once, we need to re-save it each time
    if let Some(token_str) = start_response.restore_token() {
        if let Err(e) = write_token(token_str) {
            log::warn!("failed to save RemoteDesktop token: {}", e);
        }
    }

    let fd = remote_desktop
        .connect_to_eis(&session, Default::default())
        .await?;
    Ok((remote_desktop, session, fd))
}

impl LibeiEmulation {
    pub(crate) async fn new() -> Result<Self, LibeiEmulationCreationError> {
        let (_remote_desktop, session, eifd) = get_ei_fd().await?;
        let stream = UnixStream::from(eifd);
        stream.set_nonblocking(true)?;
        let context = ei::Context::new(stream)?;
        let (conn, events) = context
            .handshake_tokio("de.feschber.LanMouse", ContextType::Sender)
            .await?;
        let devices = Devices::default();
        let libei_error = Arc::new(AtomicBool::default());
        let error = Arc::new(Mutex::new(None));
        // Register a duplicate of the socket for writable readiness: the original fd
        // is already registered for readability by the reis event stream.
        let write_ready = Arc::new(AsyncFd::with_interest(
            context.as_fd().try_clone_to_owned()?,
            Interest::WRITABLE,
        )?);
        let ei_handler = ei_task(
            events,
            conn.clone(),
            context.clone(),
            write_ready.clone(),
            devices.clone(),
            libei_error.clone(),
            error.clone(),
        );
        let ei_task = tokio::task::spawn_local(ei_handler);

        Ok(Self {
            context,
            write_ready,
            conn,
            devices,
            ei_task,
            error,
            libei_error,
            _remote_desktop,
            session,
        })
    }
}

impl Drop for LibeiEmulation {
    fn drop(&mut self) {
        self.ei_task.abort();
    }
}

#[async_trait]
impl Emulation for LibeiEmulation {
    async fn consume(
        &mut self,
        event: Event,
        _handle: EmulationHandle,
    ) -> Result<(), EmulationError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        if self.libei_error.load(Ordering::SeqCst) {
            // don't break sending additional events but signal error
            if let Some(e) = self.error.lock().unwrap().take() {
                return Err(e);
            }
        }
        match event {
            Event::Pointer(p) => match p {
                PointerEvent::Motion { time: _, dx, dy } => {
                    let pointer_device = self.devices.pointer.read().unwrap();
                    if let Some((d, p)) = pointer_device.as_ref() {
                        p.motion_relative(dx as f32, dy as f32);
                        d.frame(self.conn.serial(), now);
                    }
                }
                PointerEvent::Button {
                    time: _,
                    button,
                    state,
                } => {
                    let button_device = self.devices.button.read().unwrap();
                    if let Some((d, b)) = button_device.as_ref() {
                        b.button(
                            button,
                            match state {
                                0 => ButtonState::Released,
                                _ => ButtonState::Press,
                            },
                        );
                        d.frame(self.conn.serial(), now);
                    }
                }
                PointerEvent::Axis {
                    time: _,
                    axis,
                    value,
                } => {
                    let scroll_device = self.devices.scroll.read().unwrap();
                    if let Some((d, s)) = scroll_device.as_ref() {
                        match axis {
                            0 => s.scroll(0., value as f32),
                            _ => s.scroll(value as f32, 0.),
                        }
                        d.frame(self.conn.serial(), now);
                    }
                }
                PointerEvent::AxisDiscrete120 { axis, value } => {
                    let scroll_device = self.devices.scroll.read().unwrap();
                    if let Some((d, s)) = scroll_device.as_ref() {
                        match axis {
                            0 => s.scroll_discrete(0, value),
                            _ => s.scroll_discrete(value, 0),
                        }
                        d.frame(self.conn.serial(), now);
                    }
                }
            },
            Event::Keyboard(k) => match k {
                KeyboardEvent::Key {
                    time: _,
                    key,
                    state,
                } => {
                    let keyboard_device = self.devices.keyboard.read().unwrap();
                    if let Some((d, k)) = keyboard_device.as_ref() {
                        k.key(
                            key,
                            match state {
                                0 => KeyState::Released,
                                _ => KeyState::Press,
                            },
                        );
                        d.frame(self.conn.serial(), now);
                    }
                }
                KeyboardEvent::Modifiers { .. } => {}
            },
        }
        flush_context(&self.context, &self.write_ready).await?;
        Ok(())
    }

    async fn retry_pending(
        &mut self,
        _event: Event,
        _handle: EmulationHandle,
    ) -> Result<(), EmulationError> {
        // The original event is already in reis's write buffer. Flush it rather than
        // appending a second copy when the caller retries after cancellation.
        retry_pending_context(&self.context, &self.write_ready).await?;
        Ok(())
    }

    async fn create(&mut self, _: EmulationHandle) {}
    async fn destroy(&mut self, _: EmulationHandle) {}

    async fn terminate(&mut self) {
        let _ = self.session.close().await;
        self.ei_task.abort();
    }
}

/// Flushes buffered libei requests, waiting for writable readiness on `WouldBlock`.
///
/// `reis` only drains the bytes the kernel accepted and keeps the unsent suffix in
/// its write buffer when a socket write reports `WouldBlock`
/// (`wire::backend::Buffer::flush_write`), so flushing again once the socket becomes
/// writable resumes the buffered messages instead of dropping them.
async fn flush_context(context: &ei::Context, write_ready: &AsyncFd<OwnedFd>) -> io::Result<()> {
    loop {
        match context.flush() {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // The kernel send buffer is full; wait until it accepts more data and
                // resume the buffered flush afterwards.
                write_ready.writable().await?.clear_ready();
            }
            Err(e) => return Err(io::Error::new(e.kind(), e)),
        }
    }
}

/// Resume output left in reis's write buffer without appending the event again.
async fn retry_pending_context(
    context: &ei::Context,
    write_ready: &AsyncFd<OwnedFd>,
) -> io::Result<()> {
    flush_context(context, write_ready).await
}

async fn ei_task(
    mut events: EiConvertEventStream,
    _conn: Connection,
    context: ei::Context,
    write_ready: Arc<AsyncFd<OwnedFd>>,
    devices: Devices,
    libei_error: Arc<AtomicBool>,
    error: Arc<Mutex<Option<EmulationError>>>,
) {
    loop {
        match ei_event_handler(&mut events, &context, &write_ready, &devices).await {
            Ok(()) => {}
            Err(e) => {
                libei_error.store(true, Ordering::SeqCst);
                error.lock().unwrap().replace(e);
                // wait for termination -> otherwise we will loop forever
                future::pending::<()>().await;
            }
        }
    }
}

async fn ei_event_handler(
    events: &mut EiConvertEventStream,
    context: &ei::Context,
    write_ready: &AsyncFd<OwnedFd>,
    devices: &Devices,
) -> Result<(), EmulationError> {
    loop {
        let event = events.next().await.ok_or(EmulationError::EndOfStream)??;
        let capabilities = DeviceCapability::Pointer
            | DeviceCapability::PointerAbsolute
            | DeviceCapability::Keyboard
            | DeviceCapability::Touch
            | DeviceCapability::Scroll
            | DeviceCapability::Button;
        log::debug!("{event:?}");
        match event {
            EiEvent::Disconnected(e) => {
                log::debug!("ei disconnected: {e:?}");
                return Err(EmulationError::EndOfStream);
            }
            EiEvent::SeatAdded(e) => {
                e.seat().bind_capabilities(capabilities);
            }
            EiEvent::SeatRemoved(e) => {
                log::debug!("seat removed: {:?}", e.seat());
            }
            EiEvent::DeviceAdded(e) => {
                let device_type = e.device().device_type();
                log::debug!("device added: {device_type:?}");
                let device = e.device();
                if let Some(pointer) = e.device().interface::<Pointer>() {
                    devices
                        .pointer
                        .write()
                        .unwrap()
                        .replace((device.device().clone(), pointer));
                }
                if let Some(keyboard) = e.device().interface::<Keyboard>() {
                    devices
                        .keyboard
                        .write()
                        .unwrap()
                        .replace((device.device().clone(), keyboard));
                }
                if let Some(scroll) = e.device().interface::<Scroll>() {
                    devices
                        .scroll
                        .write()
                        .unwrap()
                        .replace((device.device().clone(), scroll));
                }
                if let Some(button) = e.device().interface::<Button>() {
                    devices
                        .button
                        .write()
                        .unwrap()
                        .replace((device.device().clone(), button));
                }
            }
            EiEvent::DeviceRemoved(e) => {
                log::debug!("device removed: {:?}", e.device().device_type());
            }
            EiEvent::DevicePaused(e) => {
                log::debug!("device paused: {:?}", e.device().device_type());
            }
            EiEvent::DeviceResumed(e) => {
                log::debug!("device resumed: {:?}", e.device().device_type());
                e.device().device().start_emulating(0, 0);
            }
            EiEvent::KeyboardModifiers(e) => {
                log::debug!("modifiers: {e:?}");
            }
            // only for receiver context
            // EiEvent::Frame(_) => { },
            // EiEvent::DeviceStartEmulating(_) => { },
            // EiEvent::DeviceStopEmulating(_) => { },
            // EiEvent::PointerMotion(_) => { },
            // EiEvent::PointerMotionAbsolute(_) => { },
            // EiEvent::Button(_) => { },
            // EiEvent::ScrollDelta(_) => { },
            // EiEvent::ScrollStop(_) => { },
            // EiEvent::ScrollCancel(_) => { },
            // EiEvent::ScrollDiscrete(_) => { },
            // EiEvent::KeyboardKey(_) => { },
            // EiEvent::TouchDown(_) => { },
            // EiEvent::TouchUp(_) => { },
            // EiEvent::TouchMotion(_) => { },
            _ => unreachable!("unexpected ei event"),
        }
        flush_context(context, write_ready).await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    /// Larger than the default unix socket send buffer (typically ~200 KiB), so a
    /// single buffered message cannot be flushed in one go.
    const FILL_PAYLOAD: usize = 4 << 20;

    /// A full kernel send buffer must not drop buffered events: `flush_context`
    /// observes `WouldBlock`, waits for writable readiness and resumes the flush once
    /// the peer drains the socket.
    #[tokio::test]
    async fn flush_resumes_after_would_block() {
        let (socket, peer) = UnixStream::pair().unwrap();
        // Mimic the production layout: reis registers the socket for read readiness
        // while the write readiness uses a duplicate of the same socket.
        let read_ready = AsyncFd::with_interest(socket, Interest::READABLE).unwrap();
        let context = ei::Context::new(read_ready.get_ref().try_clone().unwrap()).unwrap();
        let write_ready = AsyncFd::with_interest(
            context.as_fd().try_clone_to_owned().unwrap(),
            Interest::WRITABLE,
        )
        .unwrap();

        // Buffer a message larger than the send buffer, then flush once: the kernel
        // accepts a prefix and reports `WouldBlock` for the rest, which reis keeps.
        let payload = "x".repeat(FILL_PAYLOAD);
        context.handshake().interface_version(&payload, 1);
        let err = context
            .flush()
            .expect_err("flushing a full send buffer must report WouldBlock");
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

        peer.set_nonblocking(true).unwrap();
        let mut peer = tokio::net::UnixStream::from_std(peer).unwrap();
        let flush = retry_pending_context(&context, &write_ready);
        tokio::pin!(flush);

        // The peer is not draining yet, so the flush must stay pending.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut flush)
                .await
                .is_err(),
            "flush returned although the send buffer is still full"
        );

        // Drain the peer and require the whole buffered payload to arrive within a
        // bounded time; an EOF instead of the payload must fail loudly, not hang.
        let drain = async move {
            let mut seen = 0usize;
            let mut buf = [0u8; 64 * 1024];
            while seen < FILL_PAYLOAD {
                let n = peer.read(&mut buf).await.expect("peer read failed");
                assert!(n > 0, "peer reached EOF before the flush completed");
                seen += buf[..n].iter().filter(|&&b| b == b'x').count();
            }
            seen
        };

        let (flush_result, payload_bytes) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(&mut flush, drain)
        })
        .await
        .expect("buffered flush did not resume within the timeout");
        flush_result.expect("retrying flush failed");
        assert_eq!(payload_bytes, FILL_PAYLOAD);
    }
}
