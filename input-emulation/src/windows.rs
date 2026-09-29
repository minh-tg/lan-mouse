use super::error::{EmulationError, WindowsEmulationCreationError};
use input_event::{
    BTN_BACK, BTN_FORWARD, BTN_LEFT, BTN_MIDDLE, BTN_RIGHT, Event, KeyboardEvent, PointerEvent,
    scancode,
};

use async_trait::async_trait;
use std::io;
use std::ops::BitOrAssign;
use std::time::Duration;
use tokio::task::AbortHandle;
use windows::Win32::UI::Input::KeyboardAndMouse::{
    INPUT, INPUT_KEYBOARD, INPUT_MOUSE, KEYBDINPUT, KEYEVENTF_KEYUP, KEYEVENTF_SCANCODE,
    MOUSEEVENTF_HWHEEL, MOUSEEVENTF_LEFTDOWN, MOUSEEVENTF_LEFTUP, MOUSEEVENTF_MIDDLEDOWN,
    MOUSEEVENTF_MIDDLEUP, MOUSEEVENTF_MOVE, MOUSEEVENTF_RIGHTDOWN, MOUSEEVENTF_RIGHTUP,
    MOUSEEVENTF_WHEEL, MOUSEINPUT,
};
use windows::Win32::UI::Input::KeyboardAndMouse::{
    INPUT_0, KEYEVENTF_EXTENDEDKEY, MOUSEEVENTF_XDOWN, MOUSEEVENTF_XUP, SendInput,
};
use windows::Win32::UI::WindowsAndMessaging::{XBUTTON1, XBUTTON2};

use super::{Emulation, EmulationHandle};

const DEFAULT_REPEAT_DELAY: Duration = Duration::from_millis(500);
const DEFAULT_REPEAT_INTERVAL: Duration = Duration::from_millis(32);

pub(crate) struct WindowsEmulation {
    repeat_task: Option<AbortHandle>,
}

impl WindowsEmulation {
    pub(crate) fn new() -> Result<Self, WindowsEmulationCreationError> {
        Ok(Self { repeat_task: None })
    }
}

#[async_trait]
impl Emulation for WindowsEmulation {
    async fn consume(&mut self, event: Event, _: EmulationHandle) -> Result<(), EmulationError> {
        match event {
            Event::Pointer(pointer_event) => match pointer_event {
                PointerEvent::Motion { time: _, dx, dy } => {
                    rel_mouse(dx as i32, dy as i32)?;
                }
                PointerEvent::Button {
                    time: _,
                    button,
                    state,
                } => mouse_button(button, state)?,
                PointerEvent::Axis {
                    time: _,
                    axis,
                    value,
                } => scroll(axis, value as i32)?,
                PointerEvent::AxisDiscrete120 { axis, value } => scroll(axis, value)?,
            },
            Event::Keyboard(keyboard_event) => match keyboard_event {
                KeyboardEvent::Key {
                    time: _,
                    key,
                    state,
                } => {
                    // The repeat helpers do not suspend, so this key transition is
                    // posted before `consume` can be cancelled. If they start awaiting,
                    // override `retry_pending` to avoid submitting it twice.
                    match state {
                        // pressed
                        0 => self.kill_repeat_task(),
                        1 => self.spawn_repeat_task(key).await,
                        _ => {}
                    }
                    key_event(key, state)?;
                }
                KeyboardEvent::Modifiers { .. } => {}
            },
        }
        // FIXME
        Ok(())
    }

    async fn create(&mut self, _handle: EmulationHandle) {}

    async fn destroy(&mut self, _handle: EmulationHandle) {}

    async fn terminate(&mut self) {}
}

impl WindowsEmulation {
    async fn spawn_repeat_task(&mut self, key: u32) {
        // there can only be one repeating key and it's
        // always the last to be pressed
        self.kill_repeat_task();
        let repeat_task = tokio::task::spawn_local(async move {
            tokio::time::sleep(DEFAULT_REPEAT_DELAY).await;
            loop {
                if let Err(e) = key_event(key, 1) {
                    // A refused injection will not fix itself by retrying in a tight
                    // loop; stop the repeat and let the failing source be handled by
                    // the caller instead of spinning here.
                    log::warn!("stopping key repeat for key {key}: {e}");
                    break;
                }
                tokio::time::sleep(DEFAULT_REPEAT_INTERVAL).await;
            }
        });
        self.repeat_task = Some(repeat_task.abort_handle());
    }
    fn kill_repeat_task(&mut self) {
        if let Some(task) = self.repeat_task.take() {
            task.abort();
        }
    }
}

/// Attempts made for a single `SendInput` call before reporting a failure.
///
/// `SendInput` returns zero when the injection is refused, which happens for example
/// while a higher-integrity (elevated) window has focus. That refusal often clears by
/// itself, but `consume` must never wait indefinitely: an unbounded retry would stall
/// the emulation task and prevent the bounded cleanup of held keys from running.
/// Retrying a few times absorbs a momentary refusal; a persistent one is reported as
/// `EmulationError::InputRefused`, so that only the affected source is dropped and
/// retried later.
const MAX_SEND_INPUT_ATTEMPTS: usize = 3;

/// Submits one input event, reporting a transient error if the OS refuses it.
fn send_input(input: INPUT) -> Result<(), EmulationError> {
    for _ in 0..MAX_SEND_INPUT_ATTEMPTS {
        // SAFETY: `input` is a fully initialized `INPUT` and the slice length matches
        // the `cbSize` argument, as required by `SendInput`.
        if unsafe { SendInput(&[input], std::mem::size_of::<INPUT>() as i32) } > 0 {
            return Ok(());
        }
    }
    Err(EmulationError::InputRefused(io::Error::last_os_error()))
}

fn send_mouse_input(mi: MOUSEINPUT) -> Result<(), EmulationError> {
    send_input(INPUT {
        r#type: INPUT_MOUSE,
        Anonymous: INPUT_0 { mi },
    })
}

fn send_keyboard_input(ki: KEYBDINPUT) -> Result<(), EmulationError> {
    send_input(INPUT {
        r#type: INPUT_KEYBOARD,
        Anonymous: INPUT_0 { ki },
    })
}
fn rel_mouse(dx: i32, dy: i32) -> Result<(), EmulationError> {
    let mi = MOUSEINPUT {
        dx,
        dy,
        mouseData: 0,
        dwFlags: MOUSEEVENTF_MOVE,
        time: 0,
        dwExtraInfo: 0,
    };
    send_mouse_input(mi)
}

fn mouse_button(button: u32, state: u32) -> Result<(), EmulationError> {
    let dw_flags = match state {
        0 => match button {
            BTN_LEFT => MOUSEEVENTF_LEFTUP,
            BTN_RIGHT => MOUSEEVENTF_RIGHTUP,
            BTN_MIDDLE => MOUSEEVENTF_MIDDLEUP,
            BTN_BACK => MOUSEEVENTF_XUP,
            BTN_FORWARD => MOUSEEVENTF_XUP,
            _ => return Ok(()),
        },
        1 => match button {
            BTN_LEFT => MOUSEEVENTF_LEFTDOWN,
            BTN_RIGHT => MOUSEEVENTF_RIGHTDOWN,
            BTN_MIDDLE => MOUSEEVENTF_MIDDLEDOWN,
            BTN_BACK => MOUSEEVENTF_XDOWN,
            BTN_FORWARD => MOUSEEVENTF_XDOWN,
            _ => return Ok(()),
        },
        _ => return Ok(()),
    };
    let mouse_data = match button {
        BTN_BACK => XBUTTON1 as u32,
        BTN_FORWARD => XBUTTON2 as u32,
        _ => 0,
    };
    let mi = MOUSEINPUT {
        dx: 0,
        dy: 0, // no movement
        mouseData: mouse_data,
        dwFlags: dw_flags,
        time: 0,
        dwExtraInfo: 0,
    };
    send_mouse_input(mi)
}

fn scroll(axis: u8, value: i32) -> Result<(), EmulationError> {
    let event_type = match axis {
        0 => MOUSEEVENTF_WHEEL,
        1 => MOUSEEVENTF_HWHEEL,
        _ => return Ok(()),
    };
    let mi = MOUSEINPUT {
        dx: 0,
        dy: 0,
        mouseData: -value as u32,
        dwFlags: event_type,
        time: 0,
        dwExtraInfo: 0,
    };
    send_mouse_input(mi)
}

fn key_event(key: u32, state: u8) -> Result<(), EmulationError> {
    let scancode = match linux_keycode_to_windows_scancode(key) {
        Some(code) => code,
        None => return Ok(()),
    };
    let extended = scancode > 0xff;
    let scancode = scancode & 0xff;
    let mut flags = KEYEVENTF_SCANCODE;
    if extended {
        flags.bitor_assign(KEYEVENTF_EXTENDEDKEY);
    }
    if state == 0 {
        flags.bitor_assign(KEYEVENTF_KEYUP);
    }
    let ki = KEYBDINPUT {
        wVk: Default::default(),
        wScan: scancode,
        dwFlags: flags,
        time: 0,
        dwExtraInfo: 0,
    };
    send_keyboard_input(ki)
}

fn linux_keycode_to_windows_scancode(linux_keycode: u32) -> Option<u16> {
    let linux_scancode = match scancode::Linux::try_from(linux_keycode) {
        Ok(s) => s,
        Err(_) => {
            log::warn!("unknown keycode: {linux_keycode}");
            return None;
        }
    };
    log::trace!("linux code: {linux_scancode:?}");
    let windows_scancode = match scancode::Windows::try_from(linux_scancode) {
        Ok(s) => s,
        Err(_) => {
            log::warn!("failed to translate linux code into windows scancode: {linux_scancode:?}");
            return None;
        }
    };
    log::trace!("windows code: {windows_scancode:?}");
    Some(windows_scancode as u16)
}
