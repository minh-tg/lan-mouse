#[derive(Debug, Error)]
pub enum InputEmulationError {
    #[error("error creating input-emulation: `{0}`")]
    Create(#[from] EmulationCreationError),
    #[error("error emulating input: `{0}`")]
    Emulate(#[from] EmulationError),
}

#[cfg(any(libei, rdp))]
use ashpd::{Error::Response, desktop::ResponseError};
use std::io;
use thiserror::Error;

#[cfg(wlroots)]
use wayland_client::{
    ConnectError, DispatchError,
    backend::WaylandError,
    globals::{BindError, GlobalError},
};

#[derive(Debug, Error)]
pub enum EmulationError {
    #[error("event stream closed")]
    EndOfStream,
    #[cfg(libei)]
    #[error("libei error: `{0}`")]
    Libei(#[from] reis::Error),
    #[cfg(wlroots)]
    #[error("wayland error: `{0}`")]
    Wayland(#[from] wayland_client::backend::WaylandError),
    #[cfg(any(rdp, libei))]
    #[error("xdg-desktop-portal: `{0}`")]
    Ashpd(#[from] ashpd::Error),
    #[error("io error: `{0}`")]
    Io(#[from] io::Error),
    /// The operating system refused to inject the event, for example `SendInput` while
    /// a higher-integrity window has focus. The refusal may clear on its own, so only
    /// the affected source is dropped and retried later.
    #[error("input injection was refused: `{0}`")]
    InputRefused(io::Error),
}

impl EmulationError {
    /// Returns whether the failure may clear on its own and only affects the event's
    /// source, so the caller can drop that source's input and retry it later.
    ///
    /// This covers an output socket that is momentarily full (`WouldBlock`) and an
    /// event the operating system refused to inject
    /// ([`InputRefused`](Self::InputRefused)).
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Io(error) => error.kind() == io::ErrorKind::WouldBlock,
            // The classification comes from the variant, not the error kind: the OS
            // error reported for a refused injection is not a reliable indicator.
            Self::InputRefused(_) => true,
            #[cfg(wlroots)]
            Self::Wayland(WaylandError::Io(error)) => error.kind() == io::ErrorKind::WouldBlock,
            _ => false,
        }
    }
}

#[derive(Debug, Error)]
pub enum EmulationCreationError {
    #[cfg(wlroots)]
    #[error("wlroots backend: `{0}`")]
    Wlroots(#[from] WlrootsEmulationCreationError),
    #[cfg(libei)]
    #[error("libei backend: `{0}`")]
    Libei(#[from] LibeiEmulationCreationError),
    #[cfg(rdp)]
    #[error("xdg-desktop-portal: `{0}`")]
    Xdp(#[from] XdpEmulationCreationError),
    #[cfg(x11)]
    #[error("x11: `{0}`")]
    X11(#[from] X11EmulationCreationError),
    #[cfg(target_os = "macos")]
    #[error("macos: `{0}`")]
    MacOs(#[from] MacOSEmulationCreationError),
    #[cfg(windows)]
    #[error("windows: `{0}`")]
    Windows(#[from] WindowsEmulationCreationError),
    #[error("capture error")]
    NoAvailableBackend,
}

impl EmulationCreationError {
    /// request was intentionally denied by the user
    pub(crate) fn cancelled_by_user(&self) -> bool {
        #[cfg(libei)]
        if matches!(
            self,
            EmulationCreationError::Libei(LibeiEmulationCreationError::Ashpd(Response(
                ResponseError::Cancelled,
            )))
        ) {
            return true;
        }
        #[cfg(rdp)]
        if matches!(
            self,
            EmulationCreationError::Xdp(XdpEmulationCreationError::Ashpd(Response(
                ResponseError::Cancelled,
            )))
        ) {
            return true;
        }
        false
    }
}

#[cfg(wlroots)]
#[derive(Debug, Error)]
pub enum WlrootsEmulationCreationError {
    #[error(transparent)]
    Connect(#[from] ConnectError),
    #[error(transparent)]
    Global(#[from] GlobalError),
    #[error(transparent)]
    Wayland(#[from] WaylandError),
    #[error(transparent)]
    Bind(#[from] WaylandBindError),
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(wlroots)]
#[derive(Debug, Error)]
#[error("wayland protocol \"{protocol}\" not supported: {inner}")]
pub struct WaylandBindError {
    inner: BindError,
    protocol: &'static str,
}

#[cfg(wlroots)]
impl WaylandBindError {
    pub(crate) fn new(inner: BindError, protocol: &'static str) -> Self {
        Self { inner, protocol }
    }
}

#[cfg(libei)]
#[derive(Debug, Error)]
pub enum LibeiEmulationCreationError {
    #[error(transparent)]
    Ashpd(#[from] ashpd::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Reis(#[from] reis::Error),
}

#[cfg(rdp)]
#[derive(Debug, Error)]
pub enum XdpEmulationCreationError {
    #[error(transparent)]
    Ashpd(#[from] ashpd::Error),
}

#[cfg(x11)]
#[derive(Debug, Error)]
pub enum X11EmulationCreationError {
    #[error("could not open display")]
    OpenDisplay,
}

#[cfg(target_os = "macos")]
#[derive(Debug, Error)]
pub enum MacOSEmulationCreationError {
    #[error("could not create event source")]
    EventSourceCreation,
    #[error("accessibility permission is required")]
    AccessibilityPermission,
    #[error("input control permission is required")]
    InputControlPermission,
}

#[cfg(windows)]
#[derive(Debug, Error)]
pub enum WindowsEmulationCreationError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emulation_error_identifies_transient_failures() {
        let io_error = EmulationError::Io(io::Error::from(io::ErrorKind::WouldBlock));
        assert!(io_error.is_transient());

        // A refused injection is transient even though its error kind is not
        // `WouldBlock`: the classification comes from the variant.
        let refused =
            EmulationError::InputRefused(io::Error::from(io::ErrorKind::PermissionDenied));
        assert!(refused.is_transient());

        let other_io_error = EmulationError::Io(io::Error::from(io::ErrorKind::Other));
        assert!(!other_io_error.is_transient());

        #[cfg(wlroots)]
        {
            let wayland_error = EmulationError::Wayland(WaylandError::Io(io::Error::from(
                io::ErrorKind::WouldBlock,
            )));
            assert!(wayland_error.is_transient());
        }
    }
}
