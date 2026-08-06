use thiserror::Error;
use triad_runtime::{
    AcceptedConnection, ArgumentError, AsyncListenerError, AsyncListenerSocket,
    AsyncMultiConnectionRuntime, AsyncMultiListenerDaemon, AsyncMultiListenerDaemonError,
    BindingSurface, ComponentArgument, ComponentCommand, ExitReport, RequestErrorLog, SocketMode,
};
/// The component hook surface for Router's daemon runtime.
pub trait ComponentDaemon: Sized + 'static {
    type Configuration: BindingSurface;
    type ConfigurationError: std::error::Error;
    type Engine: Send + Sync + 'static;
    type Error: std::fmt::Display + Send + Sync + 'static;
    const PROCESS_NAME: &'static str;
    /// Load the binary rkyv `Configuration` from the daemon's single argument.
    fn load_configuration(
        path: &std::path::Path,
    ) -> Result<Self::Configuration, Self::ConfigurationError>;
    /// Validate the loaded configuration before any runtime, listener,
    /// or store is built. Components that carry only already-validated
    /// typed configuration keep the default no-op; components with decoded
    /// path records override this hook so bad startup shape fails before
    /// socket preparation or state mutation.
    fn validate_configuration(
        configuration: &Self::Configuration,
    ) -> Result<(), Self::ConfigurationError> {
        let _ = configuration;
        Ok(())
    }
    /// Open the component's durable Store and construct its Engine.
    fn build_runtime(configuration: &Self::Configuration) -> Result<Self::Engine, Self::Error>;
    /// Lifecycle: called once before the listener serves, once after it stops.
    fn start(engine: &Self::Engine) -> Result<(), Self::Error> {
        let _ = engine;
        Ok(())
    }
    fn stop(engine: &Self::Engine) -> Result<(), Self::Error> {
        let _ = engine;
        Ok(())
    }
    /// Run one accepted working connection.
    fn handle_working_connection(
        engine: &Self::Engine,
        connection: AcceptedConnection,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send + '_;
    /// Run one accepted meta connection.
    fn handle_meta_connection(
        engine: &Self::Engine,
        connection: AcceptedConnection,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send + '_ {
        async move {
            let _ = engine;
            let _ = connection;
            Ok(())
        }
    }
}
/// argv -> binary `Configuration` -> the bound daemon. The single-argument
/// rule: exactly one argument, a signal-encoded (rkyv) configuration file.
pub struct DaemonCommand<Daemon: ComponentDaemon> {
    command: ComponentCommand,
    daemon: std::marker::PhantomData<fn() -> Daemon>,
}
impl<Daemon: ComponentDaemon> DaemonCommand<Daemon> {
    pub fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
            daemon: std::marker::PhantomData,
        }
    }
    pub fn from_arguments<Arguments, Argument>(arguments: Arguments) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self {
            command: ComponentCommand::from_arguments(arguments),
            daemon: std::marker::PhantomData,
        }
    }
    pub fn configuration(&self) -> Result<Daemon::Configuration, DaemonError<Daemon>> {
        match self.command.signal_file_argument()? {
            ComponentArgument::SignalFile(file) => {
                let configuration = Daemon::load_configuration(file.as_path())
                    .map_err(DaemonError::Configuration)?;
                Daemon::validate_configuration(&configuration)
                    .map_err(DaemonError::Configuration)?;
                Ok(configuration)
            }
            ComponentArgument::InlineNota(_) | ComponentArgument::NotaFile(_) => {
                Err(DaemonError::Argument(ArgumentError::ExpectedSignalFile))
            }
        }
    }
    pub fn run(&self) -> Result<(), DaemonError<Daemon>> {
        tokio::runtime::Runtime::new()
            .map_err(DaemonError::Runtime)?
            .block_on(async {
                Daemon::bind(self.configuration()?)?
                    .run()
                    .await
                    .map_err(DaemonError::from)
            })
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListenerTier {
    Working,
    Meta,
}
impl std::fmt::Display for ListenerTier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Working => formatter.write_str("working"),
            Self::Meta => formatter.write_str("meta"),
        }
    }
}
/// The bound daemon constructor on the component trait: builds the engine,
/// wraps it in the component connection runtime, and returns the
/// async task-backed listener shell the `DaemonCommand` drives. The component
/// supplies policy through `ComponentDaemon`; this module owns mechanics.
pub trait DaemonBinder: ComponentDaemon {
    fn bind(
        configuration: Self::Configuration,
    ) -> Result<AsyncMultiListenerDaemon<ComponentDaemonRuntime<Self>>, DaemonError<Self>> {
        let engine = Self::build_runtime(&configuration).map_err(DaemonError::Component)?;
        let runtime = ComponentDaemonRuntime::<Self>::new(engine);
        Ok({
            let working_socket = AsyncListenerSocket::new(
                ListenerTier::Working,
                configuration.socket_path().to_path_buf(),
            );
            let working_socket = match configuration.socket_mode() {
                Some(socket_mode) => working_socket.with_socket_mode(socket_mode),
                None => working_socket,
            };
            let mut listener_sockets = std::vec![working_socket];
            let meta_socket_path = configuration
                .meta_socket_path()
                .ok_or(DaemonError::MissingMetaSocket)?
                .to_path_buf();
            listener_sockets.push(
                AsyncListenerSocket::new(ListenerTier::Meta, meta_socket_path)
                    .with_socket_mode(SocketMode::new(0o600)),
            );
            AsyncMultiListenerDaemon::new(
                listener_sockets,
                runtime.clone(),
                RequestErrorLog::new(Self::PROCESS_NAME),
            )
            .with_concurrency_limit(configuration.request_concurrency_limit())
        })
    }
}
impl<Daemon: ComponentDaemon> DaemonBinder for Daemon {}
/// The runtime struct that owns the engine. Its
/// `handle_connection` IS the async decode -> execute -> encode spine.
pub struct ComponentDaemonRuntime<Daemon: ComponentDaemon> {
    engine: std::sync::Arc<Daemon::Engine>,
}
impl<Daemon: ComponentDaemon> ComponentDaemonRuntime<Daemon> {
    fn new(engine: Daemon::Engine) -> Self {
        Self {
            engine: std::sync::Arc::new(engine),
        }
    }
    async fn handle_working_connection(
        &self,
        connection: AcceptedConnection,
    ) -> Result<(), Daemon::Error> {
        Daemon::handle_working_connection(self.engine.as_ref(), connection).await
    }
}
impl<Daemon: ComponentDaemon> Clone for ComponentDaemonRuntime<Daemon> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
        }
    }
}
impl<Daemon: ComponentDaemon> AsyncMultiConnectionRuntime for ComponentDaemonRuntime<Daemon> {
    type Listener = ListenerTier;
    type Error = Daemon::Error;
    async fn start(&self) -> Result<(), Self::Error> {
        Daemon::start(self.engine.as_ref())
    }
    async fn stop(&self) -> Result<(), Self::Error> {
        Daemon::stop(self.engine.as_ref())
    }
    async fn handle_connection(
        &self,
        listener: Self::Listener,
        connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        match listener {
            ListenerTier::Working => self.handle_working_connection(connection).await,
            ListenerTier::Meta => {
                Daemon::handle_meta_connection(self.engine.as_ref(), connection).await
            }
        }
    }
}
/// The daemon error: argv, configuration, listener, and the
/// component error. The component's own error rides the `Component` arm.
#[derive(Debug, Error)]
pub enum DaemonError<Daemon: ComponentDaemon> {
    #[error("daemon argument error: {0}")]
    Argument(ArgumentError),
    #[error("daemon configuration error: {0}")]
    Configuration(Daemon::ConfigurationError),
    #[error("daemon runtime error: {0}")]
    Runtime(std::io::Error),
    #[error("daemon listener error: {0}")]
    Listener(AsyncListenerError),
    #[error("daemon meta socket path missing from configuration")]
    MissingMetaSocket,
    #[error("component error: {0}")]
    Component(Daemon::Error),
}
impl<Daemon: ComponentDaemon> From<ArgumentError> for DaemonError<Daemon> {
    fn from(error: ArgumentError) -> Self {
        Self::Argument(error)
    }
}
impl<Daemon: ComponentDaemon> From<AsyncMultiListenerDaemonError<Daemon::Error>>
    for DaemonError<Daemon>
{
    fn from(error: AsyncMultiListenerDaemonError<Daemon::Error>) -> Self {
        match error {
            AsyncMultiListenerDaemonError::Listener(error) => Self::Listener(error),
            AsyncMultiListenerDaemonError::Start(error)
            | AsyncMultiListenerDaemonError::Stop(error) => Self::Component(error),
        }
    }
}
/// The component-agnostic exit body. The component's binary calls
/// `<SpiritDaemon as DaemonEntry>::run_to_exit_code()` from `fn main`.
pub trait DaemonEntry: ComponentDaemon {
    fn run_to_exit_code() -> std::process::ExitCode {
        ExitReport::new(Self::PROCESS_NAME)
            .from_result(DaemonCommand::<Self>::from_environment().run())
    }
}
impl<Daemon: ComponentDaemon> DaemonEntry for Daemon {}
