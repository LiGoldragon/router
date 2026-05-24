use nota_config::ConfigurationSource;
use router::RouterCommandLine;
use router::router::RouterDaemon;
use signal_router::RouterDaemonConfiguration;

fn main() -> router::Result<()> {
    // The supervised production launch passes a typed
    // `RouterDaemonConfiguration` as argv[1]. The same binary also
    // serves the CLI (and standalone `daemon --socket --store ...`)
    // surface; pick the typed path when argv looks like a
    // configuration source.
    if first_argument_is_typed_configuration_source() {
        let configuration: RouterDaemonConfiguration =
            ConfigurationSource::from_argv()?.decode()?;
        return RouterDaemon::from_configuration(configuration)?.run();
    }
    RouterCommandLine::from_env().run(std::io::stdout().lock())
}

fn first_argument_is_typed_configuration_source() -> bool {
    let Some(argument) = std::env::args_os().nth(1) else {
        return false;
    };
    let lossy = argument.to_string_lossy();
    if lossy.starts_with('(') {
        return true;
    }
    let path = std::path::Path::new(&argument);
    matches!(
        path.extension().and_then(|extension| extension.to_str()),
        Some("nota") | Some("rkyv")
    )
}
