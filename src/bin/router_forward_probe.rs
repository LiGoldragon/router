use router::RouterForwardProbe;

fn main() {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("router-forward-probe: tokio runtime: {error}");
            std::process::exit(1);
        }
    };
    if let Err(error) = runtime.block_on(RouterForwardProbe::from_environment().run()) {
        eprintln!("router-forward-probe: {error}");
        std::process::exit(1);
    }
}
