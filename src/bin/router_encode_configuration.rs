use router::RouterConfigurationEncoder;

fn main() {
    if let Err(error) = RouterConfigurationEncoder::from_environment().run() {
        eprintln!("router-encode-configuration: {error}");
        std::process::exit(1);
    }
}
