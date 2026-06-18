use router::RouterBootstrapEncoder;

fn main() {
    if let Err(error) = RouterBootstrapEncoder::from_environment().run() {
        eprintln!("router-encode-bootstrap: {error}");
        std::process::exit(1);
    }
}
