use router::RouterCommand;

fn main() {
    if let Err(error) = RouterCommand::from_env().run(std::io::stdout().lock()) {
        eprintln!("router: {error}");
        std::process::exit(1);
    }
}
