use router::MetaRouterCommand;

fn main() {
    if let Err(error) = MetaRouterCommand::from_env().run(std::io::stdout().lock()) {
        eprintln!("meta-router: {error}");
        std::process::exit(1);
    }
}
