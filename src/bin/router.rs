use persona_router::RouterClient;

fn main() -> persona_router::Result<()> {
    RouterClient::from_environment()?.run(std::io::stdout())
}
