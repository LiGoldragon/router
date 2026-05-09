use persona_router::RouterDaemon;

fn main() -> persona_router::Result<()> {
    RouterDaemon::from_environment()?.run()
}
