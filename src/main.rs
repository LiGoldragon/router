use persona_router::RouterCommandLine;

fn main() -> persona_router::Result<()> {
    RouterCommandLine::from_env().run(std::io::stdout().lock())
}
