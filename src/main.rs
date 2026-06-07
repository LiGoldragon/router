use router::{RouterDaemonCommand, RouterResult};

fn main() -> RouterResult<()> {
    RouterDaemonCommand::from_environment().run()
}
