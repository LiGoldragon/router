use router::{Result, RouterDaemonCommand};

fn main() -> Result<()> {
    RouterDaemonCommand::from_environment().run()
}
