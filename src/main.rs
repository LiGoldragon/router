use router::{DaemonEntry, RouterProcessDaemon};

fn main() -> std::process::ExitCode {
    RouterProcessDaemon::run_to_exit_code()
}
