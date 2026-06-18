use router::RouterHarnessWitness;

fn main() {
    let witness = match RouterHarnessWitness::from_environment() {
        Ok(witness) => witness,
        Err(error) => {
            eprintln!("router-harness-witness: {error}");
            std::process::exit(1);
        }
    };
    if let Err(error) = witness.run() {
        eprintln!("router-harness-witness: {error}");
        std::process::exit(1);
    }
}
