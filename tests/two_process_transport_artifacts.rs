//! The deploy-artifact + process-boundary witness beneath the two-kernel
//! `runNixOSTest` (report 136 rung L1). It exercises the exact machinery the
//! NixOS module wires, but as two real `router-daemon` OS processes over
//! loopback TCP on one kernel — so it runs in CI with no `/dev/kvm`:
//!
//!   1. the deploy-time NOTA → rkyv encoders (`router-encode-configuration`,
//!      `router-encode-bootstrap`) produce the two binary artifacts the daemon
//!      consumes (the daemon takes ONE rkyv arg, no flags, and rejects NOTA);
//!   2. a receiving daemon (`prometheus`) configured with a tailnet ingress
//!      binds that ingress eagerly at startup (the `start` lifecycle hook), so
//!      a peer can forward to it the instant it is ready;
//!   3. the `router-forward-probe` — the cross-host transport's first client —
//!      sends a real `signal-router::ForwardMessage` frame to the receiver's
//!      ingress and reads the typed reply.
//!
//! The load-bearing assertions:
//!   - a happy `Origin` forward is accepted with a REAL minted slot (`!= 0`),
//!     the durable peer receipt;
//!   - that forward is actually DELIVERED to the receiving actor: the
//!     receiver homes `prometheus-responder` on a `HarnessSocket` endpoint
//!     bound by this test, and the forwarded message arrives there — the same
//!     delivery witness the in-process loopback test
//!     (`tests/end_to_end_remote_forward.rs`) uses, now across an OS process
//!     boundary, so the test proves delivery and not merely receipt;
//!   - a second daemon registers the receiver as a remote router via a
//!     bootstrap document carrying `RegisterRemoteRouter` (the deploy path),
//!     and its tailnet ingress comes up bound to its own address;
//!   - a `Forwarded`-marked frame is refused with `AlreadyForwarded` by the
//!     receiver's loop guard and never delivered.
//!
//! Fully offline: loopback TCP, no criome (the receiving daemon has no
//! `criome_socket_path`, so the offline fixed-identity verifier is selected
//! and admits the probe's offline-identity attestation).

use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use signal_frame::{NonEmpty, Reply, SubReply};
use signal_harness::{
    DeliveryCompleted, HarnessEvent, HarnessFrame, HarnessFrameBody, HarnessName, HarnessRequest,
};

struct NodeFixture {
    directory: PathBuf,
    router_socket: PathBuf,
    meta_socket: PathBuf,
    supervision_socket: PathBuf,
    store: PathBuf,
    bootstrap_nota: PathBuf,
    bootstrap_rkyv: PathBuf,
    configuration_rkyv: PathBuf,
    witness_socket: PathBuf,
    identity: String,
    tailnet_port: u16,
}

impl NodeFixture {
    fn new(name: &str, identity: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "router-two-process-{name}-{}-{now}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).expect("create node fixture directory");
        Self {
            router_socket: directory.join("router.sock"),
            meta_socket: directory.join("router-meta.sock"),
            supervision_socket: directory.join("router-supervision.sock"),
            store: directory.join("router.sema"),
            bootstrap_nota: directory.join("router-bootstrap.nota"),
            bootstrap_rkyv: directory.join("router-bootstrap.rkyv"),
            configuration_rkyv: directory.join("router-config.rkyv"),
            witness_socket: directory.join("responder-witness.sock"),
            identity: identity.to_string(),
            tailnet_port: free_loopback_port(),
            directory,
        }
    }

    fn tailnet_address(&self) -> String {
        format!("127.0.0.1:{}", self.tailnet_port)
    }

    fn witness_socket_path(&self) -> String {
        self.witness_socket.display().to_string()
    }

    /// Author the bootstrap NOTA-lines document and seal it to rkyv through the
    /// real deploy encoder — the daemon will only accept the rkyv.
    fn encode_bootstrap(&self, lines: &str) {
        std::fs::write(&self.bootstrap_nota, lines).expect("write bootstrap nota");
        run_tool(
            env!("CARGO_BIN_EXE_router-encode-bootstrap"),
            &format!(
                "(RouterBootstrapArtifact {} {})",
                self.bootstrap_nota.display(),
                self.bootstrap_rkyv.display()
            ),
        );
    }

    /// Author the typed daemon configuration NOTA and seal it to rkyv through
    /// the real deploy encoder. `criome_socket_path` is `None`, selecting the
    /// offline verifier (milestone-3 criome attestation is intentionally not
    /// buildable yet).
    fn encode_configuration(&self, with_bootstrap: bool) {
        let bootstrap_field = if with_bootstrap {
            format!("(Some {})", self.bootstrap_rkyv.display())
        } else {
            "None".to_string()
        };
        let configuration = format!(
            "(RouterConfigurationArtifact ({router} 384 {meta} 384 {supervision} 384 {store} {bootstrap} (UnixUser 1000) (Some {tailnet}) {identity} None) {output})",
            router = self.router_socket.display(),
            meta = self.meta_socket.display(),
            supervision = self.supervision_socket.display(),
            store = self.store.display(),
            bootstrap = bootstrap_field,
            tailnet = self.tailnet_address(),
            identity = self.identity,
            output = self.configuration_rkyv.display(),
        );
        run_tool(
            env!("CARGO_BIN_EXE_router-encode-configuration"),
            &configuration,
        );
    }

    fn spawn_daemon(&self) -> DaemonProcess {
        let child = Command::new(env!("CARGO_BIN_EXE_router-daemon"))
            .arg(&self.configuration_rkyv)
            .spawn()
            .expect("spawn router daemon");
        let process = DaemonProcess { child };
        wait_for_socket(&self.router_socket);
        wait_for_tcp(self.tailnet_port);
        process
    }
}

impl Drop for NodeFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

struct DaemonProcess {
    child: Child,
}

impl Drop for DaemonProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The actor-harness delivery witness, the cross-process twin of the
/// in-process `HarnessWitness` in `tests/end_to_end_remote_forward.rs`: the
/// test process binds the `EndpointKind::HarnessSocket` Unix socket the
/// receiving daemon's `HarnessDelivery` connects to, accepts one
/// `signal-harness` `MessageDelivery`, reports it to the test thread, and
/// replies `DeliveryCompleted` so the daemon's delivery path closes. Proving
/// the witness receives the delivery proves the forward was DELIVERED to the
/// receiving actor, not merely accepted with a minted slot.
struct HarnessWitness {
    received: Receiver<WitnessedDelivery>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WitnessedDelivery {
    harness: String,
    sender: String,
    body: String,
}

impl HarnessWitness {
    fn bind(path: &Path) -> Self {
        if path.exists() {
            let _ = std::fs::remove_file(path);
        }
        let listener = UnixListener::bind(path).expect("harness witness socket binds");
        let (sender, received) = channel();
        // Accept deliveries in a loop: each `Origin` forward the receiver
        // accepts is also delivered to this socket, so the witness must serve
        // every connection the daemon's delivery path opens, not just the
        // first. The loop ends when the daemon shuts down and the listener
        // errors, which the test ignores.
        thread::spawn(move || {
            while let Ok((mut stream, _)) = listener.accept() {
                let frame = read_harness_frame(&mut stream);
                let HarnessFrameBody::Request { exchange, request } = frame.into_body() else {
                    panic!("expected harness request frame");
                };
                let HarnessRequest::MessageDelivery(delivery) = request.payloads().head().clone()
                else {
                    panic!("expected message delivery request");
                };
                sender
                    .send(WitnessedDelivery {
                        harness: delivery.harness.as_str().to_string(),
                        sender: delivery.sender.as_str().to_string(),
                        body: delivery.body.as_str().to_string(),
                    })
                    .expect("harness witness reports delivery");
                let reply = HarnessFrame::new(HarnessFrameBody::Reply {
                    exchange,
                    reply: Reply::committed(NonEmpty::single(SubReply::Ok(
                        HarnessEvent::DeliveryCompleted(DeliveryCompleted {
                            harness: HarnessName::new(delivery.harness.as_str()),
                            message_slot: delivery.message_slot,
                        }),
                    ))),
                });
                stream
                    .write_all(
                        reply
                            .encode_length_prefixed()
                            .expect("harness witness reply encodes")
                            .as_slice(),
                    )
                    .expect("harness witness writes reply");
                stream.flush().expect("harness witness flushes reply");
            }
        });
        Self { received }
    }

    fn received(&self) -> WitnessedDelivery {
        self.received
            .recv_timeout(Duration::from_secs(5))
            .expect("harness witness receives delivery")
    }
}

fn read_harness_frame(stream: &mut impl Read) -> HarnessFrame {
    let mut prefix = [0_u8; 4];
    stream
        .read_exact(&mut prefix)
        .expect("harness witness reads frame prefix");
    let length = u32::from_be_bytes(prefix) as usize;
    let mut bytes = Vec::with_capacity(4 + length);
    bytes.extend_from_slice(&prefix);
    bytes.resize(4 + length, 0);
    stream
        .read_exact(&mut bytes[4..])
        .expect("harness witness reads frame body");
    HarnessFrame::decode_length_prefixed(bytes.as_slice()).expect("harness frame decodes")
}

#[test]
fn two_router_daemon_processes_forward_over_loopback_with_real_minted_slot_and_loop_guard() {
    // The RECEIVER (prometheus): a tailnet ingress + a bootstrap registering a
    // local actor `prometheus-responder` HOMED ON A REAL `HarnessSocket`
    // endpoint and a channel grant for the probe-stamped sender `message`. The
    // endpoint is a Unix socket bound by this test process (the witness), so a
    // forward the daemon accepts is then DELIVERED to that actor harness — the
    // test proves delivery, not just the minted-slot receipt.
    let receiver = NodeFixture::new("prometheus", "prometheus-router");
    let witness = HarnessWitness::bind(&receiver.witness_socket);
    receiver.encode_bootstrap(&format!(
        "(RegisterActor ((prometheus-responder 7 (Some (HarnessSocket {witness} None))) None))\n\
         (GrantDirectMessage (message prometheus-responder))\n",
        witness = receiver.witness_socket_path(),
    ));
    receiver.encode_configuration(true);
    let _receiver_daemon = receiver.spawn_daemon();

    // The SENDER (ouranos): a tailnet ingress + a bootstrap registering the
    // receiver as a remote router (the deploy `RegisterRemoteRouter` path) and
    // homing `prometheus-responder` on it. Its own ingress binding proves the
    // configuration encoder produced a sound networked config for both nodes.
    let sender = NodeFixture::new("ouranos", "ouranos-router");
    sender.encode_bootstrap(&format!(
        "(RegisterRemoteRouter (prometheus-router {address}))\n\
         (RegisterActor ((prometheus-responder 7 None) (Some prometheus-router)))\n",
        address = receiver.tailnet_address(),
    ));
    sender.encode_configuration(true);
    let _sender_daemon = sender.spawn_daemon();

    // (1) + (2) A happy forward to the receiver's tailnet ingress is accepted
    // with a REAL minted slot (!= 0) — the durable peer receipt keyed to the
    // receiving router's own slot, not a placeholder.
    let accept_reply = run_probe(&format!(
        "(RouterForwardProbe {address} prometheus-responder two-process-forward-1 Origin [relay across the network])",
        address = receiver.tailnet_address(),
    ));
    let slot = parse_accepted_slot(&accept_reply);
    assert_ne!(
        slot, 0,
        "receiver should return a real minted slot, got {accept_reply:?}"
    );

    // (3) The forward was DELIVERED, not merely accepted: the receiving
    // daemon resolved `prometheus-responder`'s `HarnessSocket` endpoint and
    // delivered the message to the witness this test bound. The sender is
    // `message` — the probe stamps the forward's `from`, and the receiver's
    // channel grant `(message prometheus-responder)` authorizes it.
    let delivered = witness.received();
    assert_eq!(delivered.harness, "prometheus-responder");
    assert_eq!(delivered.sender, "message");
    assert_eq!(delivered.body, "relay across the network");

    // A distinct nonce, still Origin, still accepted — the receiver keeps
    // minting durable receipts for fresh forwards.
    let second_reply = run_probe(&format!(
        "(RouterForwardProbe {address} prometheus-responder two-process-forward-2 Origin [second relay])",
        address = receiver.tailnet_address(),
    ));
    let second_slot = parse_accepted_slot(&second_reply);
    assert_ne!(
        second_slot, 0,
        "second forward slot should be real: {second_reply:?}"
    );

    // (3) A `Forwarded`-marked frame — an object the wire claims was already
    // forwarded once — is refused with `AlreadyForwarded` by the loop guard,
    // before any delivery.
    let loop_reply = run_probe(&format!(
        "(RouterForwardProbe {address} prometheus-responder two-process-loop-guard-1 Forwarded [already forwarded once])",
        address = receiver.tailnet_address(),
    ));
    assert!(
        loop_reply.contains("ForwardRefused") && loop_reply.contains("AlreadyForwarded"),
        "Forwarded frame should be refused AlreadyForwarded, got {loop_reply:?}"
    );
}

fn run_tool(binary: &str, nota_argument: &str) -> String {
    let output = Command::new(binary)
        .arg(nota_argument)
        .output()
        .unwrap_or_else(|error| panic!("run {binary}: {error}"));
    assert!(
        output.status.success(),
        "{binary} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("tool stdout is utf8")
}

fn run_probe(nota_argument: &str) -> String {
    run_tool(env!("CARGO_BIN_EXE_router-forward-probe"), nota_argument)
        .trim()
        .to_string()
}

fn parse_accepted_slot(reply: &str) -> u64 {
    let trimmed = reply.trim();
    let inner = trimmed
        .strip_prefix("(ForwardAccepted ")
        .and_then(|rest| rest.strip_suffix(')'))
        .unwrap_or_else(|| panic!("expected (ForwardAccepted <slot>), got {reply:?}"));
    inner
        .trim()
        .parse::<u64>()
        .unwrap_or_else(|error| panic!("slot is not an integer in {reply:?}: {error}"))
}

fn free_loopback_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral loopback port");
    let port = listener
        .local_addr()
        .expect("read ephemeral local address")
        .port();
    drop(listener);
    port
}

fn wait_for_socket(path: &Path) {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(30) {
        if path.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("router socket did not appear at {}", path.display());
}

fn wait_for_tcp(port: u16) {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(30) {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("router tailnet ingress did not bind on 127.0.0.1:{port}");
}
