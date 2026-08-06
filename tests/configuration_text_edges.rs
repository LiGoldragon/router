#![cfg(feature = "dotos-text")]
//! Deploy-time text-edge witnesses for the networked router daemon.
//!
//! The NixOS persona-router module authors NOTA and runs two text edges at
//! `ExecStartPre`; the daemon then reads only rkyv. These tests drive the real
//! `router-write-configuration` and `router-write-bootstrap` binaries across
//! the process boundary and read the produced files back through the exact
//! daemon-side readers (`Configuration::from_binary_path`,
//! `RouterBootstrap::from_path`), proving the networked configuration and the
//! hardwired peer/actor-home tables survive the NOTA → rkyv crossing.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;

use router::{Configuration, RouterBootstrap};

fn state_directory(name: &str) -> PathBuf {
    let directory = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    std::fs::create_dir_all(&directory).expect("create test state directory");
    directory
}

#[test]
fn router_configuration_carries_listen_identity_and_criome_socket() {
    let directory = state_directory("router_configuration_text_edge");
    let bootstrap = directory.join("bootstrap.rkyv");
    let output = directory.join("router-daemon.rkyv");
    let request = format!(
        "(ConfigurationWriteRequest \
         /run/persona-router/router.sock \
         /run/persona-router/meta.sock \
         /run/persona-router/supervision.sock \
         /var/lib/persona-router/router.sema \
         (Some {bootstrap}) \
         1000 \
         (Some 0.0.0.0:7440) \
         router-a \
         (Some /run/criome/criome.sock) \
         {output})",
        bootstrap = bootstrap.display(),
        output = output.display(),
    );

    let status = Command::new(env!("CARGO_BIN_EXE_router-write-configuration"))
        .arg(&request)
        .status()
        .expect("run router-write-configuration");
    assert!(status.success(), "configuration writer exited with failure");

    let configuration =
        Configuration::from_binary_path(&output).expect("daemon reads back the configuration");

    let expected_listen: SocketAddr = "0.0.0.0:7440".parse().expect("listen address parses");
    assert_eq!(
        configuration.tailnet_listen_address(),
        Some(expected_listen)
    );
    assert_eq!(
        configuration.router_identity(),
        &signal_router::z2VNwn::new("router-a".to_owned()),
    );
    assert_eq!(
        configuration.criome_socket_path(),
        Some(Path::new("/run/criome/criome.sock")),
    );
    assert_eq!(configuration.bootstrap_path(), Some(bootstrap.as_path()));
}

#[test]
fn router_bootstrap_carries_hardwired_peers_and_actor_homes() {
    let directory = state_directory("router_bootstrap_text_edge");
    let output = directory.join("bootstrap.rkyv");
    let request = format!(
        "(BootstrapWriteRequest \
         {output} \
         [ (router-b 192.168.1.20:7440) ] \
         [ (mirror 0 (Some router-b) None) \
           (mirror 0 None (Some (ComponentSocket /run/mirror/working.sock))) \
           (owner 0 None None) ] \
         [ (owner mirror) ])",
        output = output.display(),
    );

    let status = Command::new(env!("CARGO_BIN_EXE_router-write-bootstrap"))
        .arg(&request)
        .status()
        .expect("run router-write-bootstrap");
    assert!(status.success(), "bootstrap writer exited with failure");

    let operations = RouterBootstrap::from_path(&output)
        .operations()
        .expect("daemon reads back the bootstrap document");

    let expected = vec![
        signal_router::z2VNh2::z2VdzM(signal_router::z2VWkj {
            field_0: signal_router::z2Vdz8::new(signal_router::z2VNwn::new("router-b".to_owned())),
            field_1: signal_router::z2VbwY::new(signal_router::z2VVPx::new(
                "192.168.1.20:7440".to_owned(),
            )),
        }),
        signal_router::z2VNh2::z2VQBJ(signal_router::z2VPdn {
            field_0: signal_router::z2VTFJ {
                field_0: signal_router::z2VQ8d::new(signal_router::z2VNMz::new(
                    "mirror".to_owned(),
                )),
                field_1: signal_router::z2VVdV::new(0),
                field_2: None,
            },
            field_1: Some(signal_router::z2VNwn::new("router-b".to_owned())),
        }),
        signal_router::z2VNh2::z2VQBJ(signal_router::z2VPdn {
            field_0: signal_router::z2VTFJ {
                field_0: signal_router::z2VQ8d::new(signal_router::z2VNMz::new(
                    "mirror".to_owned(),
                )),
                field_1: signal_router::z2VVdV::new(0),
                field_2: Some(signal_router::z2VLce {
                    field_0: signal_router::z2VZNB::new(signal_router::z2VaJt::z2VUHw),
                    field_1: signal_router::z2VXQX::new("/run/mirror/working.sock".to_owned()),
                    field_2: None,
                }),
            },
            field_1: None,
        }),
        signal_router::z2VNh2::z2VQBJ(signal_router::z2VPdn {
            field_0: signal_router::z2VTFJ {
                field_0: signal_router::z2VQ8d::new(signal_router::z2VNMz::new("owner".to_owned())),
                field_1: signal_router::z2VVdV::new(0),
                field_2: None,
            },
            field_1: None,
        }),
        signal_router::z2VNh2::z2VUXc(signal_router::z2VPkk {
            field_0: signal_router::z2VVbN::new(signal_router::z2VNMz::new("owner".to_owned())),
            field_1: signal_router::z2VVYB::new(signal_router::z2VNMz::new("mirror".to_owned())),
        }),
    ];
    assert_eq!(operations, expected);
}
