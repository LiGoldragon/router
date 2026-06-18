//! The actor-harness delivery witness: a standalone Unix-socket listener that
//! stands in for a real receiving actor process so a deploy / test surface can
//! prove a forward was *delivered*, not merely accepted.
//!
//! It is the cross-VM twin of the in-process `HarnessWitness` the milestone-2
//! loopback test (`tests/end_to_end_remote_forward.rs`) uses: it binds the same
//! `EndpointKind::HarnessSocket` Unix socket the router's `HarnessDelivery`
//! connects to (`src/harness_delivery.rs`), decodes the inbound
//! `signal-harness` `MessageDelivery` frame, reports the witnessed delivery as
//! one NOTA line on stdout, and replies `DeliveryCompleted` so the router's
//! delivery path closes the exchange.
//!
//! In the two-kernel `runNixOSTest` (report 136 rung L1) it runs on the
//! RECEIVER guest, registered as `prometheus-responder`'s endpoint, so the test
//! asserts the forward crossed two kernels AND landed in the receiving actor
//! harness — delivery, not just receipt. It takes ONE NOTA argument and emits
//! the witnessed deliveries as NOTA. No flags.

use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;

use nota_next::{Delimiter, NotaBlock, NotaDecode, NotaDecodeError, NotaEncode, NotaSource};
use signal_frame::{NonEmpty, Reply, SubReply};
use signal_harness::{
    DeliveryCompleted, HarnessEvent, HarnessFrame, HarnessFrameBody, HarnessName, HarnessRequest,
};
use thiserror::Error;
use triad_runtime::{ArgumentError, ComponentArgument, ComponentCommand};

/// `(RouterHarnessWitness <socket-path> <delivery-count>)`: bind `socket-path`,
/// accept exactly `delivery-count` actor-harness deliveries, print each as a
/// `(WitnessedDelivery <harness> <sender> <body>)` NOTA line, and reply
/// `DeliveryCompleted` to each.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterHarnessWitness {
    socket_path: PathBuf,
    delivery_count: u32,
}

impl RouterHarnessWitness {
    pub fn from_environment() -> Result<Self, HarnessWitnessError> {
        let command = ComponentCommand::from_environment();
        let text = match command.nota_argument()? {
            ComponentArgument::InlineNota(argument) => argument.into_string(),
            ComponentArgument::NotaFile(file) => {
                let path = file.into_path();
                std::fs::read_to_string(&path)
                    .map_err(|source| HarnessWitnessError::ReadNotaFile { path, source })?
            }
            ComponentArgument::SignalFile(file) => {
                return Err(HarnessWitnessError::SignalInput {
                    path: file.into_path(),
                });
            }
        };
        Ok(NotaSource::new(&text).parse()?)
    }

    /// Bind the socket, then accept and witness exactly `delivery_count`
    /// deliveries. Each witnessed delivery is printed as one NOTA line; the
    /// caller (test driver) reads stdout to assert delivery actually happened.
    pub fn run(&self) -> Result<(), HarnessWitnessError> {
        if self.socket_path.exists() {
            std::fs::remove_file(&self.socket_path).map_err(|source| {
                HarnessWitnessError::BindSocket {
                    path: self.socket_path.clone(),
                    source,
                }
            })?;
        }
        let listener = UnixListener::bind(&self.socket_path).map_err(|source| {
            HarnessWitnessError::BindSocket {
                path: self.socket_path.clone(),
                source,
            }
        })?;
        for _ in 0..self.delivery_count {
            let (stream, _) = listener
                .accept()
                .map_err(|source| HarnessWitnessError::Accept { source })?;
            let delivery = self.witness_one(stream)?;
            println!("{}", delivery.to_nota());
            std::io::stdout()
                .flush()
                .map_err(|source| HarnessWitnessError::Accept { source })?;
        }
        Ok(())
    }

    /// Decode one inbound `MessageDelivery` frame, reply `DeliveryCompleted`,
    /// and return the witnessed fields.
    fn witness_one(
        &self,
        mut stream: UnixStream,
    ) -> Result<WitnessedDelivery, HarnessWitnessError> {
        let frame = Self::read_harness_frame(&mut stream)?;
        let HarnessFrameBody::Request { exchange, request } = frame.into_body() else {
            return Err(HarnessWitnessError::UnexpectedFrame {
                detail: "expected a harness request frame".to_string(),
            });
        };
        let HarnessRequest::MessageDelivery(delivery) = request.payloads().head().clone() else {
            return Err(HarnessWitnessError::UnexpectedFrame {
                detail: "expected a message-delivery request".to_string(),
            });
        };
        let witnessed = WitnessedDelivery {
            harness: delivery.harness.as_str().to_string(),
            sender: delivery.sender.as_str().to_string(),
            body: delivery.body.as_str().to_string(),
        };
        let reply = HarnessFrame::new(HarnessFrameBody::Reply {
            exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(
                HarnessEvent::DeliveryCompleted(DeliveryCompleted {
                    harness: HarnessName::new(delivery.harness.as_str()),
                    message_slot: delivery.message_slot,
                }),
            ))),
        });
        let encoded = reply
            .encode_length_prefixed()
            .map_err(|source| HarnessWitnessError::EncodeReply { source })?;
        stream
            .write_all(encoded.as_slice())
            .map_err(|source| HarnessWitnessError::WriteReply { source })?;
        stream
            .flush()
            .map_err(|source| HarnessWitnessError::WriteReply { source })?;
        Ok(witnessed)
    }

    fn read_harness_frame(stream: &mut impl Read) -> Result<HarnessFrame, HarnessWitnessError> {
        let mut prefix = [0_u8; 4];
        stream
            .read_exact(&mut prefix)
            .map_err(|source| HarnessWitnessError::ReadFrame { source })?;
        let length = u32::from_be_bytes(prefix) as usize;
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        stream
            .read_exact(&mut bytes[4..])
            .map_err(|source| HarnessWitnessError::ReadFrame { source })?;
        HarnessFrame::decode_length_prefixed(bytes.as_slice())
            .map_err(|source| HarnessWitnessError::DecodeFrame { source })
    }
}

impl NotaDecode for RouterHarnessWitness {
    fn from_nota_block(block: &nota_next::Block) -> Result<Self, NotaDecodeError> {
        let fields = NotaBlock::new(block).expect_children(
            Delimiter::Parenthesis,
            "RouterHarnessWitness",
            3,
        )?;
        match fields[0].demote_to_string() {
            Some("RouterHarnessWitness") => {}
            Some(variant) => {
                return Err(NotaDecodeError::UnknownVariant {
                    enum_name: "RouterHarnessWitness",
                    variant: variant.to_owned(),
                });
            }
            None => {
                return Err(NotaDecodeError::ExpectedAtom {
                    type_name: "RouterHarnessWitness",
                });
            }
        }
        let socket_path = fields[1]
            .demote_to_string()
            .ok_or(NotaDecodeError::ExpectedAtom {
                type_name: "RouterHarnessWitness socket path",
            })?
            .into();
        let delivery_count = NotaBlock::new(&fields[2]).parse_integer()?;
        let delivery_count =
            u32::try_from(delivery_count).map_err(|_| NotaDecodeError::InvalidInteger {
                value: delivery_count.to_string(),
            })?;
        Ok(Self {
            socket_path,
            delivery_count,
        })
    }
}

/// One witnessed actor-harness delivery, rendered as a stable NOTA line the
/// test driver asserts against: `(WitnessedDelivery <harness> <sender> <body>)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WitnessedDelivery {
    harness: String,
    sender: String,
    body: String,
}

impl NotaEncode for WitnessedDelivery {
    fn to_nota(&self) -> String {
        Delimiter::Parenthesis.wrap([
            "WitnessedDelivery".to_string(),
            self.harness.to_nota(),
            self.sender.to_nota(),
            self.body.to_nota(),
        ])
    }
}

#[derive(Debug, Error)]
pub enum HarnessWitnessError {
    #[error("witness argument error: {0}")]
    Argument(#[from] ArgumentError),

    #[error("decode witness NOTA request: {0}")]
    Decode(#[from] NotaDecodeError),

    #[error("read witness NOTA request file {path}: {source}")]
    ReadNotaFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("witness does not accept a signal-file argument: {path}")]
    SignalInput { path: PathBuf },

    #[error("bind witness socket {path}: {source}")]
    BindSocket {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("accept witness connection: {source}")]
    Accept { source: std::io::Error },

    #[error("read witness harness frame: {source}")]
    ReadFrame { source: std::io::Error },

    #[error("decode witness harness frame: {source}")]
    DecodeFrame { source: signal_frame::FrameError },

    #[error("encode witness harness reply: {source}")]
    EncodeReply { source: signal_frame::FrameError },

    #[error("write witness harness reply: {source}")]
    WriteReply { source: std::io::Error },

    #[error("unexpected witness harness frame: {detail}")]
    UnexpectedFrame { detail: String },
}
