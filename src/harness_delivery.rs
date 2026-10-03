use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::Context;
use kameo::reply::DelegatedReply;
use signal::{FrameCapacity, FrameReading, FrameWriting};
use signal_harness::{
    MessageDelivery, Query as HarnessRequest, Response as HarnessEvent, Restorable, Signal,
    Signalizable,
};
use signal_router::z2Vcrd as RoutedContractObject;
use triad_runtime::{FrameBody, LengthPrefixedCodec};

use crate::{Actor, EndpointKind, Error, Message, RouterResult};

#[derive(Debug)]
pub struct HarnessDelivery {
    attempted_delivery_count: u64,
    delegated_delivery_count: u64,
}

impl HarnessDelivery {
    pub fn new() -> Self {
        Self {
            attempted_delivery_count: 0,
            delegated_delivery_count: 0,
        }
    }

    fn deliver(
        actor: &Actor,
        message: &Message,
        message_slot: u64,
        routed_objects: &[RoutedContractObject],
    ) -> RouterResult<bool> {
        let Some(endpoint) = &actor.endpoint else {
            return Ok(false);
        };
        if endpoint.kind == EndpointKind::HarnessSocket {
            return Self::deliver_to_harness_socket(actor, message, message_slot, &endpoint.target);
        }
        match endpoint.kind {
            EndpointKind::Human => Ok(false),
            EndpointKind::HarnessSocket => Err(Error::UnexpectedSignalFrame {
                got: "harness socket endpoint cannot be treated as terminal transport".to_string(),
            }),
            EndpointKind::PtySocket => Self::deliver_to_terminal_socket(message, &endpoint.target),
            EndpointKind::ComponentSocket => {
                Self::deliver_to_component_socket(routed_objects, &endpoint.target)
            }
        }
    }

    fn deliver_to_terminal_socket(message: &Message, path: &str) -> RouterResult<bool> {
        let text = message.to_nota();
        let mut stream = UnixStream::connect(Path::new(path))?;
        stream.write_all(b"P")?;
        stream.write_all(&(text.len() as u64).to_be_bytes())?;
        stream.write_all(text.as_bytes())?;
        stream.flush()?;
        let mut acceptance = [0_u8; 1];
        stream.read_exact(&mut acceptance)?;
        Ok(acceptance[0] == b'A')
    }

    /// One `signal-harness` exchange: a plain Signal frame of the
    /// `MessageDelivery` query, answered by one frame of the harness
    /// `Response`. Delivered only when the harness reports this actor's
    /// delivery completed.
    fn deliver_to_harness_socket(
        actor: &Actor,
        message: &Message,
        message_slot: u64,
        path: &str,
    ) -> RouterResult<bool> {
        let message_slot =
            i64::try_from(message_slot).map_err(|_| Error::UnexpectedSignalFrame {
                got: format!("message slot {message_slot} exceeds the harness slot range"),
            })?;
        let request = HarnessRequest::MessageDelivery(MessageDelivery {
            harness_name: actor.name.as_str().to_owned(),
            message_sender: message.from.as_str().to_owned(),
            message_body: message.body.clone(),
            message_slot,
        });
        let signal = request
            .signalize()
            .map_err(|error| Error::UnexpectedSignalFrame {
                got: format!("harness request did not archive: {error}"),
            })?;
        let mut stream = UnixStream::connect(Path::new(path))?;
        stream
            .write_frame(&signal, FrameCapacity::default())
            .map_err(Self::frame_failure)?;
        match Self::read_harness_event(&mut stream)? {
            HarnessEvent::DeliveryCompleted(event) => {
                Ok(event.harness_name == actor.name.as_str() && event.message_slot == message_slot)
            }
            HarnessEvent::DeliveryFailed(_) => Ok(false),
            _ => Ok(false),
        }
    }

    fn read_harness_event(stream: &mut impl Read) -> RouterResult<HarnessEvent> {
        let body = stream
            .read_frame(FrameCapacity::default())
            .map_err(Self::frame_failure)?;
        Signal::<HarnessEvent>::from(Vec::from(body))
            .restore()
            .map_err(|error| Error::UnexpectedSignalFrame {
                got: format!("harness reply did not restore: {error}"),
            })
    }

    fn frame_failure(error: signal::FrameError) -> Error {
        Error::UnexpectedSignalFrame {
            got: format!("harness signal frame: {error}"),
        }
    }

    fn deliver_to_component_socket(
        routed_objects: &[RoutedContractObject],
        path: &str,
    ) -> RouterResult<bool> {
        if routed_objects.is_empty() {
            return Ok(false);
        }
        for object in routed_objects {
            let octets = Self::object_payload_octets(object)?;
            let mut stream = UnixStream::connect(Path::new(path))?;
            let codec = LengthPrefixedCodec::default();
            codec.write_body(&mut stream, &FrameBody::new(octets))?;
            stream.flush()?;
            let _reply = codec.read_body(&mut stream)?;
        }
        Ok(true)
    }

    fn object_payload_octets(object: &RoutedContractObject) -> RouterResult<Vec<u8>> {
        let declared = *object.field_2.payload();
        let actual = object.field_3.len() as u64;
        if declared != actual {
            return Err(Error::UnexpectedSignalFrame {
                got: format!("routed object declared {declared} octets but carried {actual}"),
            });
        }
        object
            .field_3
            .iter()
            .map(|octet| {
                u8::try_from(*octet).map_err(|_| Error::UnexpectedSignalFrame {
                    got: format!("routed object octet {octet} is outside 0..=255"),
                })
            })
            .collect()
    }
}

impl Default for HarnessDelivery {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverHarness {
    pub actor: Actor,
    pub message: Message,
    pub message_slot: u64,
    pub routed_objects: Vec<RoutedContractObject>,
}

#[derive(Debug, kameo::Reply)]
pub struct HarnessDeliveryOutcome {
    result: RouterResult<bool>,
}

impl HarnessDeliveryOutcome {
    fn from_result(result: RouterResult<bool>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> RouterResult<bool> {
        self.result
    }
}

impl kameo::actor::Actor for HarnessDelivery {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        actor: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}

impl kameo::message::Message<DeliverHarness> for HarnessDelivery {
    type Reply = DelegatedReply<HarnessDeliveryOutcome>;

    async fn handle(
        &mut self,
        message: DeliverHarness,
        context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.attempted_delivery_count = self.attempted_delivery_count.saturating_add(1);
        self.delegated_delivery_count = self.delegated_delivery_count.saturating_add(1);
        context.spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                HarnessDelivery::deliver(
                    &message.actor,
                    &message.message,
                    message.message_slot,
                    &message.routed_objects,
                )
            })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))
            .and_then(|result| result);
            HarnessDeliveryOutcome::from_result(result)
        })
    }
}
