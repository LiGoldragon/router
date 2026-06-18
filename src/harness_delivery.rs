use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::Context;
use kameo::reply::DelegatedReply;
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, Request, SessionEpoch, SubReply,
};
use signal_harness::{
    HarnessEvent, HarnessFrame, HarnessFrameBody, HarnessName, HarnessRequest, MessageBody,
    MessageDelivery, MessageSender, MessageSlot,
};
use signal_router::{
    EndpointKind as SignalRouterEndpointKind, EndpointTransport as SignalRouterEndpointTransport,
    ObjectAvailable, Output as SignalRouterOutput, RoutedContractObject,
};
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

    fn deliver_to_harness_socket(
        actor: &Actor,
        message: &Message,
        message_slot: u64,
        path: &str,
    ) -> RouterResult<bool> {
        let mut stream = UnixStream::connect(Path::new(path))?;
        let request = HarnessRequest::MessageDelivery(MessageDelivery {
            harness: HarnessName::new(actor.name.as_str()),
            sender: MessageSender::new(message.from.as_str()),
            body: MessageBody::new(message.body.as_str()),
            message_slot: MessageSlot::new(message_slot),
        });
        let exchange = ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::first(),
        );
        let frame = HarnessFrame::new(HarnessFrameBody::Request {
            exchange,
            request: Request::from_payload(request),
        });
        stream.write_all(frame.encode_length_prefixed()?.as_slice())?;
        stream.flush()?;
        match Self::read_harness_event(&mut stream)? {
            HarnessEvent::DeliveryCompleted(event) => {
                Ok(event.harness.as_str() == actor.name.as_str())
            }
            HarnessEvent::DeliveryFailed(_) => Ok(false),
            _ => Ok(false),
        }
    }

    fn read_harness_event(stream: &mut impl Read) -> RouterResult<HarnessEvent> {
        let mut prefix = [0_u8; 4];
        stream.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        stream.read_exact(&mut bytes[4..])?;
        match HarnessFrame::decode_length_prefixed(bytes.as_slice())?.into_body() {
            HarnessFrameBody::Reply { reply, .. } => match reply {
                Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok(payload) => Ok(payload),
                    other => Err(Error::UnexpectedSignalFrame {
                        got: format!("unexpected harness sub-reply: {other:?}"),
                    }),
                },
                Reply::Rejected { reason } => Err(Error::UnexpectedSignalFrame {
                    got: format!("harness delivery rejected: {reason:?}"),
                }),
            },
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("unexpected harness frame: {other:?}"),
            }),
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
            Self::write_component_socket_body(path, octets)?;
        }
        Ok(true)
    }

    /// The generalized component-socket writer: connect the endpoint's Unix
    /// socket, write one `LengthPrefixedCodec` `FrameBody` of the given octets,
    /// and read the component's reply body. The routed-object delivery and the
    /// attendance fan-out (`Output::ObjectAvailable`) share this verbatim —
    /// only the body bytes differ.
    fn write_component_socket_body(path: &str, octets: Vec<u8>) -> RouterResult<bool> {
        let mut stream = UnixStream::connect(Path::new(path))?;
        let codec = LengthPrefixedCodec::default();
        codec.write_body(&mut stream, &FrameBody::new(octets))?;
        stream.flush()?;
        let _reply = codec.read_body(&mut stream)?;
        Ok(true)
    }

    /// Push one reference (`Output::ObjectAvailable`) to an attender's
    /// ComponentSocket. The body is the rkyv-encoded signal-router `Output` the
    /// attender's ComponentSocket reader decodes — the same length-prefixed
    /// shape `write_component_socket_body` writes for routed objects. Carries
    /// the REFERENCE octets, never the object payload (m0p2).
    fn push_object_available(
        endpoint: &SignalRouterEndpointTransport,
        push: &ObjectAvailable,
    ) -> RouterResult<bool> {
        if endpoint.kind != SignalRouterEndpointKind::ComponentSocket {
            return Ok(false);
        }
        let body = SignalRouterOutput::ObjectAvailable(push.clone()).encode_signal_frame()?;
        Self::write_component_socket_body(endpoint.target.as_str(), body)
    }

    fn object_payload_octets(object: &RoutedContractObject) -> RouterResult<Vec<u8>> {
        let declared = *object.contract_payload_size.payload();
        let actual = object.payload_octets().len() as u64;
        if declared != actual {
            return Err(Error::UnexpectedSignalFrame {
                got: format!("routed object declared {declared} octets but carried {actual}"),
            });
        }
        object
            .payload_octets()
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

/// The attendance fan-out push: one `Output::ObjectAvailable` reference written
/// to an attender's ComponentSocket. Reuses the existing component-socket writer
/// the routed-object delivery uses; only the body bytes change (the rkyv
/// `Output` instead of routed-object octets).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverComponentReference {
    pub endpoint: SignalRouterEndpointTransport,
    pub push: ObjectAvailable,
}

impl DeliverComponentReference {
    pub fn new(endpoint: SignalRouterEndpointTransport, push: ObjectAvailable) -> Self {
        Self { endpoint, push }
    }
}

impl kameo::message::Message<DeliverComponentReference> for HarnessDelivery {
    type Reply = DelegatedReply<HarnessDeliveryOutcome>;

    async fn handle(
        &mut self,
        message: DeliverComponentReference,
        context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.attempted_delivery_count = self.attempted_delivery_count.saturating_add(1);
        self.delegated_delivery_count = self.delegated_delivery_count.saturating_add(1);
        context.spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                HarnessDelivery::push_object_available(&message.endpoint, &message.push)
            })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))
            .and_then(|result| result);
            HarnessDeliveryOutcome::from_result(result)
        })
    }
}
