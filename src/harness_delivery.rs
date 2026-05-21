use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::Context;
use kameo::reply::DelegatedReply;
use signal_core::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, Request, SessionEpoch, SubReply,
};
use signal_persona_harness::{
    HarnessEvent, HarnessFrame, HarnessFrameBody, HarnessName, HarnessRequest, MessageBody,
    MessageDelivery, MessageSender, MessageSlot,
};

use crate::{Actor, EndpointKind, Error, Message, Result};

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

    fn deliver(actor: &Actor, message: &Message, message_slot: u64) -> Result<bool> {
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
        }
    }

    fn deliver_to_terminal_socket(message: &Message, path: &str) -> Result<bool> {
        let text = message.to_nota()?;
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
    ) -> Result<bool> {
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

    fn read_harness_event(stream: &mut impl Read) -> Result<HarnessEvent> {
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
                    SubReply::Ok { payload, .. } => Ok(payload),
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
}

#[derive(Debug, kameo::Reply)]
pub struct HarnessDeliveryOutcome {
    result: Result<bool>,
}

impl HarnessDeliveryOutcome {
    fn from_result(result: Result<bool>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> Result<bool> {
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
                HarnessDelivery::deliver(&message.actor, &message.message, message.message_slot)
            })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))
            .and_then(|result| result);
            HarnessDeliveryOutcome::from_result(result)
        })
    }
}
