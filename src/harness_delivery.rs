use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::Context;
use kameo::reply::DelegatedReply;
use persona_harness::{
    HarnessId, HarnessTerminalBinding, HarnessTerminalDelivery as TerminalDelivery,
    HarnessTerminalEndpoint,
};
use persona_message::schema::{Actor, EndpointKind, EndpointTransport, Message};

use crate::{Error, Result};

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

    fn deliver(actor: &Actor, message: &Message) -> Result<bool> {
        let Some(endpoint) = &actor.endpoint else {
            return Ok(false);
        };
        let text = message.to_nota()?;
        let terminal = HarnessTerminalBinding::for_harness(HarnessId::new(actor.name.as_str()));
        let mut delivery = TerminalDelivery::new(Self::terminal_endpoint(endpoint)?);
        Ok(delivery.deliver_text(&terminal, &text)?.delivered())
    }

    fn terminal_endpoint(endpoint: &EndpointTransport) -> Result<HarnessTerminalEndpoint> {
        match endpoint.kind {
            EndpointKind::Human => Ok(HarnessTerminalEndpoint::Human),
            EndpointKind::PtySocket => Ok(HarnessTerminalEndpoint::PtySocket {
                path: endpoint.target.clone().into(),
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
                HarnessDelivery::deliver(&message.actor, &message.message)
            })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))
            .and_then(|result| result);
            HarnessDeliveryOutcome::from_result(result)
        })
    }
}
