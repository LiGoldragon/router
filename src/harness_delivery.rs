use std::thread;
use std::time::Duration;

use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::Context;
use kameo::reply::DelegatedReply;
use persona_message::schema::{Actor, EndpointKind, Message};
use persona_wezterm::pty::PtySocket;
use persona_wezterm::terminal::{TerminalPrompt, WezTermMux};

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
        let prompt = TerminalPrompt::from_text(text.clone());
        let delivered = match endpoint.kind {
            EndpointKind::Human => true,
            EndpointKind::PtySocket => {
                let socket = PtySocket::from_path(&endpoint.target);
                socket.send_prompt(prompt.as_str())?;
                thread::sleep(Duration::from_millis(1000));
                let evidence = text.chars().take(24).collect::<String>();
                let capture = socket.capture()?.to_string_lossy();
                capture.contains(&evidence)
            }
            EndpointKind::WezTermPane => {
                let pane_id = endpoint
                    .target
                    .parse()
                    .map_err(|_| Error::DeliveryBlocked {
                        reason: format!("invalid wezterm pane id {:?}", endpoint.target),
                    })?;
                let mux = match &endpoint.aux {
                    Some(socket) => WezTermMux::from_environment().with_socket(socket),
                    None => WezTermMux::from_environment(),
                };
                mux.pane(pane_id).deliver(&prompt)?;
                true
            }
        };
        Ok(delivered)
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
