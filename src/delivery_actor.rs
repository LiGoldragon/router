use std::thread;
use std::time::Duration;

use kameo::actor::{Actor as KameoActor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message as KameoMessage};
use persona_message::schema::{Actor as PersonaActor, EndpointKind, Message};
use persona_wezterm::pty::PtySocket;
use persona_wezterm::terminal::{TerminalPrompt, WezTermMux};

use crate::{Error, Result};

#[derive(Debug)]
pub struct HarnessDeliveryActor {
    attempted_delivery_count: u64,
    delivered_message_count: u64,
}

impl HarnessDeliveryActor {
    pub fn new() -> Self {
        Self {
            attempted_delivery_count: 0,
            delivered_message_count: 0,
        }
    }

    fn deliver(&mut self, actor: &PersonaActor, message: &Message) -> Result<bool> {
        self.attempted_delivery_count = self.attempted_delivery_count.saturating_add(1);
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
        if delivered {
            self.delivered_message_count = self.delivered_message_count.saturating_add(1);
        }
        Ok(delivered)
    }
}

impl Default for HarnessDeliveryActor {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverHarnessMessage {
    pub actor: PersonaActor,
    pub message: Message,
}

#[derive(Debug, kameo::Reply)]
pub struct HarnessDeliveryReply {
    result: Result<bool>,
}

impl HarnessDeliveryReply {
    fn from_result(result: Result<bool>) -> Self {
        Self { result }
    }

    pub fn into_result(self) -> Result<bool> {
        self.result
    }
}

impl KameoActor for HarnessDeliveryActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        actor: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}

impl KameoMessage<DeliverHarnessMessage> for HarnessDeliveryActor {
    type Reply = HarnessDeliveryReply;

    async fn handle(
        &mut self,
        message: DeliverHarnessMessage,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        HarnessDeliveryReply::from_result(self.deliver(&message.actor, &message.message))
    }
}
