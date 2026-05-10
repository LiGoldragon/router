use persona_router::{
    DeliveryGate, Message, MessageBody, MessageId, PendingDelivery, PromptFact, RouterInput,
    RouterOutput,
};
use signal_persona_system::{
    FocusObservation, InputBufferObservation, InputBufferState, SystemTarget,
};

struct DeliveryGateFixture {
    target: SystemTarget,
}

impl DeliveryGateFixture {
    fn new() -> Self {
        Self {
            target: SystemTarget::niri_window(42),
        }
    }

    fn focus_observation(&self, focused: bool) -> FocusObservation {
        FocusObservation {
            target: self.target,
            focused,
            generation: 1,
        }
    }

    fn input_buffer_observation(&self, state: InputBufferState) -> InputBufferObservation {
        InputBufferObservation {
            target: self.target,
            state,
            generation: 1,
        }
    }

    fn gate_with(&self, focused: bool, input_buffer_state: InputBufferState) -> DeliveryGate {
        DeliveryGate::from_observations(
            Some(self.focus_observation(focused)),
            Some(self.input_buffer_observation(input_buffer_state)),
        )
    }

    fn mismatched_target_gate(&self) -> DeliveryGate {
        DeliveryGate::from_observations(
            Some(self.focus_observation(false)),
            Some(InputBufferObservation {
                target: SystemTarget::niri_window(777),
                state: InputBufferState::Empty,
                generation: 1,
            }),
        )
    }
}

#[test]
fn delivery_gate_defers_when_human_focuses_target() {
    let fixture = DeliveryGateFixture::new();
    let gate = fixture.gate_with(true, InputBufferState::Empty);

    assert!(!gate.decide().is_ready());
}

#[test]
fn delivery_gate_defers_when_input_buffer_is_occupied() {
    let fixture = DeliveryGateFixture::new();
    let gate = fixture.gate_with(false, InputBufferState::Occupied);

    assert!(!gate.decide().is_ready());
}

#[test]
fn delivery_gate_defers_when_input_buffer_is_unknown() {
    let fixture = DeliveryGateFixture::new();
    let gate = fixture.gate_with(false, InputBufferState::Unknown);

    assert!(!gate.decide().is_ready());
}

#[test]
fn delivery_gate_defers_when_observations_have_different_targets() {
    let fixture = DeliveryGateFixture::new();
    let gate = fixture.mismatched_target_gate();

    assert!(!gate.decide().is_ready());
}

#[test]
fn delivery_gate_allows_delivery_when_system_facts_are_clear() {
    let fixture = DeliveryGateFixture::new();
    let gate = fixture.gate_with(false, InputBufferState::Empty);

    assert!(gate.decide().is_ready());
}

#[test]
fn pending_delivery_keeps_recipient() {
    let message = Message::new(
        MessageId::new("m-abc"),
        "operator",
        "responder",
        MessageBody::new("hello"),
    );
    let delivery = PendingDelivery::new(message);

    assert_eq!(delivery.recipient(), "responder");
}

#[test]
fn router_input_decodes_prompt_observation() {
    let input =
        RouterInput::from_nota("(PromptObservation responder Empty)").expect("input decodes");

    assert!(matches!(
        input,
        RouterInput::PromptObservation(observation)
            if observation.actor.as_str() == "responder"
                && observation.state == PromptFact::Empty
    ));
}

#[test]
fn router_input_decodes_status_requester() {
    let input = RouterInput::from_nota("(Status operator)").expect("input decodes");

    assert!(matches!(
        input,
        RouterInput::Status(status) if status.requester.as_str() == "operator"
    ));
}

#[test]
fn router_output_encodes_delivery_changed() {
    let output = RouterOutput::DeliveryChanged(persona_router::DeliveryChanged {
        delivered: 1,
        pending: 0,
    });

    assert_eq!(
        output.to_nota().expect("output encodes"),
        "(DeliveryChanged 1 0)"
    );
}
