use persona_router::{
    DeliveryGate, MessageBody, MessageId, PendingDelivery, PersonaMessage, PromptFact, RouterInput,
    RouterOutput,
};

#[test]
fn delivery_gate_defers_when_human_focuses_target() {
    let gate = DeliveryGate::new(true, true);

    assert!(!gate.decide().is_ready());
}

#[test]
fn pending_delivery_keeps_recipient() {
    let message = PersonaMessage::new(
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
