use persona_router::{DeliveryGate, MessageBody, MessageId, PendingDelivery, PersonaMessage};

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
