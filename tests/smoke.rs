use std::io::Write;

use persona_router::{
    Message, MessageBody, MessageId, PendingDelivery, RouterConnection, RouterInput, RouterOutput,
};
use signal_core::{FrameBody, Request};
use signal_persona::TimestampNanos;
use signal_persona_auth::{ComponentName, MessageOrigin};
use signal_persona_message::{
    Frame, MessageBody as SignalMessageBody, MessageKind, MessageRecipient, MessageRequest,
    MessageSubmission, StampedMessageSubmission,
};

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

#[test]
fn router_connection_decodes_signal_persona_message_frame() {
    let (mut client, server) = std::os::unix::net::UnixStream::pair().expect("socket pair");
    let request = MessageRequest::StampedMessageSubmission(StampedMessageSubmission {
        submission: MessageSubmission {
            recipient: MessageRecipient::new("responder"),
            kind: MessageKind::Send,
            body: SignalMessageBody::new("socket frame"),
        },
        origin: MessageOrigin::Internal(ComponentName::Message),
        stamped_at: TimestampNanos::new(1),
    });
    let frame = Frame::new(FrameBody::Request(Request::assert(request)));
    client
        .write_all(
            frame
                .encode_length_prefixed()
                .expect("signal frame encodes")
                .as_slice(),
        )
        .expect("client writes frame");
    let mut connection = RouterConnection::from_stream(server);

    let input = connection
        .read_signal_input()
        .expect("router reads signal input");

    assert_eq!(input.sender().as_str(), "message");
    assert_eq!(
        input.origin(),
        &MessageOrigin::Internal(ComponentName::Message)
    );
    assert!(matches!(
        input.request(),
        MessageRequest::StampedMessageSubmission(stamped)
            if stamped.submission.recipient.as_str() == "responder"
                && stamped.submission.kind == MessageKind::Send
                && stamped.submission.body.as_str() == "socket frame"
                && stamped.origin == MessageOrigin::Internal(ComponentName::Message)
    ));
}
