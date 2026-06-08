use router::{
    RouterProcessDaemon,
    schema::{daemon, nexus, sema, signal},
};

#[test]
fn generated_router_planes_expose_signal_nexus_and_sema_nouns() {
    let ingress = signal::MessageIngress {
        recipient: "terminal".to_owned(),
        body: "deliver this".to_owned(),
        message_kind: signal::MessageKind::DirectMessage,
    };

    let signal_input = signal::Input::accept_message(ingress.clone());
    let nexus_work = nexus::NexusWork::signal_arrived(signal_input);
    assert!(matches!(nexus_work, nexus::NexusWork::SignalArrived(_)));

    let sema_write = sema::WriteInput::record_accepted_message(ingress.clone());
    let nexus_write = nexus::NexusAction::command_sema_write(sema_write);
    assert!(matches!(
        nexus_write,
        nexus::NexusAction::CommandSemaWrite(_)
    ));

    let delivery = nexus::DeliveryCommand::attempt_delivery(ingress);
    let effect = nexus::NexusEffectCommand::deliver_to_harness(delivery);
    let nexus_effect = nexus::NexusAction::command_effect(effect);
    assert!(matches!(nexus_effect, nexus::NexusAction::CommandEffect(_)));

    let committed = sema::WriteOutput::committed(1);
    let completed = nexus::NexusWork::sema_write_completed(committed);
    assert!(matches!(completed, nexus::NexusWork::SemaWriteCompleted(_)));
}

#[test]
fn generated_router_daemon_surface_is_part_of_the_schema_stack() {
    assert_eq!(daemon::ListenerTier::Working.to_string(), "working");
    assert_eq!(daemon::ListenerTier::Meta.to_string(), "meta");
    assert_daemon_entry::<RouterProcessDaemon>();
}

fn assert_daemon_entry<Daemon: daemon::DaemonEntry>() {}
