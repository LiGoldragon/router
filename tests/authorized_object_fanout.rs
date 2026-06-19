use router::{
    ActorIdentifier, AttendAuthorizedObjects, PublishAuthorizedObjectReference,
    ReadAuthorizedObjectFanoutStatus, RouterRuntime, StandardReference, WithdrawAuthorizedObjects,
};
use signal_standard::{
    AuthorizedObjectInterest, AuthorizedObjectKind, AuthorizedObjectReference,
    ComponentObjectInterest, ObjectDigest,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authorized_object_fanout_delivers_reference_only_updates_to_matching_subscribers() {
    let router = RouterRuntime::start().await;
    let mentci_interest =
        AuthorizedObjectInterest::Component(signal_standard::ComponentKind::Criome);
    let mirror_interest =
        AuthorizedObjectInterest::Component(signal_standard::ComponentKind::Mirror);

    let mentci = router
        .ask(AttendAuthorizedObjects {
            subscriber: ActorIdentifier::new("mentci-local"),
            interest: mentci_interest,
        })
        .await
        .expect("router actor accepts attend");
    let _mirror = router
        .ask(AttendAuthorizedObjects {
            subscriber: ActorIdentifier::new("mirror-local"),
            interest: mirror_interest,
        })
        .await
        .expect("router actor accepts attend");

    let reference = AuthorizedObjectReference::new(
        signal_standard::ComponentKind::Criome,
        ObjectDigest::new("criome-authorized-object-1"),
        AuthorizedObjectKind::Operation,
    );
    let publication = router
        .ask(PublishAuthorizedObjectReference {
            reference: reference.clone(),
        })
        .await
        .expect("router actor accepts publish");

    assert_eq!(publication.deliveries.len(), 1);
    assert_eq!(
        publication.deliveries[0].subscriber,
        ActorIdentifier::new("mentci-local")
    );
    assert_eq!(publication.deliveries[0].reference, reference);

    let status = router
        .ask(ReadAuthorizedObjectFanoutStatus {
            requester: ActorIdentifier::new("operator"),
        })
        .await
        .expect("router actor accepts status read");
    assert_eq!(status.subscription_count, 2);
    assert_eq!(status.update_count, 1);
    assert_eq!(status.delivery_count, 1);

    let withdrawn = router
        .ask(WithdrawAuthorizedObjects {
            token: mentci.token,
        })
        .await
        .expect("router actor accepts withdraw");
    assert!(withdrawn.retracted);

    let second_reference = AuthorizedObjectReference::new(
        signal_standard::ComponentKind::Criome,
        ObjectDigest::new("criome-authorized-object-2"),
        AuthorizedObjectKind::Operation,
    );
    let second_publication = router
        .ask(PublishAuthorizedObjectReference {
            reference: second_reference,
        })
        .await
        .expect("router actor accepts second publish");

    assert!(second_publication.deliveries.is_empty());
    router
        .stop_gracefully()
        .await
        .expect("router runtime stops gracefully");
    router.wait_for_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authorized_object_fanout_returns_matching_snapshot_on_late_attend() {
    let router = RouterRuntime::start().await;
    let time_reference = AuthorizedObjectReference::new(
        signal_standard::ComponentKind::Criome,
        ObjectDigest::new("time-object"),
        AuthorizedObjectKind::Time,
    );
    let operation_reference = AuthorizedObjectReference::new(
        signal_standard::ComponentKind::Criome,
        ObjectDigest::new("operation-object"),
        AuthorizedObjectKind::Operation,
    );

    for reference in [time_reference.clone(), operation_reference] {
        router
            .ask(PublishAuthorizedObjectReference { reference })
            .await
            .expect("router actor accepts pre-subscription publish");
    }

    let snapshot = router
        .ask(AttendAuthorizedObjects {
            subscriber: ActorIdentifier::new("clock-viewer"),
            interest: AuthorizedObjectInterest::ObjectKind(AuthorizedObjectKind::Time),
        })
        .await
        .expect("router actor accepts late attend");

    assert_eq!(snapshot.references, vec![time_reference]);
    router
        .stop_gracefully()
        .await
        .expect("router runtime stops gracefully");
    router.wait_for_shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn criome_reference_projects_to_router_reference_only_pulse() {
    let router = RouterRuntime::start().await;
    let subscriber = router
        .ask(AttendAuthorizedObjects {
            subscriber: ActorIdentifier::new("spirit-replica"),
            interest: AuthorizedObjectInterest::ComponentObject(ComponentObjectInterest::new(
                signal_standard::ComponentKind::Spirit,
                AuthorizedObjectKind::Head,
            )),
        })
        .await
        .expect("router actor accepts spirit attendance");
    assert!(subscriber.references.is_empty());

    let criome_reference = signal_criome::AuthorizedObjectReference {
        component: signal_criome::ComponentKind::Spirit,
        digest: signal_criome::ObjectDigest::new("spirit-head-digest"),
        kind: signal_criome::AuthorizedObjectKind::Head,
    };
    let publication = router
        .ask(PublishAuthorizedObjectReference {
            reference: StandardReference::from(criome_reference).into_inner(),
        })
        .await
        .expect("router actor accepts projected criome reference");

    assert_eq!(publication.deliveries.len(), 1);
    assert_eq!(
        publication.deliveries[0].subscriber,
        ActorIdentifier::new("spirit-replica")
    );
    assert_eq!(
        publication.deliveries[0].reference.digest.as_str(),
        "spirit-head-digest"
    );
    assert_eq!(
        publication.deliveries[0].reference.kind,
        AuthorizedObjectKind::Head
    );
    router
        .stop_gracefully()
        .await
        .expect("router runtime stops gracefully");
    router.wait_for_shutdown().await;
}
