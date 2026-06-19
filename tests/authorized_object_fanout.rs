use router::{
    ActorIdentifier, AttendAuthorizedObjects, PublishAuthorizedObjectReference,
    ReadAuthorizedObjectFanoutStatus, RouterRuntime, WithdrawAuthorizedObjects,
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

struct StandardReference {
    inner: AuthorizedObjectReference,
}

impl StandardReference {
    fn into_inner(self) -> AuthorizedObjectReference {
        self.inner
    }
}

impl From<signal_criome::AuthorizedObjectReference> for StandardReference {
    fn from(reference: signal_criome::AuthorizedObjectReference) -> Self {
        Self {
            inner: AuthorizedObjectReference::new(
                StandardComponentKind::from(reference.component).into_inner(),
                ObjectDigest::new(reference.digest.as_str()),
                StandardAuthorizedObjectKind::from(reference.kind).into_inner(),
            ),
        }
    }
}

struct StandardComponentKind {
    inner: signal_standard::ComponentKind,
}

impl StandardComponentKind {
    fn into_inner(self) -> signal_standard::ComponentKind {
        self.inner
    }
}

impl From<signal_criome::ComponentKind> for StandardComponentKind {
    fn from(component: signal_criome::ComponentKind) -> Self {
        let inner = match component {
            signal_criome::ComponentKind::Spirit => signal_standard::ComponentKind::Spirit,
            signal_criome::ComponentKind::Criome => signal_standard::ComponentKind::Criome,
            signal_criome::ComponentKind::Router => signal_standard::ComponentKind::Router,
            signal_criome::ComponentKind::Mirror => signal_standard::ComponentKind::Mirror,
            signal_criome::ComponentKind::Lojix => signal_standard::ComponentKind::Lojix,
            signal_criome::ComponentKind::Persona => signal_standard::ComponentKind::Persona,
            signal_criome::ComponentKind::Agent => signal_standard::ComponentKind::Agent,
        };
        Self { inner }
    }
}

struct StandardAuthorizedObjectKind {
    inner: AuthorizedObjectKind,
}

impl StandardAuthorizedObjectKind {
    fn into_inner(self) -> AuthorizedObjectKind {
        self.inner
    }
}

impl From<signal_criome::AuthorizedObjectKind> for StandardAuthorizedObjectKind {
    fn from(kind: signal_criome::AuthorizedObjectKind) -> Self {
        let inner = match kind {
            signal_criome::AuthorizedObjectKind::Operation => AuthorizedObjectKind::Operation,
            signal_criome::AuthorizedObjectKind::Contract => AuthorizedObjectKind::Contract,
            signal_criome::AuthorizedObjectKind::Agreement => AuthorizedObjectKind::Agreement,
            signal_criome::AuthorizedObjectKind::Time => AuthorizedObjectKind::Time,
            signal_criome::AuthorizedObjectKind::Head => AuthorizedObjectKind::Head,
        };
        Self { inner }
    }
}
