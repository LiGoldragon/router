use signal_standard::{
    AuthorizedObjectKind, AuthorizedObjectReference, ComponentKind, ObjectDigest,
};

pub struct StandardReference {
    inner: AuthorizedObjectReference,
}

impl StandardReference {
    pub fn into_inner(self) -> AuthorizedObjectReference {
        self.inner
    }
}

impl From<signal_criome::AuthorizedObjectReference> for StandardReference {
    fn from(reference: signal_criome::AuthorizedObjectReference) -> Self {
        Self {
            inner: AuthorizedObjectReference::new(
                StandardComponentKind::from(reference.component_kind).into_inner(),
                ObjectDigest::new(reference.object_digest.as_str()),
                StandardAuthorizedObjectKind::from(reference.authorized_object_kind).into_inner(),
            ),
        }
    }
}

struct StandardComponentKind {
    inner: ComponentKind,
}

impl StandardComponentKind {
    fn into_inner(self) -> ComponentKind {
        self.inner
    }
}

impl From<signal_criome::ComponentKind> for StandardComponentKind {
    fn from(component: signal_criome::ComponentKind) -> Self {
        let inner = match component {
            signal_criome::ComponentKind::Spirit => ComponentKind::Spirit,
            signal_criome::ComponentKind::Criome => ComponentKind::Criome,
            signal_criome::ComponentKind::Router => ComponentKind::Router,
            signal_criome::ComponentKind::Mirror => ComponentKind::Mirror,
            signal_criome::ComponentKind::Lojix => ComponentKind::Lojix,
            signal_criome::ComponentKind::Persona => ComponentKind::Persona,
            signal_criome::ComponentKind::Agent => ComponentKind::Agent,
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
