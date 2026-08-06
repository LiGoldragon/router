use signal_standard::{
    z2VSyM as ObjectDigest, z2VTjK as AuthorizedObjectReference, z2VWWD as ComponentKind,
    z2Vbhy as AuthorizedObjectKind,
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
                ObjectDigest::new(reference.object_digest.as_str().to_owned()),
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
            signal_criome::ComponentKind::Spirit => ComponentKind::z2VPuL,
            signal_criome::ComponentKind::Criome => ComponentKind::z2VSDw,
            signal_criome::ComponentKind::Router => ComponentKind::z2VZ4y,
            signal_criome::ComponentKind::Mirror => ComponentKind::z2VVh8,
            signal_criome::ComponentKind::Lojix => ComponentKind::z2VN8F,
            signal_criome::ComponentKind::Persona => ComponentKind::z2Vc9t,
            signal_criome::ComponentKind::Agent => ComponentKind::z2VNYL,
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
            signal_criome::AuthorizedObjectKind::Operation => AuthorizedObjectKind::z2VPDv,
            signal_criome::AuthorizedObjectKind::Contract => AuthorizedObjectKind::z2Ve6d,
            signal_criome::AuthorizedObjectKind::Agreement => AuthorizedObjectKind::z2VV79,
            signal_criome::AuthorizedObjectKind::Time => AuthorizedObjectKind::z2VYDX,
            signal_criome::AuthorizedObjectKind::Head => AuthorizedObjectKind::z2Vd4Q,
        };
        Self { inner }
    }
}
