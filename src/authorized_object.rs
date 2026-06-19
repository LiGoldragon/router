use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_standard::{AuthorizedObjectInterest, AuthorizedObjectReference};

use crate::ActorIdentifier;

#[derive(Debug)]
pub struct AuthorizedObjectFanout {
    subscriptions: Vec<AuthorizedObjectAttendanceToken>,
    updates: Vec<AuthorizedObjectReference>,
    deliveries: Vec<AuthorizedObjectDelivery>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttendAuthorizedObjects {
    pub subscriber: ActorIdentifier,
    pub interest: AuthorizedObjectInterest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WithdrawAuthorizedObjects {
    pub token: AuthorizedObjectAttendanceToken,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishAuthorizedObjectReference {
    pub reference: AuthorizedObjectReference,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadAuthorizedObjectFanoutStatus {
    pub requester: ActorIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedObjectAttendanceToken {
    pub subscriber: ActorIdentifier,
    pub interest: AuthorizedObjectInterest,
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct AuthorizedObjectAttendanceSnapshot {
    pub token: AuthorizedObjectAttendanceToken,
    pub references: Vec<AuthorizedObjectReference>,
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct AuthorizedObjectAttendanceWithdrawn {
    pub token: AuthorizedObjectAttendanceToken,
    pub retracted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedObjectDelivery {
    pub subscriber: ActorIdentifier,
    pub reference: AuthorizedObjectReference,
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct AuthorizedObjectPublication {
    pub deliveries: Vec<AuthorizedObjectDelivery>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, kameo::Reply)]
pub struct AuthorizedObjectFanoutStatus {
    pub subscription_count: u64,
    pub update_count: u64,
    pub delivery_count: u64,
}

impl AuthorizedObjectFanout {
    pub fn new() -> Self {
        Self {
            subscriptions: Vec::new(),
            updates: Vec::new(),
            deliveries: Vec::new(),
        }
    }

    fn attend(
        &mut self,
        attendance: AttendAuthorizedObjects,
    ) -> AuthorizedObjectAttendanceSnapshot {
        let token = AuthorizedObjectAttendanceToken {
            subscriber: attendance.subscriber,
            interest: attendance.interest,
        };
        if !self.subscriptions.contains(&token) {
            self.subscriptions.push(token.clone());
        }
        let references = self
            .updates
            .iter()
            .filter(|reference| reference.matches_interest(&token.interest))
            .cloned()
            .collect();
        AuthorizedObjectAttendanceSnapshot { token, references }
    }

    fn withdraw(
        &mut self,
        withdrawal: WithdrawAuthorizedObjects,
    ) -> AuthorizedObjectAttendanceWithdrawn {
        let retracted = match self
            .subscriptions
            .iter()
            .position(|token| token == &withdrawal.token)
        {
            Some(index) => {
                self.subscriptions.remove(index);
                true
            }
            None => false,
        };
        AuthorizedObjectAttendanceWithdrawn {
            token: withdrawal.token,
            retracted,
        }
    }

    fn publish(
        &mut self,
        publication: PublishAuthorizedObjectReference,
    ) -> AuthorizedObjectPublication {
        let deliveries: Vec<_> = self
            .subscriptions
            .iter()
            .filter(|token| publication.reference.matches_interest(&token.interest))
            .map(|token| AuthorizedObjectDelivery {
                subscriber: token.subscriber.clone(),
                reference: publication.reference.clone(),
            })
            .collect();
        self.updates.push(publication.reference);
        self.deliveries.extend(deliveries.clone());
        AuthorizedObjectPublication { deliveries }
    }

    fn status(&self) -> AuthorizedObjectFanoutStatus {
        AuthorizedObjectFanoutStatus {
            subscription_count: self.subscriptions.len() as u64,
            update_count: self.updates.len() as u64,
            delivery_count: self.deliveries.len() as u64,
        }
    }
}

impl Default for AuthorizedObjectFanout {
    fn default() -> Self {
        Self::new()
    }
}

impl Actor for AuthorizedObjectFanout {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        actor: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(actor)
    }
}

impl Message<AttendAuthorizedObjects> for AuthorizedObjectFanout {
    type Reply = AuthorizedObjectAttendanceSnapshot;

    async fn handle(
        &mut self,
        message: AttendAuthorizedObjects,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.attend(message)
    }
}

impl Message<WithdrawAuthorizedObjects> for AuthorizedObjectFanout {
    type Reply = AuthorizedObjectAttendanceWithdrawn;

    async fn handle(
        &mut self,
        message: WithdrawAuthorizedObjects,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.withdraw(message)
    }
}

impl Message<PublishAuthorizedObjectReference> for AuthorizedObjectFanout {
    type Reply = AuthorizedObjectPublication;

    async fn handle(
        &mut self,
        message: PublishAuthorizedObjectReference,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.publish(message)
    }
}

impl Message<ReadAuthorizedObjectFanoutStatus> for AuthorizedObjectFanout {
    type Reply = AuthorizedObjectFanoutStatus;

    async fn handle(
        &mut self,
        _message: ReadAuthorizedObjectFanoutStatus,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.status()
    }
}
