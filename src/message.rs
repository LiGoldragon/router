#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MessageId {
    value: String,
}

impl MessageId {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageBody {
    value: String,
}

impl MessageBody {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonaMessage {
    id: MessageId,
    sender: String,
    recipient: String,
    body: MessageBody,
}

impl PersonaMessage {
    pub fn new(
        id: MessageId,
        sender: impl Into<String>,
        recipient: impl Into<String>,
        body: MessageBody,
    ) -> Self {
        Self {
            id,
            sender: sender.into(),
            recipient: recipient.into(),
            body,
        }
    }

    pub fn recipient(&self) -> &str {
        &self.recipient
    }
}
