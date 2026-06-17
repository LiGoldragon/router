use nota_next::{Block, Delimiter, NotaBlock, NotaDecode, NotaDecodeError, NotaEncode};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

#[derive(
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    NotaEncode,
    NotaDecode,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
)]
pub struct ActorIdentifier(String);

impl ActorIdentifier {
    pub fn new(text: impl Into<String>) -> Self {
        Self(text.into())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    NotaEncode,
    NotaDecode,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
)]
pub struct MessageIdentifier(String);

impl MessageIdentifier {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn from_parts(
        sequence: u64,
        thread: &ThreadIdentifier,
        sender: &ActorIdentifier,
        recipient: &ActorIdentifier,
        body: &str,
    ) -> Self {
        let mut hash = ShortMessageHash::new();
        hash.feed_u64(sequence);
        hash.feed_str(thread.as_str());
        hash.feed_str(sender.as_str());
        hash.feed_str(recipient.as_str());
        hash.feed_str(body);
        Self(format!("m-{}", hash.finish_base32_3()))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    NotaEncode,
    NotaDecode,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
)]
pub struct ThreadIdentifier(String);

impl ThreadIdentifier {
    pub fn new(text: impl Into<String>) -> Self {
        Self(text.into())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct Actor {
    pub name: ActorIdentifier,
    pub pid: u32,
    pub endpoint: Option<EndpointTransport>,
}

impl NotaDecode for Actor {
    fn from_nota_block(block: &Block) -> std::result::Result<Self, NotaDecodeError> {
        let fields = NotaBlock::new(block).expect_children(Delimiter::Parenthesis, "Actor", 3)?;
        let pid = NotaBlock::new(&fields[1]).parse_integer()?;
        let pid = u32::try_from(pid).map_err(|_| NotaDecodeError::InvalidInteger {
            value: pid.to_string(),
        })?;
        Ok(Self {
            name: ActorIdentifier::from_nota_block(&fields[0])?,
            pid,
            endpoint: Option::<EndpointTransport>::from_nota_block(&fields[2])?,
        })
    }
}

impl NotaEncode for Actor {
    fn to_nota(&self) -> String {
        Delimiter::Parenthesis.wrap([
            self.name.to_nota(),
            self.pid.to_string(),
            self.endpoint.to_nota(),
        ])
    }
}

#[derive(
    Archive, RkyvSerialize, RkyvDeserialize, NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq,
)]
pub struct EndpointTransport {
    pub kind: EndpointKind,
    pub target: String,
    pub aux: Option<String>,
}

#[derive(
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    NotaEncode,
    NotaDecode,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
)]
pub enum EndpointKind {
    Human,
    HarnessSocket,
    PtySocket,
    ComponentSocket,
}

#[derive(
    Archive, RkyvSerialize, RkyvDeserialize, NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq,
)]
pub struct Attachment {
    pub path: String,
    pub media_type: Option<String>,
}

#[derive(
    Archive, RkyvSerialize, RkyvDeserialize, NotaEncode, NotaDecode, Debug, Clone, PartialEq, Eq,
)]
pub struct Message {
    pub id: MessageIdentifier,
    pub thread: ThreadIdentifier,
    pub from: ActorIdentifier,
    pub to: ActorIdentifier,
    pub body: String,
    pub attachments: Vec<Attachment>,
}

impl Message {
    pub fn new(
        id: MessageIdentifier,
        sender: impl Into<String>,
        recipient: impl Into<String>,
        body: MessageBody,
    ) -> Self {
        let sender = ActorIdentifier::new(sender.into());
        let recipient = ActorIdentifier::new(recipient.into());
        Self {
            id,
            thread: ThreadIdentifier::new(format!(
                "direct-{}-{}",
                sender.as_str(),
                recipient.as_str()
            )),
            from: sender,
            to: recipient,
            body: body.into_string(),
            attachments: Vec::new(),
        }
    }

    pub fn recipient(&self) -> &str {
        self.to.as_str()
    }

    pub fn to_nota(&self) -> String {
        NotaEncode::to_nota(self)
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

    fn into_string(self) -> String {
        self.value
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ShortMessageHash {
    value: u64,
}

impl ShortMessageHash {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    const ALPHABET: &'static [u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

    fn new() -> Self {
        Self {
            value: Self::OFFSET,
        }
    }

    fn feed_u64(&mut self, value: u64) {
        self.feed_bytes(value.to_le_bytes().as_slice());
    }

    fn feed_str(&mut self, text: &str) {
        self.feed_u64(text.len() as u64);
        self.feed_bytes(text.as_bytes());
    }

    fn feed_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.value ^= u64::from(*byte);
            self.value = self.value.wrapping_mul(Self::PRIME);
        }
    }

    fn finish_base32_3(self) -> String {
        let mut value = self.value;
        let mut text = String::with_capacity(3);
        for _ in 0..3 {
            text.push(Self::ALPHABET[(value & 31) as usize] as char);
            value >>= 5;
        }
        text
    }
}
