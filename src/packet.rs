use std::fmt;

/// Size of an encoded uTP header in bytes.
const UTP_PACKET_HEADER_LEN: usize = 20;

/// Size of a Selective ACK segment in bits.
const SELECTIVE_ACK_BITS: usize = 32;

/// Size of an extension identifier in bytes.
const EXTENSION_TYPE_LEN: usize = 1;

/// Size of an extension length specifier in bytes.
const EXTENSION_LEN_LEN: usize = 1;

#[derive(Copy, Clone, Debug)]
pub struct InvalidUtpPacketType;

impl fmt::Display for InvalidUtpPacketType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid uTP packet type")
    }
}

#[derive(Copy, Clone, Debug)]
pub struct InvalidUtpVersion;

impl fmt::Display for InvalidUtpVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid uTP version")
    }
}

#[derive(Copy, Clone, Debug)]
pub enum SelectiveAckError {
    InsufficientLen,
    InvalidLen,
}

impl From<SelectiveAckError> for UtpPacketError {
    fn from(value: SelectiveAckError) -> Self {
        Self::InvalidExtension(UtpExtensionError::from(value))
    }
}

impl fmt::Display for SelectiveAckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::InsufficientLen => "selective ACK len must be at least 32 bits",
            Self::InvalidLen => "selective ACK len must be a multiple of 32 bits",
        };

        write!(f, "{}", s)
    }
}

#[derive(Clone, Debug)]
pub enum UtpExtensionError {
    InsufficientLen,
    InvalidSelectiveAck(SelectiveAckError),
}

impl From<SelectiveAckError> for UtpExtensionError {
    fn from(value: SelectiveAckError) -> Self {
        Self::InvalidSelectiveAck(value)
    }
}

impl fmt::Display for UtpExtensionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s: String = match self {
            Self::InsufficientLen => String::from("insufficient extension len"),
            Self::InvalidSelectiveAck(err) => err.to_string(),
        };

        write!(f, "{}", s)
    }
}

#[derive(Clone, Debug)]
pub enum UtpPacketHeaderError {
    InvalidPacketType(InvalidUtpPacketType),
    InvalidVersion(InvalidUtpVersion),
    InvalidExtension(UtpExtensionError),
    InvalidLen,
}

impl From<InvalidUtpPacketType> for UtpPacketHeaderError {
    fn from(value: InvalidUtpPacketType) -> Self {
        Self::InvalidPacketType(value)
    }
}

impl From<InvalidUtpVersion> for UtpPacketHeaderError {
    fn from(value: InvalidUtpVersion) -> Self {
        Self::InvalidVersion(value)
    }
}

impl From<UtpExtensionError> for UtpPacketHeaderError {
    fn from(value: UtpExtensionError) -> Self {
        Self::InvalidExtension(value)
    }
}

#[derive(Clone, Debug)]
pub enum UtpPacketError {
    InvalidHeader(UtpPacketHeaderError),
    InvalidExtension(UtpExtensionError),
    InvalidLen,
    EmptyDataPayload,
}

impl From<UtpPacketHeaderError> for UtpPacketError {
    fn from(value: UtpPacketHeaderError) -> Self {
        Self::InvalidHeader(value)
    }
}

impl From<UtpExtensionError> for UtpPacketError {
    fn from(value: UtpExtensionError) -> Self {
        Self::InvalidExtension(value)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum UtpPacketType {
    Data,
    Fin,
    State,
    Reset,
    Syn,
}

impl TryFrom<u8> for UtpPacketType {
    type Error = InvalidUtpPacketType;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Data),
            1 => Ok(Self::Fin),
            2 => Ok(Self::State),
            3 => Ok(Self::Reset),
            4 => Ok(Self::Syn),
            _ => Err(InvalidUtpPacketType),
        }
    }
}

impl From<UtpPacketType> for u8 {
    fn from(value: UtpPacketType) -> u8 {
        match value {
            UtpPacketType::Data => 0,
            UtpPacketType::Fin => 1,
            UtpPacketType::State => 2,
            UtpPacketType::Reset => 3,
            UtpPacketType::Syn => 4,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum UtpVersion {
    One,
}

impl TryFrom<u8> for UtpVersion {
    type Error = InvalidUtpVersion;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::One),
            _ => Err(InvalidUtpVersion),
        }
    }
}

impl From<UtpVersion> for u8 {
    fn from(value: UtpVersion) -> u8 {
        match value {
            UtpVersion::One => 1,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Extension {
    None,
    SelectiveAck,
    Unknown(u8),
}

impl From<u8> for Extension {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::None,
            1 => Self::SelectiveAck,
            unknown => Self::Unknown(unknown),
        }
    }
}

impl From<Extension> for u8 {
    fn from(value: Extension) -> u8 {
        match value {
            Extension::None => 0,
            Extension::SelectiveAck => 1,
            Extension::Unknown(ext) => ext,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct UtpPacketHeader {
    packet_type: UtpPacketType,
    version: UtpVersion,
    extension: Extension,
    conn_id: u16,
    ts_microseconds: u32,
    ts_diff_microseconds: u32,
    window_size: u32,
    seq_num: u16,
    ack_num: u16,
}

impl UtpPacketHeader {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![];

        let packet_type = Into::<u8>::into(self.packet_type).to_be_bytes()[0];
        let version = Into::<u8>::into(self.version).to_be_bytes()[0];
        let type_version = (packet_type << 4) | version;
        bytes.push(type_version);

        let extension = u8::from(self.extension);
        bytes.push(extension);

        bytes.extend(self.conn_id.to_be_bytes());
        bytes.extend(self.ts_microseconds.to_be_bytes());
        bytes.extend(self.ts_diff_microseconds.to_be_bytes());
        bytes.extend(self.window_size.to_be_bytes());
        bytes.extend(self.seq_num.to_be_bytes());
        bytes.extend(self.ack_num.to_be_bytes());

        bytes
    }

    pub fn decode(value: &[u8]) -> Result<Self, UtpPacketHeaderError> {
        if value.len() != UTP_PACKET_HEADER_LEN {
            return Err(UtpPacketHeaderError::InvalidLen);
        }

        let packet_type = value[0] >> 4;
        let packet_type = UtpPacketType::try_from(u8::from_be(packet_type))?;

        let version = value[0] & 0x0F;
        let version = UtpVersion::try_from(u8::from_be(version))?;

        let extension = u8::from_be(value[1]);
        let extension = Extension::from(extension);

        let conn_id = [value[2], value[3]];
        let conn_id = u16::from_be_bytes(conn_id);

        let ts_microseconds = [value[4], value[5], value[6], value[7]];
        let ts_microseconds = u32::from_be_bytes(ts_microseconds);

        let ts_diff_microseconds = [value[8], value[9], value[10], value[11]];
        let ts_diff_microseconds = u32::from_be_bytes(ts_diff_microseconds);

        let window_size = [value[12], value[13], value[14], value[15]];
        let window_size = u32::from_be_bytes(window_size);

        let seq_num = [value[16], value[17]];
        let seq_num = u16::from_be_bytes(seq_num);

        let ack_num = [value[18], value[19]];
        let ack_num = u16::from_be_bytes(ack_num);

        Ok(Self {
            packet_type,
            version,
            extension,
            conn_id,
            ts_microseconds,
            ts_diff_microseconds,
            window_size,
            seq_num,
            ack_num,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectiveAck {
    acked: Vec<[bool; SELECTIVE_ACK_BITS]>,
}

impl fmt::Display for SelectiveAck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::new();
        for chunk in &self.acked {
            for bit in chunk {
                if *bit {
                    s.push('1');
                } else {
                    s.push('0');
                }
            }
        }
        write!(f, "{}", s)
    }
}

impl SelectiveAck {
    pub fn new(acked: Vec<bool>) -> Self {
        let chunks = acked.as_slice().chunks_exact(SELECTIVE_ACK_BITS);
        let remainder = chunks.remainder();

        let mut acked = Vec::new();
        for chunk in chunks {
            let mut fragment: [bool; SELECTIVE_ACK_BITS] = [false; SELECTIVE_ACK_BITS];
            fragment.copy_from_slice(chunk);
            acked.push(fragment);
        }

        if !remainder.is_empty() {
            let mut fragment: [bool; SELECTIVE_ACK_BITS] = [false; SELECTIVE_ACK_BITS];
            fragment[..remainder.len()].copy_from_slice(remainder);
            acked.push(fragment);
        }

        Self { acked }
    }

    /// Returns the length in bytes of the encoded Selective ACK.
    pub fn encoded_len(&self) -> usize {
        (self.acked.len() * SELECTIVE_ACK_BITS) / 8
    }

    pub fn acked(&self) -> Vec<bool> {
        self.acked
            .clone()
            .into_iter()
            .flatten()
            .collect::<Vec<bool>>()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bitmask = vec![];

        for word in &self.acked {
            let chunks = word.as_slice().chunks_exact(8);

            for chunk in chunks {
                let mut byte = 0;

                byte |= u8::from(chunk[7]) << 7;
                byte |= u8::from(chunk[6]) << 6;
                byte |= u8::from(chunk[5]) << 5;
                byte |= u8::from(chunk[4]) << 4;
                byte |= u8::from(chunk[3]) << 3;
                byte |= u8::from(chunk[2]) << 2;
                byte |= u8::from(chunk[1]) << 1;
                byte |= u8::from(chunk[0]);

                bitmask.push(byte);
            }
        }

        bitmask
    }

    pub fn decode(value: &[u8]) -> Result<Self, SelectiveAckError> {
        if value.len() < 4 {
            return Err(SelectiveAckError::InsufficientLen);
        }
        if value.len() % 4 != 0 {
            return Err(SelectiveAckError::InvalidLen);
        }

        let mut acked: Vec<[bool; 32]> = vec![];
        let mut tmp = [false; 32];
        for (index, byte) in value.iter().enumerate() {
            tmp[(index * 8) % 32] = (*byte & 0b0000_0001) != 0;
            tmp[(index * 8 + 1) % 32] = (*byte & 0b0000_0010) != 0;
            tmp[(index * 8 + 2) % 32] = (*byte & 0b0000_0100) != 0;
            tmp[(index * 8 + 3) % 32] = (*byte & 0b0000_1000) != 0;
            tmp[(index * 8 + 4) % 32] = (*byte & 0b0001_0000) != 0;
            tmp[(index * 8 + 5) % 32] = (*byte & 0b0010_0000) != 0;
            tmp[(index * 8 + 6) % 32] = (*byte & 0b0100_0000) != 0;
            tmp[(index * 8 + 7) % 32] = (*byte & 0b1000_0000) != 0;

            if (index + 1) % 4 == 0 {
                acked.push(tmp);
                tmp = [false; 32];
            }
        }

        if value.len() % 4 != 0 {
            acked.push(tmp);
        }

        Ok(Self { acked })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UtpPacket {
    header: UtpPacketHeader,
    selective_ack: Option<SelectiveAck>,
    payload: Vec<u8>,
}

impl UtpPacket {
    pub fn packet_type(&self) -> UtpPacketType {
        self.header.packet_type
    }

    pub fn conn_id(&self) -> u16 {
        self.header.conn_id
    }

    pub fn ts_microseconds(&self) -> u32 {
        self.header.ts_microseconds
    }

    pub fn ts_diff_microseconds(&self) -> u32 {
        self.header.ts_diff_microseconds
    }

    pub fn window_size(&self) -> u32 {
        self.header.window_size
    }

    pub fn seq_num(&self) -> u16 {
        self.header.seq_num
    }

    pub fn ack_num(&self) -> u16 {
        self.header.ack_num
    }

    pub fn selective_ack(&self) -> Option<&SelectiveAck> {
        self.selective_ack.as_ref()
    }

    pub fn payload(&self) -> &Vec<u8> {
        &self.payload
    }

    /// Returns the length in bytes of the encoded packet.
    pub fn encoded_len(&self) -> usize {
        let mut len = UTP_PACKET_HEADER_LEN;
        if let Some(ref sack) = self.selective_ack {
            len += sack.encoded_len() + EXTENSION_TYPE_LEN + EXTENSION_LEN_LEN;
        }
        len += self.payload.len();

        len
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![];

        bytes.extend(self.header.encode());
        if let Some(ack) = &self.selective_ack {
            let ack = ack.encode();
            bytes.push(Extension::None.into());
            bytes.push((ack.len() as u8).to_be_bytes()[0]);
            bytes.extend(ack);
        }
        bytes.extend_from_slice(self.payload.as_slice());

        bytes
    }

    pub fn decode(value: &[u8]) -> Result<Self, UtpPacketError> {
        if value.len() < UTP_PACKET_HEADER_LEN {
            return Err(UtpPacketError::InvalidHeader(
                UtpPacketHeaderError::InvalidLen,
            ));
        }

        let mut header: [u8; UTP_PACKET_HEADER_LEN] = [0; UTP_PACKET_HEADER_LEN];
        header.copy_from_slice(&value[..UTP_PACKET_HEADER_LEN]);
        let header = UtpPacketHeader::decode(&header)?;

        let (extensions, extensions_len) =
            Self::decode_raw_extensions(header.extension, &value[UTP_PACKET_HEADER_LEN..])?;

        // Look for the first (if any) Selective ACK extension, and attempt to decode it.
        // TODO: Evaluate whether duplicate extensions should constitute an error.
        let selective_ack = extensions
            .iter()
            .find(|(ext, _)| *ext == Extension::SelectiveAck);
        let selective_ack = match selective_ack {
            Some((_, data)) => Some(SelectiveAck::decode(data)?),
            None => None,
        };

        // TODO: Save all raw extensions and make them accessible. People should be able to use
        // custom extensions.

        // The packet payload is the remainder of the packet.
        let payload_start_index = UTP_PACKET_HEADER_LEN + extensions_len;
        let payload = if payload_start_index == value.len() {
            vec![]
        } else {
            value[payload_start_index..].to_vec()
        };

        if header.packet_type == UtpPacketType::Data && payload.is_empty() {
            return Err(UtpPacketError::EmptyDataPayload);
        }

        Ok(Self {
            header,
            selective_ack,
            payload,
        })
    }

    // TODO: Resolve disabled clippy lint.
    #[allow(clippy::type_complexity)]
    fn decode_raw_extensions(
        first_ext: Extension,
        data: &[u8],
    ) -> Result<(Vec<(Extension, Vec<u8>)>, usize), UtpExtensionError> {
        let mut ext = first_ext;
        let mut index = 0;

        let mut extensions: Vec<(Extension, Vec<u8>)> = Vec::new();
        while ext != Extension::None {
            if data[index..].len() < 2 {
                return Err(UtpExtensionError::InsufficientLen);
            }

            let next_ext = u8::from_be_bytes([data[index]]);

            let ext_len = u8::from_be_bytes([data[index + 1]]);
            let ext_len = usize::from(ext_len);

            let ext_start = index + 2;
            if data[ext_start..].len() < ext_len {
                return Err(UtpExtensionError::InsufficientLen);
            }

            let ext_data = data[ext_start..ext_start + ext_len].to_vec();
            extensions.push((ext, ext_data));

            ext = Extension::from(next_ext);
            index = ext_start + ext_len;
        }

        Ok((extensions, index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use quickcheck::{quickcheck, Arbitrary, Gen, TestResult};

    impl Arbitrary for UtpPacketHeader {
        fn arbitrary(g: &mut Gen) -> Self {
            let packet_type = u8::arbitrary(g);
            let packet_type = if packet_type % 5 == 0 {
                UtpPacketType::Data
            } else if packet_type % 5 == 1 {
                UtpPacketType::Fin
            } else if packet_type % 5 == 2 {
                UtpPacketType::State
            } else if packet_type % 5 == 3 {
                UtpPacketType::Reset
            } else {
                UtpPacketType::Syn
            };

            let extension = u8::arbitrary(g);
            let extension = Extension::from(extension);

            Self {
                packet_type,
                version: UtpVersion::One,
                extension,
                conn_id: u16::arbitrary(g),
                ts_microseconds: u32::arbitrary(g),
                ts_diff_microseconds: u32::arbitrary(g),
                window_size: u32::arbitrary(g),
                ack_num: u16::arbitrary(g),
                seq_num: u16::arbitrary(g),
            }
        }
    }

    impl Arbitrary for SelectiveAck {
        fn arbitrary(g: &mut Gen) -> Self {
            let bits: Vec<bool> = Vec::arbitrary(g);

            let mut acked: Vec<[bool; 32]> = vec![];

            let mut tmp = [false; 32];
            for (index, bit) in bits.iter().enumerate() {
                tmp[index % 32] = *bit;

                if (index + 1) % 32 == 0 {
                    acked.push(tmp);
                    tmp = [false; 32];
                }
            }

            if bits.len() % 32 != 0 || acked.len() == 0 {
                acked.push(tmp);
            }

            Self { acked }
        }
    }

    // TODO: Add more tests. For example, packet encoding and decoding should test for arbitrary
    // extensions.

    #[test]
    fn header_encode_decode() {
        fn prop(header: UtpPacketHeader) -> TestResult {
            let encoded = header.encode();
            let encoded: [u8; 20] = encoded
                .try_into()
                .expect("invalid length for encoded uTP packet header");
            let decoded =
                UtpPacketHeader::decode(&encoded).expect("failed to decode uTP packet header");

            TestResult::from_bool(decoded == header)
        }
        quickcheck(prop as fn(UtpPacketHeader) -> TestResult);
    }

    #[test]
    fn selective_ack_encode_decode() {
        fn prop(selective_ack: SelectiveAck) -> TestResult {
            let encoded_len = selective_ack.encoded_len();

            let encoded = selective_ack.encode();

            assert!(encoded.len() % (SELECTIVE_ACK_BITS / 8) == 0);
            assert_eq!(encoded.len(), encoded_len);

            let decoded = SelectiveAck::decode(&encoded).expect("failed to decode Selective ACK");

            TestResult::from_bool(decoded == selective_ack)
        }
        quickcheck(prop as fn(SelectiveAck) -> TestResult);
    }

    #[test]
    fn packet_encode_decode() {
        fn prop(
            mut header: UtpPacketHeader,
            selective_ack: SelectiveAck,
            payload: Vec<u8>,
        ) -> TestResult {
            if payload.is_empty() {
                return TestResult::discard();
            }

            let selective_ack = if selective_ack.acked.is_empty() {
                None
            } else {
                Some(selective_ack)
            };
            match selective_ack {
                Some(..) => {
                    header.extension = Extension::SelectiveAck;
                }
                None => {
                    header.extension = Extension::None;
                }
            }

            let packet = UtpPacket {
                header,
                selective_ack,
                payload,
            };

            let encoded_len = packet.encoded_len();

            let encoded = packet.encode();

            assert_eq!(encoded.len(), encoded_len);

            let decoded = UtpPacket::decode(&encoded).expect("failed to decode uTP packet");

            TestResult::from_bool(decoded == packet)
        }
        quickcheck(prop as fn(UtpPacketHeader, SelectiveAck, Vec<u8>) -> TestResult);
    }
}
