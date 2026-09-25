//! NVMe/TCP PDU framing and NVMe command layout (NVMe/TCP transport spec 1.0,
//! NVMe base spec 2.x). Header and data digests are not negotiated.

use anyhow::{bail, Context, Result};
use std::io::{Read, Write};

pub const PDU_IC_REQ: u8 = 0x00;
pub const PDU_IC_RESP: u8 = 0x01;
pub const PDU_H2C_TERM: u8 = 0x02;
pub const PDU_C2H_TERM: u8 = 0x03;
pub const PDU_CAPSULE_CMD: u8 = 0x04;
pub const PDU_CAPSULE_RESP: u8 = 0x05;
pub const PDU_H2C_DATA: u8 = 0x06;
pub const PDU_C2H_DATA: u8 = 0x07;
pub const PDU_R2T: u8 = 0x09;

pub const FLAG_LAST_PDU: u8 = 0x04;
pub const FLAG_C2H_SUCCESS: u8 = 0x08;
/// Connect CATTR: SQ flow control disabled. The host gives up SQ head
/// pointer reporting, which lets the controller finish a read with the
/// SUCCESS flag on its last C2HData PDU instead of a response capsule.
pub const CATTR_DISABLE_SQFLOW: u8 = 1 << 2;

pub const CH_LEN: usize = 8;
pub const CMD_HLEN: usize = CH_LEN + 64;
pub const DATA_HLEN: usize = 24;

// NVMe opcodes.
pub const OPC_FLUSH: u8 = 0x00;
pub const OPC_WRITE: u8 = 0x01;
pub const OPC_READ: u8 = 0x02;
pub const OPC_ADMIN_IDENTIFY: u8 = 0x06;
pub const OPC_ADMIN_KEEP_ALIVE: u8 = 0x18;
pub const OPC_FABRICS: u8 = 0x7f;

pub const FCTYPE_PROP_SET: u8 = 0x00;
pub const FCTYPE_CONNECT: u8 = 0x01;
pub const FCTYPE_PROP_GET: u8 = 0x04;

/// PSDT = SGL for the data pointer (CDW0 byte 1 bits 7:6 = 01b).
const FLAGS_SGL: u8 = 0x40;
/// SGL identifier: Transport SGL Data Block (type 5), transport-specific subtype 0xA.
const SGL_TRANSPORT_DATA_BLOCK: u8 = 0x5a;
/// SGL identifier: Data Block (type 0), subtype Offset (1): in-capsule data.
const SGL_INCAPSULE_OFFSET: u8 = 0x01;

/// A 64-byte submission queue entry.
#[derive(Clone, Copy)]
pub struct Sqe(pub [u8; 64]);

impl Sqe {
    pub fn new(opcode: u8, cid: u16, nsid: u32) -> Self {
        let mut b = [0u8; 64];
        b[0] = opcode;
        b[1] = FLAGS_SGL;
        b[2..4].copy_from_slice(&cid.to_le_bytes());
        b[4..8].copy_from_slice(&nsid.to_le_bytes());
        Sqe(b)
    }
    pub fn cid(&self) -> u16 {
        u16::from_le_bytes([self.0[2], self.0[3]])
    }
    pub fn set_cid(&mut self, cid: u16) {
        self.0[2..4].copy_from_slice(&cid.to_le_bytes());
    }
    pub fn set_u8(&mut self, off: usize, v: u8) {
        self.0[off] = v;
    }
    pub fn set_u16(&mut self, off: usize, v: u16) {
        self.0[off..off + 2].copy_from_slice(&v.to_le_bytes());
    }
    pub fn set_u32(&mut self, off: usize, v: u32) {
        self.0[off..off + 4].copy_from_slice(&v.to_le_bytes());
    }
    pub fn set_u64(&mut self, off: usize, v: u64) {
        self.0[off..off + 8].copy_from_slice(&v.to_le_bytes());
    }
    /// Data pointer describing a transfer the target moves with C2HData/R2T.
    pub fn sgl_transport(&mut self, len: u32) {
        self.set_u64(24, 0);
        self.set_u32(32, len);
        self.0[39] = SGL_TRANSPORT_DATA_BLOCK;
    }
    /// Data pointer describing data carried inside the command capsule.
    pub fn sgl_incapsule(&mut self, len: u32) {
        self.set_u64(24, 0);
        self.set_u32(32, len);
        self.0[39] = SGL_INCAPSULE_OFFSET;
    }
}

/// A 16-byte completion queue entry.
#[derive(Clone, Copy, Debug, Default)]
pub struct Cqe {
    pub dw0: u32,
    pub dw1: u32,
    pub cid: u16,
    pub status: u16,
}

impl Cqe {
    pub fn parse(b: &[u8]) -> Self {
        Cqe {
            dw0: u32::from_le_bytes(b[0..4].try_into().unwrap()),
            dw1: u32::from_le_bytes(b[4..8].try_into().unwrap()),
            cid: u16::from_le_bytes([b[12], b[13]]),
            status: u16::from_le_bytes([b[14], b[15]]),
        }
    }
    /// Status code + status code type, phase bit dropped. 0 = success.
    pub fn sc(&self) -> u16 {
        self.status >> 1
    }
}

/// Common header of a received PDU.
#[derive(Clone, Copy, Debug)]
pub struct Ch {
    pub ptype: u8,
    pub flags: u8,
    pub hlen: u8,
    pub pdo: u8,
    pub plen: u32,
}

pub fn read_ch(r: &mut impl Read) -> Result<Ch> {
    let mut b = [0u8; CH_LEN];
    r.read_exact(&mut b).context("read PDU common header")?;
    Ok(Ch {
        ptype: b[0],
        flags: b[1],
        hlen: b[2],
        pdo: b[3],
        plen: u32::from_le_bytes(b[4..8].try_into().unwrap()),
    })
}

/// Read one PDU's common header and PDU-specific header from a blocking
/// stream and validate them before any length in them is used.
pub fn read_hdr(r: &mut impl Read) -> Result<(Ch, Vec<u8>)> {
    let mut h = vec![0u8; CH_LEN];
    r.read_exact(&mut h).context("read PDU common header")?;
    let hlen = h[2] as usize;
    if hlen < CH_LEN {
        bail!("PDU type {:#x}: hlen {hlen} below the common header", h[0]);
    }
    h.resize(hlen, 0);
    r.read_exact(&mut h[CH_LEN..]).context("read PDU header")?;
    check_pdu_header(&h).map_err(|e| anyhow::anyhow!(e))?;
    let ch = Ch { ptype: h[0], flags: h[1], hlen: h[2], pdo: h[3], plen: u32::from_le_bytes(h[4..8].try_into().unwrap()) };
    Ok((ch, h.split_off(CH_LEN)))
}

fn ch_bytes(ptype: u8, flags: u8, hlen: usize, pdo: usize, plen: usize) -> [u8; CH_LEN] {
    let mut b = [0u8; CH_LEN];
    b[0] = ptype;
    b[1] = flags;
    b[2] = hlen as u8;
    b[3] = pdo as u8;
    b[4..8].copy_from_slice(&(plen as u32).to_le_bytes());
    b
}

/// Initialize Connection handshake. Returns (cpda, maxh2cdata).
pub fn ic_handshake(s: &mut (impl Read + Write)) -> Result<(u8, u32)> {
    let mut req = [0u8; 128];
    req[..CH_LEN].copy_from_slice(&ch_bytes(PDU_IC_REQ, 0, 128, 0, 128));
    // pfv=0, hpda=0, dgst=0, maxr2t=0 (one outstanding R2T per command).
    s.write_all(&req).context("send ICReq")?;
    let ch = read_ch(s)?;
    if ch.ptype != PDU_IC_RESP || ch.plen != 128 || ch.hlen != 128 {
        bail!("expected ICResp, got type {:#x} hlen {} plen {}", ch.ptype, ch.hlen, ch.plen);
    }
    let mut rest = [0u8; 120];
    s.read_exact(&mut rest)?;
    let cpda = rest[2];
    let dgst = rest[3];
    let maxh2c = u32::from_le_bytes(rest[4..8].try_into().unwrap());
    if dgst != 0 {
        bail!("target enabled digests ({dgst:#x}); not supported by the prototype");
    }
    Ok((cpda, maxh2c))
}

/// A command capsule, optionally carrying in-capsule data.
pub fn write_capsule(w: &mut impl Write, sqe: &Sqe, data: &[u8]) -> Result<()> {
    let pdo = if data.is_empty() { 0 } else { CMD_HLEN };
    let hdr = ch_bytes(PDU_CAPSULE_CMD, 0, CMD_HLEN, pdo, CMD_HLEN + data.len());
    let mut buf = Vec::with_capacity(CMD_HLEN + data.len());
    buf.extend_from_slice(&hdr);
    buf.extend_from_slice(&sqe.0);
    buf.extend_from_slice(data);
    w.write_all(&buf).context("send command capsule")
}

/// One H2CData PDU answering an R2T.
pub fn write_h2c_data(w: &mut impl Write, cid: u16, ttag: u16, datao: u32, data: &[u8], last: bool) -> Result<()> {
    let flags = if last { FLAG_LAST_PDU } else { 0 };
    let mut hdr = [0u8; DATA_HLEN];
    hdr[..CH_LEN].copy_from_slice(&ch_bytes(PDU_H2C_DATA, flags, DATA_HLEN, DATA_HLEN, DATA_HLEN + data.len()));
    hdr[8..10].copy_from_slice(&cid.to_le_bytes());
    hdr[10..12].copy_from_slice(&ttag.to_le_bytes());
    hdr[12..16].copy_from_slice(&datao.to_le_bytes());
    hdr[16..20].copy_from_slice(&(data.len() as u32).to_le_bytes());
    let mut buf = Vec::with_capacity(DATA_HLEN + data.len());
    buf.extend_from_slice(&hdr);
    buf.extend_from_slice(data);
    w.write_all(&buf).context("send H2CData")
}

/// Parsed PDU-specific header of a C2HData / R2T / CapsuleResp.
pub struct DataHdr {
    pub cid: u16,
    pub ttag: u16,
    pub off: u32,
    pub len: u32,
}

pub fn parse_data_hdr(psh: &[u8]) -> DataHdr {
    // psh starts right after the 8-byte common header.
    DataHdr {
        cid: u16::from_le_bytes([psh[0], psh[1]]),
        ttag: u16::from_le_bytes([psh[2], psh[3]]),
        off: u32::from_le_bytes(psh[4..8].try_into().unwrap()),
        len: u32::from_le_bytes(psh[8..12].try_into().unwrap()),
    }
}

/// Fabrics Connect command + its 1024-byte data.
pub fn connect_cmd(cid: u16, qid: u16, sqsize: u16, cattr: u8, kato_ms: u32, cntlid: u16, hostid: &[u8; 16], subnqn: &str, hostnqn: &str) -> (Sqe, Vec<u8>) {
    let mut sqe = Sqe::new(OPC_FABRICS, cid, 0);
    sqe.set_u8(4, FCTYPE_CONNECT);
    sqe.set_u16(40, 0); // recfmt
    sqe.set_u16(42, qid);
    sqe.set_u16(44, sqsize); // 0-based
    sqe.set_u8(46, cattr);
    sqe.set_u32(48, kato_ms);
    let mut data = vec![0u8; 1024];
    data[..16].copy_from_slice(hostid);
    data[16..18].copy_from_slice(&cntlid.to_le_bytes());
    data[256..256 + subnqn.len()].copy_from_slice(subnqn.as_bytes());
    data[512..512 + hostnqn.len()].copy_from_slice(hostnqn.as_bytes());
    sqe.sgl_incapsule(1024);
    (sqe, data)
}

pub fn prop_set_cmd(cid: u16, offset: u32, value: u64, size8: bool) -> Sqe {
    let mut sqe = Sqe::new(OPC_FABRICS, cid, 0);
    sqe.set_u8(4, FCTYPE_PROP_SET);
    sqe.set_u8(40, if size8 { 1 } else { 0 });
    sqe.set_u32(44, offset);
    sqe.set_u64(48, value);
    sqe
}

pub fn prop_get_cmd(cid: u16, offset: u32, size8: bool) -> Sqe {
    let mut sqe = Sqe::new(OPC_FABRICS, cid, 0);
    sqe.set_u8(4, FCTYPE_PROP_GET);
    sqe.set_u8(40, if size8 { 1 } else { 0 });
    sqe.set_u32(44, offset);
    sqe
}

pub fn identify_cmd(cid: u16, nsid: u32, cns: u32) -> Sqe {
    let mut sqe = Sqe::new(OPC_ADMIN_IDENTIFY, cid, nsid);
    sqe.set_u32(40, cns);
    sqe.sgl_transport(4096);
    sqe
}

pub fn rw_cmd(opcode: u8, cid: u16, nsid: u32, slba: u64, nlb: u32, len: u32, inline: bool) -> Sqe {
    let mut sqe = Sqe::new(opcode, cid, nsid);
    sqe.set_u64(40, slba);
    sqe.set_u32(48, nlb - 1);
    if inline {
        sqe.sgl_incapsule(len);
    } else {
        sqe.sgl_transport(len);
    }
    sqe
}

pub fn flush_cmd(cid: u16, nsid: u32) -> Sqe {
    Sqe::new(OPC_FLUSH, cid, nsid)
}

pub fn keep_alive_cmd(cid: u16) -> Sqe {
    Sqe::new(OPC_ADMIN_KEEP_ALIVE, cid, 0)
}

/// Header bytes of a command capsule whose in-capsule data (if any) is sent
/// separately, so the payload can go out straight from the caller's buffer.
pub fn capsule_header(sqe: &Sqe, data_len: usize) -> Vec<u8> {
    let pdo = if data_len == 0 { 0 } else { CMD_HLEN };
    let mut v = Vec::with_capacity(CMD_HLEN);
    v.extend_from_slice(&ch_bytes(PDU_CAPSULE_CMD, 0, CMD_HLEN, pdo, CMD_HLEN + data_len));
    v.extend_from_slice(&sqe.0);
    v
}

/// Header bytes of an H2CData PDU; the payload follows separately.
pub fn h2c_header(cid: u16, ttag: u16, datao: u32, len: usize, last: bool) -> Vec<u8> {
    let flags = if last { FLAG_LAST_PDU } else { 0 };
    let mut hdr = vec![0u8; DATA_HLEN];
    hdr[..CH_LEN].copy_from_slice(&ch_bytes(PDU_H2C_DATA, flags, DATA_HLEN, DATA_HLEN, DATA_HLEN + len));
    hdr[8..10].copy_from_slice(&cid.to_le_bytes());
    hdr[10..12].copy_from_slice(&ttag.to_le_bytes());
    hdr[12..16].copy_from_slice(&datao.to_le_bytes());
    hdr[16..20].copy_from_slice(&(len as u32).to_le_bytes());
    hdr
}

pub const FLAG_HDGST: u8 = 0x01;
pub const FLAG_DDGST: u8 = 0x02;

/// Validate a received PDU's header before any field is used to index a
/// buffer. `h` holds at least the header bytes that have arrived (it may be
/// the whole PDU); `plen` is the PDU length the receiver will consume. A
/// target is not trusted: a bad header must become an error that tears the
/// connection down, never a panic or a write outside the request's buffer.
pub fn check_pdu_header(h: &[u8]) -> Result<(), String> {
    if h.len() < CH_LEN {
        return Err(format!("PDU header truncated ({} bytes)", h.len()));
    }
    let (ptype, flags, hlen, pdo) = (h[0], h[1], h[2] as usize, h[3] as usize);
    let plen = u32::from_le_bytes(h[4..8].try_into().unwrap()) as usize;
    if hlen < CH_LEN || hlen > plen {
        return Err(format!("PDU type {ptype:#x}: hlen {hlen} outside 8..=plen {plen}"));
    }
    if h.len() < hlen {
        return Err(format!("PDU type {ptype:#x}: only {} of {hlen} header bytes present", h.len()));
    }
    if flags & (FLAG_HDGST | FLAG_DDGST) != 0 {
        return Err(format!("PDU type {ptype:#x}: digest flags {flags:#x} set, digests were not negotiated"));
    }
    match ptype {
        PDU_CAPSULE_RESP => {
            if hlen != CH_LEN + 16 || plen != hlen {
                return Err(format!("CapsuleResp: hlen {hlen} plen {plen}, want 24/24"));
            }
        }
        PDU_C2H_DATA => {
            if hlen != DATA_HLEN || pdo < hlen || pdo > plen {
                return Err(format!("C2HData: hlen {hlen} pdo {pdo} plen {plen}"));
            }
            let d = parse_data_hdr(&h[CH_LEN..hlen]);
            if d.len == 0 || plen != pdo + d.len as usize {
                return Err(format!("C2HData: datal {} does not match plen {plen} - pdo {pdo}", d.len));
            }
            if flags & FLAG_C2H_SUCCESS != 0 && flags & FLAG_LAST_PDU == 0 {
                return Err("C2HData: SUCCESS without LAST_PDU".into());
            }
        }
        PDU_R2T => {
            if hlen != DATA_HLEN || plen != hlen {
                return Err(format!("R2T: hlen {hlen} plen {plen}, want 24/24"));
            }
            if parse_data_hdr(&h[CH_LEN..hlen]).len == 0 {
                return Err("R2T: zero length".into());
            }
        }
        PDU_C2H_TERM => {}
        t => return Err(format!("unexpected PDU type {t:#x}")),
    }
    Ok(())
}

/// `sc` as returned by `Cqe::sc()` (status field without the phase bit):
/// bits 7:0 SC, 10:8 SCT, 14 DNR. Path-related errors (SCT 3) without Do Not
/// Retry are the target telling us to try another path, as the kernel's
/// nvme_is_path_error does.
pub fn is_path_error(sc: u16) -> bool {
    (sc >> 8) & 0x7 == 3 && sc & (1 << 14) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hdr(ptype: u8, flags: u8, hlen: usize, pdo: usize, plen: usize) -> Vec<u8> {
        let mut h = vec![0u8; hlen.max(CH_LEN)];
        h[..CH_LEN].copy_from_slice(&ch_bytes(ptype, flags, hlen, pdo, plen));
        h
    }
    fn c2h(flags: u8, pdo: usize, datal: u32, plen: usize) -> Vec<u8> {
        let mut h = hdr(PDU_C2H_DATA, flags, DATA_HLEN, pdo, plen);
        h[16..20].copy_from_slice(&datal.to_le_bytes());
        h
    }

    #[test]
    fn accepts_well_formed_pdus() {
        assert!(check_pdu_header(&hdr(PDU_CAPSULE_RESP, 0, 24, 0, 24)).is_ok());
        assert!(check_pdu_header(&c2h(FLAG_LAST_PDU | FLAG_C2H_SUCCESS, 24, 4096, 24 + 4096)).is_ok());
        assert!(check_pdu_header(&c2h(0, 32, 8192, 32 + 8192)).is_ok()); // pdo padded for cpda
        let mut r2t = hdr(PDU_R2T, 0, 24, 0, 24);
        r2t[16..20].copy_from_slice(&65536u32.to_le_bytes());
        assert!(check_pdu_header(&r2t).is_ok());
    }

    #[test]
    fn rejects_headers_that_would_index_out_of_bounds() {
        // Each of these panicked or was accepted by the receiver before
        // header validation existed.
        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("truncated common header", vec![PDU_CAPSULE_RESP, 0, 24]),
            ("hlen below common header", hdr(PDU_C2H_DATA, 0, 4, 4, 4096)),
            ("hlen beyond plen", hdr(PDU_CAPSULE_RESP, 0, 24, 0, 16)),
            ("CapsuleResp shorter than a CQE", hdr(PDU_CAPSULE_RESP, 0, 16, 0, 16)),
            ("CapsuleResp with trailing data", hdr(PDU_CAPSULE_RESP, 0, 24, 0, 64)),
            ("C2H pdo inside the header", c2h(0, 8, 4096, 8 + 4096)),
            ("C2H pdo beyond plen", c2h(0, 200, 16, 100)),
            ("C2H datal disagrees with plen", c2h(0, 24, 4096, 24 + 1000)),
            ("C2H zero datal", c2h(0, 24, 0, 24)),
            ("C2H SUCCESS without LAST", c2h(FLAG_C2H_SUCCESS, 24, 4096, 24 + 4096)),
            ("digest flag not negotiated", c2h(FLAG_DDGST | FLAG_LAST_PDU, 24, 4096, 24 + 4096)),
            ("R2T with zero length", hdr(PDU_R2T, 0, 24, 0, 24)),
            ("R2T with trailing data", hdr(PDU_R2T, 0, 24, 0, 48)),
            ("unknown type", hdr(0x42, 0, 24, 0, 24)),
        ];
        for (name, h) in cases {
            assert!(check_pdu_header(&h).is_err(), "{name} was accepted");
        }
        // Header claims more bytes than are present: must be refused, not read.
        let short = c2h(0, 24, 4096, 24 + 4096);
        assert!(check_pdu_header(&short[..20]).is_err());
    }

    #[test]
    fn path_error_classification() {
        assert!(is_path_error(0x3 << 8 | 0x01)); // ANA persistent loss
        assert!(is_path_error(0x3 << 8 | 0x71)); // host aborted
        assert!(!is_path_error(1 << 14 | 0x3 << 8 | 0x01)); // DNR set
        assert!(!is_path_error(0x0 << 8 | 0x80)); // generic LBA out of range
        assert!(!is_path_error(0x2 << 8 | 0x81)); // media: unrecovered read
    }
}
