use anyhow::{anyhow, bail, Context, Result};
use futures_util::{stream, StreamExt};
use serde_json::Value;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use tokio::net::UdpSocket;

fn raw_icmp(v6: bool) -> std::io::Result<Socket> {
    Socket::new(
        if v6 { Domain::IPV6 } else { Domain::IPV4 },
        Type::RAW,
        Some(if v6 {
            Protocol::ICMPV6
        } else {
            Protocol::ICMPV4
        }),
    )
}

pub fn supports_icmp() -> bool {
    raw_icmp(false).is_ok() || raw_icmp(true).is_ok()
}

fn bytes(text: &str, hex: bool) -> Result<Vec<u8>> {
    if hex {
        let compact: String = text.chars().filter(|ch| !ch.is_whitespace()).collect();
        Ok(hex::decode(compact).context("invalid hexadecimal packet")?)
    } else {
        Ok(text.as_bytes().to_vec())
    }
}

pub async fn udp(target: &str, options: &Value) -> Result<()> {
    let hex = options.get("encoding").and_then(Value::as_str) == Some("hex");
    let payload = bytes(
        options
            .get("send")
            .and_then(Value::as_str)
            .unwrap_or("ping"),
        hex,
    )?;
    let expected = bytes(
        options.get("expect").and_then(Value::as_str).unwrap_or(""),
        hex,
    )?;
    if payload.is_empty() || payload.len() > 65507 {
        bail!("UDP payload must contain 1–65507 bytes");
    }
    let addresses: Vec<_> =
        tokio::net::lookup_host(target.strip_prefix("udp://").unwrap_or(target))
            .await?
            .take(8)
            .collect();
    if addresses.is_empty() {
        bail!("UDP host did not resolve");
    }
    let mut attempts = stream::iter(addresses)
        .map(|address| {
            let payload = &payload;
            let expected = &expected;
            async move {
                let socket = UdpSocket::bind(if address.is_ipv6() {
                    "[::]:0"
                } else {
                    "0.0.0.0:0"
                })
                .await?;
                socket.connect(address).await?;
                socket.send(payload).await?;
                let mut reply = vec![0; 65536];
                let length = socket.recv(&mut reply).await?;
                if !expected.is_empty()
                    && !reply[..length]
                        .windows(expected.len())
                        .any(|part| part == expected)
                {
                    bail!("UDP response did not match the expected bytes");
                }
                Ok::<_, anyhow::Error>(())
            }
        })
        .buffer_unordered(4);
    let mut last = anyhow!("UDP did not receive a response");
    while let Some(result) = attempts.next().await {
        match result {
            Ok(()) => return Ok(()),
            Err(error) => last = error,
        }
    }
    Err(last)
}

fn checksum(packet: &[u8]) -> u16 {
    let mut sum = 0u32;
    for pair in packet.chunks(2) {
        sum += u32::from(u16::from_be_bytes([pair[0], *pair.get(1).unwrap_or(&0)]));
    }
    while sum > 0xffff {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

fn echo_request(v6: bool, nonce: &[u8; 24]) -> Vec<u8> {
    let mut packet = vec![if v6 { 128 } else { 8 }, 0, 0, 0, nonce[0], nonce[1], 0, 1];
    packet.extend_from_slice(nonce);
    if !v6 {
        let check = checksum(&packet);
        packet[2..4].copy_from_slice(&check.to_be_bytes());
    }
    packet
}

fn matches_echo(packet: &[u8], request: &[u8], v6: bool) -> bool {
    let offset = if !v6 && packet.first().is_some_and(|byte| byte >> 4 == 4) {
        if packet.len() < 20 || packet[9] != 1 {
            return false;
        }
        usize::from(packet[0] & 15) * 4
    } else if v6 && packet.first().is_some_and(|byte| byte >> 4 == 6) {
        if packet.len() < 40 || packet[6] != 58 {
            return false;
        }
        40
    } else {
        0
    };
    let Some(reply) = packet.get(offset..) else {
        return false;
    };
    reply.len() == request.len()
        && reply[0] == if v6 { 129 } else { 0 }
        && reply[1] == 0
        && reply[4..] == request[4..]
        && (v6 || checksum(reply) == 0)
}

async fn echo(address: IpAddr) -> Result<()> {
    let v6 = address.is_ipv6();
    let socket = raw_icmp(v6).context("ICMP requires raw-socket permission")?;
    socket.set_nonblocking(true)?;
    let bind = SocketAddr::new(
        if v6 {
            Ipv6Addr::UNSPECIFIED.into()
        } else {
            Ipv4Addr::UNSPECIFIED.into()
        },
        0,
    );
    socket.bind(&bind.into())?;
    socket.connect(&SocketAddr::new(address, 0).into())?;
    let socket = UdpSocket::from_std(socket.into())?;
    let mut nonce = [0u8; 24];
    getrandom::getrandom(&mut nonce)
        .map_err(|_| anyhow!("could not generate ICMP probe identity"))?;
    let packet = echo_request(v6, &nonce);
    socket.send(&packet).await?;
    let mut buffer = [0; 2048];
    loop {
        let (length, peer) = socket.recv_from(&mut buffer).await?;
        if peer.ip() == address && matches_echo(&buffer[..length], &packet, v6) {
            return Ok(());
        }
    }
}

pub async fn icmp(target: &str) -> Result<()> {
    let target = target.trim_start_matches('[').trim_end_matches(']');
    let addresses: Vec<_> = tokio::net::lookup_host((target, 0))
        .await?
        .take(8)
        .map(|address| address.ip())
        .collect();
    if addresses.is_empty() {
        bail!("ICMP host did not resolve");
    }
    let mut attempts = stream::iter(addresses).map(echo).buffer_unordered(4);
    let mut last = anyhow!("ICMP did not receive an echo reply");
    while let Some(result) = attempts.next().await {
        match result {
            Ok(()) => return Ok(()),
            Err(error) => last = error,
        }
    }
    Err(last)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn echo_rejects_wrong_identity_corruption_and_non_echo_packets() {
        let request = echo_request(false, &[42; 24]);
        let mut reply = request.clone();
        reply[0] = 0;
        reply[2..4].fill(0);
        let check = checksum(&reply);
        reply[2..4].copy_from_slice(&check.to_be_bytes());
        assert!(matches_echo(&reply, &request, false));
        reply[8] ^= 1;
        assert!(!matches_echo(&reply, &request, false));
        assert!(!matches_echo(&[3, 0], &request, false));
        assert!(!matches_echo(&[], &request, false));
        let request = echo_request(true, &[12; 24]);
        let mut reply = request.clone();
        reply[0] = 129;
        assert!(matches_echo(&reply, &request, true));
    }

    #[tokio::test]
    async fn udp_requires_an_actual_matching_response() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = server.local_addr().unwrap().to_string();
        let responder = tokio::spawn(async move {
            let mut buffer = [0; 64];
            let (length, peer) = server.recv_from(&mut buffer).await.unwrap();
            assert_eq!(&buffer[..length], b"ping");
            server.send_to(b"pong", peer).await.unwrap();
        });
        udp(
            &target,
            &serde_json::json!({"send":"70696e67","expect":"706f6e67","encoding":"hex"}),
        )
        .await
        .unwrap();
        responder.await.unwrap();
        let silent = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(100),
            udp(&silent.local_addr().unwrap().to_string(), &Value::Null)
        )
        .await
        .is_err());
    }
}
