// This file is part of Substrate.

// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: GPL-3.0-or-later WITH Classpath-exception-2.0

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

//! Behaviour tests for bounded delivery of notification events to protocol subscribers.

use super::*;
use crate::protocol::notifications::handler::NotificationsSinkMessage;

use futures::{channel::mpsc as sink_channel, FutureExt, Stream, StreamExt};
use prometheus_endpoint::Registry;
use std::collections::HashMap;

const PROTOCOL: &str = "/bounded-delivery/1";
const DROPPED_METRIC: &str = "substrate_sub_libp2p_notifications_dropped_total";
const NO_LIMIT: usize = usize::MAX / 4;
const KIB: usize = 1024;
const MIB: usize = 1024 * 1024;

fn scenario(text: &str) {
	println!("Scenario: {text}");
}

fn it_should(text: &str) {
	println!("It should {text}");
}

fn limits(
	max_notifications: usize,
	max_bytes: usize,
	max_notifications_per_peer: usize,
	max_bytes_per_peer: usize,
) -> NotificationBufferLimits {
	let mut limits = NotificationBufferLimits::default();
	limits.max_notifications = max_notifications;
	limits.max_bytes = max_bytes;
	limits.max_notifications_per_peer = max_notifications_per_peer;
	limits.max_bytes_per_peer = max_bytes_per_peer;
	limits
}

/// Payload layout: `[peer tag, epoch (u32 LE), sequence (u32 LE), padding...]`.
fn payload(peer_tag: u8, epoch: u32, seq: u32, size: usize) -> Vec<u8> {
	assert!(size >= 9, "test payloads carry a 9 byte header");
	let mut data = Vec::with_capacity(size);
	data.push(peer_tag);
	data.extend_from_slice(&epoch.to_le_bytes());
	data.extend_from_slice(&seq.to_le_bytes());
	data.resize(size, 0xAB);
	data
}

fn decode(data: &[u8]) -> (u8, u32, u32) {
	(
		data[0],
		u32::from_le_bytes(data[1..5].try_into().expect("4 bytes; qed")),
		u32::from_le_bytes(data[5..9].try_into().expect("4 bytes; qed")),
	)
}

/// Handshake layout: `[peer tag, epoch (u32 LE)]`.
fn handshake(peer_tag: u8, epoch: u32) -> Vec<u8> {
	let mut data = vec![peer_tag];
	data.extend_from_slice(&epoch.to_le_bytes());
	data
}

fn handshake_epoch(data: &[u8]) -> u32 {
	u32::from_le_bytes(data[1..5].try_into().expect("4 bytes; qed"))
}

/// Receiving ends of a peer's notification sink.
struct SinkEnd {
	sync_rx: sink_channel::Receiver<NotificationsSinkMessage>,
	_async_rx: sink_channel::Receiver<NotificationsSinkMessage>,
}

impl SinkEnd {
	/// Messages that were sent through this sink so far.
	fn sent(&mut self) -> Vec<Vec<u8>> {
		let mut out = Vec::new();
		while let Some(Some(message)) = self.sync_rx.next().now_or_never() {
			if let NotificationsSinkMessage::Notification { message } = message {
				out.push(message);
			}
		}
		out
	}
}

fn new_sink(peer: PeerId) -> (NotificationsSink, SinkEnd) {
	let (sink, async_rx, sync_rx) = NotificationsSink::new(peer);
	(sink, SinkEnd { sync_rx, _async_rx: async_rx })
}

/// What a subscriber observed, in a comparable form.
enum Seen {
	Validate { peer: PeerId, handshake: Vec<u8>, result_tx: oneshot::Sender<ValidationResult> },
	Opened { peer: PeerId, handshake: Vec<u8> },
	Closed { peer: PeerId },
	Data { peer: PeerId, payload: Vec<u8> },
}

impl Seen {
	fn peer(&self) -> PeerId {
		match self {
			Seen::Validate { peer, .. }
			| Seen::Opened { peer, .. }
			| Seen::Closed { peer }
			| Seen::Data { peer, .. } => *peer,
		}
	}

	fn is_lifecycle(&self) -> bool {
		matches!(self, Seen::Opened { .. } | Seen::Closed { .. })
	}

	/// Comparable summary without the validation answer channel.
	fn summary(&self) -> (u8, PeerId, Vec<u8>) {
		match self {
			Seen::Validate { peer, handshake, .. } => (0, *peer, handshake.clone()),
			Seen::Opened { peer, handshake } => (1, *peer, handshake.clone()),
			Seen::Closed { peer } => (2, *peer, Vec::new()),
			Seen::Data { peer, payload } => (3, *peer, payload.clone()),
		}
	}
}

fn convert(event: NotificationEvent) -> Seen {
	match event {
		NotificationEvent::ValidateInboundSubstream { peer, handshake, result_tx } => {
			Seen::Validate { peer: peer.into(), handshake, result_tx }
		},
		NotificationEvent::NotificationStreamOpened { peer, handshake, .. } => {
			Seen::Opened { peer: peer.into(), handshake }
		},
		NotificationEvent::NotificationStreamClosed { peer } => Seen::Closed { peer: peer.into() },
		NotificationEvent::NotificationReceived { peer, notification } => {
			Seen::Data { peer: peer.into(), payload: notification }
		},
	}
}

/// Reads everything currently available. Returns the events and whether the stream ended.
fn drain_raw(subscriber: &mut Box<dyn NotificationService>) -> (Vec<Seen>, bool) {
	let mut out = Vec::new();
	loop {
		match subscriber.next_event().now_or_never() {
			Some(Some(event)) => out.push(convert(event)),
			Some(None) => return (out, true),
			None => return (out, false),
		}
	}
}

fn drain(subscriber: &mut Box<dyn NotificationService>) -> Vec<Seen> {
	let (events, ended) = drain_raw(subscriber);
	it_should("keep the subscriber's event stream open");
	assert!(!ended, "the subscriber's event stream ended");
	events
}

fn data_of(events: &[Seen], peer: PeerId) -> Vec<Vec<u8>> {
	events
		.iter()
		.filter_map(|event| match event {
			Seen::Data { peer: p, payload } if *p == peer => Some(payload.clone()),
			_ => None,
		})
		.collect()
}

fn sequences(payloads: &[Vec<u8>]) -> Vec<u32> {
	payloads.iter().map(|p| decode(p).2).collect()
}

fn strictly_increasing(values: &[u32]) -> bool {
	values.windows(2).all(|w| w[0] < w[1])
}

fn dropped_count(registry: &Registry) -> u64 {
	registry
		.gather()
		.iter()
		.filter(|family| family.get_name() == DROPPED_METRIC)
		.flat_map(|family| family.get_metric().iter())
		.filter(|metric| {
			metric
				.get_label()
				.iter()
				.any(|label| label.get_name() == "protocol" && label.get_value() == PROTOCOL)
		})
		.map(|metric| metric.get_counter().get_value() as u64)
		.sum()
}

struct Harness {
	producer: ProtocolHandle,
	_commands: Box<dyn Stream<Item = NotificationCommand> + Send + Unpin>,
	subscriber: Box<dyn NotificationService>,
	registry: Registry,
}

impl Harness {
	fn new(limits: NotificationBufferLimits) -> Self {
		Self::from_parts(notification_service_with_limits(PROTOCOL.into(), limits))
	}

	fn with_defaults() -> Self {
		Self::from_parts(notification_service(PROTOCOL.into()))
	}

	fn from_parts(parts: (ProtocolHandlePair, Box<dyn NotificationService>)) -> Self {
		let (pair, subscriber) = parts;
		let registry = Registry::new();
		let (mut producer, commands) = pair.split();
		producer.set_metrics(NotificationMetrics::new(Some(&registry)));
		Self { producer, _commands: commands, subscriber, registry }
	}

	fn open(&mut self, peer: PeerId, handshake: Vec<u8>) -> SinkEnd {
		let (sink, end) = new_sink(peer);
		let _ =
			self.producer
				.report_substream_opened(peer, Direction::Inbound, handshake, None, sink);
		end
	}

	fn close(&mut self, peer: PeerId) {
		let _ = self.producer.report_substream_closed(peer);
	}

	fn data(&mut self, peer: PeerId, payload: Vec<u8>) {
		let _ = self.producer.report_notification_received(peer, payload);
	}

	fn replace_sink(&mut self, peer: PeerId) -> SinkEnd {
		let (sink, end) = new_sink(peer);
		let _ = self.producer.report_notification_sink_replaced(peer, sink);
		end
	}

	fn validate(&self, peer: PeerId, handshake: Vec<u8>) -> oneshot::Receiver<ValidationResult> {
		match self.producer.report_incoming_substream(peer, handshake) {
			Ok(ValidationCallResult::WaitForValidation(rx)) => rx,
			_ => panic!("validation must be forwarded to the subscriber"),
		}
	}

	fn send_to(&mut self, peer: PeerId, message: Vec<u8>) {
		self.subscriber.send_sync_notification(&peer.into(), message);
	}

	fn dropped(&self) -> u64 {
		dropped_count(&self.registry)
	}
}

fn answer(result: &mut oneshot::Receiver<ValidationResult>) -> Option<ValidationResult> {
	match result.try_recv() {
		Ok(answer) => Some(answer),
		Err(_) => None,
	}
}

#[test]
fn everything_is_delivered_exactly_when_limits_are_never_reached() {
	scenario(
		"several peers validate, connect, exchange notifications, replace their sink and \
		 reconnect while the subscriber reads regularly and limits are never reached",
	);
	let mut h = Harness::new(limits(1_000, MIB, 1_000, MIB));
	let peers: Vec<PeerId> = (0..4).map(|_| PeerId::random()).collect();
	let mut reported: HashMap<PeerId, Vec<(u8, PeerId, Vec<u8>)>> = HashMap::new();
	let mut observed: HashMap<PeerId, Vec<(u8, PeerId, Vec<u8>)>> = HashMap::new();
	let mut answers = 0usize;
	let mut ends: HashMap<PeerId, SinkEnd> = HashMap::new();

	let mut collect = |h: &mut Harness, answers: &mut usize| {
		for event in drain(&mut h.subscriber) {
			observed.entry(event.peer()).or_default().push(event.summary());
			if let Seen::Validate { result_tx, .. } = event {
				let _ = result_tx.send(ValidationResult::Accept);
				*answers += 1;
			}
		}
	};

	let mut validation = h.validate(peers[0], handshake(0, 0));
	reported.entry(peers[0]).or_default().push((0, peers[0], handshake(0, 0)));
	for (tag, peer) in peers.iter().enumerate() {
		ends.insert(*peer, h.open(*peer, handshake(tag as u8, 0)));
		reported.entry(*peer).or_default().push((1, *peer, handshake(tag as u8, 0)));
	}
	collect(&mut h, &mut answers);

	for seq in 0..50u32 {
		for (tag, peer) in peers.iter().enumerate() {
			let data = payload(tag as u8, 0, seq, 9 + (seq as usize * 7 + tag) % 90);
			reported.entry(*peer).or_default().push((3, *peer, data.clone()));
			h.data(*peer, data);
		}
		if seq == 20 {
			ends.insert(peers[1], h.replace_sink(peers[1]));
		}
		if seq % 7 == 0 {
			collect(&mut h, &mut answers);
		}
	}
	collect(&mut h, &mut answers);

	h.close(peers[2]);
	reported.entry(peers[2]).or_default().push((2, peers[2], Vec::new()));
	ends.insert(peers[2], h.open(peers[2], handshake(2, 1)));
	reported.entry(peers[2]).or_default().push((1, peers[2], handshake(2, 1)));
	for seq in 0..10u32 {
		let data = payload(2, 1, seq, 20);
		reported.entry(peers[2]).or_default().push((3, peers[2], data.clone()));
		h.data(peers[2], data);
	}
	collect(&mut h, &mut answers);

	it_should("deliver every reported event of every peer exactly once and in reported order");
	for peer in &peers {
		assert_eq!(observed.get(peer), reported.get(peer), "per-peer event sequence differs");
	}

	it_should("let the validation answer reach the network");
	assert_eq!(answers, 1);
	assert_eq!(answer(&mut validation), Some(ValidationResult::Accept));

	it_should("send through the latest sink of each connected peer");
	for peer in &peers {
		h.send_to(*peer, vec![7, 7, 7]);
		assert_eq!(ends.get_mut(peer).expect("sink exists; qed").sent(), vec![vec![7, 7, 7]]);
	}

	it_should("report no discarded notifications");
	assert_eq!(h.dropped(), 0);
}

#[test]
fn one_peer_cannot_buffer_more_notifications_than_its_quota() {
	scenario("one peer floods notifications while another sends a few and nobody reads");
	let mut h = Harness::new(limits(1_000, NO_LIMIT, 10, NO_LIMIT));
	let flooder = PeerId::random();
	let polite = PeerId::random();
	let _f = h.open(flooder, handshake(1, 0));
	let _p = h.open(polite, handshake(2, 0));

	for seq in 0..100u32 {
		h.data(flooder, payload(1, 0, seq, 16));
		if seq % 20 == 0 {
			h.data(polite, payload(2, 0, seq / 20, 16));
		}
	}
	let events = drain(&mut h.subscriber);
	let flooded = sequences(&data_of(&events, flooder));
	let polite_seen = sequences(&data_of(&events, polite));

	it_should("retain exactly the flooding peer's quota of notifications, in order");
	assert_eq!(flooded.len(), 10);
	assert!(strictly_increasing(&flooded));
	assert!(flooded.iter().all(|seq| *seq < 100));

	it_should("still deliver every notification of the peer within its quota");
	assert_eq!(polite_seen, vec![0, 1, 2, 3, 4]);

	it_should("count every discarded notification");
	assert_eq!(h.dropped(), 90);

	it_should("accept the flooding peer's notifications again once the subscriber has read");
	for seq in 100..103u32 {
		h.data(flooder, payload(1, 0, seq, 16));
	}
	let events = drain(&mut h.subscriber);
	assert_eq!(sequences(&data_of(&events, flooder)), vec![100, 101, 102]);
	assert_eq!(h.dropped(), 90);
}

#[test]
fn subscriber_never_buffers_more_notifications_than_its_total_limit() {
	scenario("three peers send notifications in turn while nobody reads");
	let mut h = Harness::new(limits(50, NO_LIMIT, 1_000, NO_LIMIT));
	let peers: Vec<PeerId> = (0..3).map(|_| PeerId::random()).collect();
	let _ends: Vec<_> = peers
		.iter()
		.enumerate()
		.map(|(t, p)| h.open(*p, handshake(t as u8, 0)))
		.collect();

	for seq in 0..40u32 {
		for (tag, peer) in peers.iter().enumerate() {
			h.data(*peer, payload(tag as u8, 0, seq, 12));
		}
	}
	let events = drain(&mut h.subscriber);

	it_should("retain exactly as many notifications as the total limit allows");
	let total: usize = peers.iter().map(|p| data_of(&events, *p).len()).sum();
	assert_eq!(total, 50);

	it_should("keep each peer's retained notifications in reported order");
	for peer in &peers {
		assert!(strictly_increasing(&sequences(&data_of(&events, *peer))));
	}

	it_should("count every discarded notification");
	assert_eq!(h.dropped(), 70);
}

#[test]
fn byte_limits_bound_buffered_payload() {
	scenario("peers send notifications of equal size until per-peer and total byte limits apply");

	let mut per_peer = Harness::new(limits(1_000, NO_LIMIT, 1_000, 1_000));
	let peer = PeerId::random();
	let _end = per_peer.open(peer, handshake(1, 0));
	for seq in 0..50u32 {
		per_peer.data(peer, payload(1, 0, seq, 100));
	}
	let events = drain(&mut per_peer.subscriber);
	it_should("retain exactly as many notifications as fit in the peer's byte quota");
	assert_eq!(data_of(&events, peer).len(), 10);
	assert!(strictly_increasing(&sequences(&data_of(&events, peer))));
	assert_eq!(per_peer.dropped(), 40);

	let mut total = Harness::new(limits(1_000, 2_500, 1_000, NO_LIMIT));
	let peers: Vec<PeerId> = (0..3).map(|_| PeerId::random()).collect();
	let _ends: Vec<_> = peers
		.iter()
		.enumerate()
		.map(|(t, p)| total.open(*p, handshake(t as u8, 0)))
		.collect();
	for seq in 0..20u32 {
		for (tag, peer) in peers.iter().enumerate() {
			total.data(*peer, payload(tag as u8, 0, seq, 100));
		}
	}
	let events = drain(&mut total.subscriber);
	it_should("retain exactly as many notifications as fit in the subscriber's byte limit");
	let retained: usize = peers.iter().map(|p| data_of(&events, *p).len()).sum();
	assert_eq!(retained, 25);
	assert_eq!(total.dropped(), 35);
}

#[test]
fn variable_sized_notifications_fill_the_byte_limit_without_exceeding_it() {
	scenario("one peer sends many notifications of varying size while nobody reads");
	let mut h = Harness::new(limits(NO_LIMIT, 5_000, NO_LIMIT, NO_LIMIT));
	let peer = PeerId::random();
	let _end = h.open(peer, handshake(1, 0));
	let mut rng = Rng::new(7);
	for seq in 0..500u32 {
		h.data(peer, payload(1, 0, seq, 9 + rng.below(292) as usize));
	}
	let events = drain(&mut h.subscriber);
	let retained = data_of(&events, peer);
	let bytes: usize = retained.iter().map(Vec::len).sum();

	it_should("never retain more payload than the byte limit");
	assert!(bytes <= 5_000, "retained {bytes} bytes");

	it_should("keep the buffer filled up to less than one notification below the limit");
	assert!(bytes > 5_000 - 300, "retained only {bytes} bytes");

	it_should("keep retained notifications in order and count every discarded one");
	assert!(strictly_increasing(&sequences(&retained)));
	assert_eq!(retained.len() as u64 + h.dropped(), 500);
}

#[test]
fn oversized_notification_is_discarded_without_blocking_smaller_ones() {
	scenario("a peer sends a notification larger than its byte quota followed by a small one");
	let mut h = Harness::new(limits(1_000, NO_LIMIT, 1_000, 1_000));
	let peer = PeerId::random();
	let _end = h.open(peer, handshake(1, 0));
	h.data(peer, payload(1, 0, 0, 1_500));
	h.data(peer, payload(1, 0, 1, 100));
	let events = drain(&mut h.subscriber);

	it_should("discard only the oversized notification");
	assert_eq!(sequences(&data_of(&events, peer)), vec![1]);
	assert_eq!(h.dropped(), 1);
}

#[test]
fn empty_notifications_are_limited_by_count() {
	scenario("a peer floods empty notifications while nobody reads");
	let mut h = Harness::new(limits(1_000, 10, 7, 10));
	let peer = PeerId::random();
	let _end = h.open(peer, handshake(1, 0));
	for _ in 0..100 {
		h.data(peer, Vec::new());
	}
	let events = drain(&mut h.subscriber);

	it_should("retain exactly the peer's notification count quota");
	assert_eq!(data_of(&events, peer).len(), 7);
	assert_eq!(h.dropped(), 93);
}

#[test]
fn validation_requests_bypass_data_limits() {
	scenario("two peers ask to be admitted while the buffer is full of notifications");
	let mut h = Harness::new(limits(20, NO_LIMIT, 20, NO_LIMIT));
	let full = PeerId::random();
	let accepted = PeerId::random();
	let rejected = PeerId::random();
	let _full_end = h.open(full, handshake(1, 0));
	drain(&mut h.subscriber);
	for seq in 0..20u32 {
		h.data(full, payload(1, 0, seq, 16));
	}
	let mut accept_answer = h.validate(accepted, handshake(3, 0));
	let mut reject_answer = h.validate(rejected, handshake(4, 0));
	let events = drain(&mut h.subscriber);

	it_should("deliver both admission requests with their handshakes");
	let requests: Vec<(PeerId, Vec<u8>)> = events
		.iter()
		.filter_map(|e| match e {
			Seen::Validate { peer, handshake, .. } => Some((*peer, handshake.clone())),
			_ => None,
		})
		.collect();
	assert_eq!(requests.len(), 2);
	assert!(requests.contains(&(accepted, handshake(3, 0))));
	assert!(requests.contains(&(rejected, handshake(4, 0))));

	it_should("still deliver every buffered notification");
	assert_eq!(sequences(&data_of(&events, full)), (0..20).collect::<Vec<_>>());

	for event in events {
		if let Seen::Validate { peer, result_tx, .. } = event {
			let verdict =
				if peer == accepted { ValidationResult::Accept } else { ValidationResult::Reject };
			let _ = result_tx.send(verdict);
		}
	}
	it_should("let each admission answer reach the network");
	assert_eq!(answer(&mut accept_answer), Some(ValidationResult::Accept));
	assert_eq!(answer(&mut reject_answer), Some(ValidationResult::Reject));
}

#[test]
fn connections_and_disconnections_bypass_data_limits() {
	scenario(
		"the buffer is full of one peer's notifications when another peer connects, sends, \
		 replaces its sink, and the first peer disconnects",
	);
	let mut h = Harness::new(limits(20, NO_LIMIT, 20, NO_LIMIT));
	let full = PeerId::random();
	let newcomer = PeerId::random();
	let _full_end = h.open(full, handshake(1, 0));
	drain(&mut h.subscriber);
	for seq in 0..20u32 {
		h.data(full, payload(1, 0, seq, 16));
	}
	let mut first = h.open(newcomer, handshake(2, 0));
	h.data(newcomer, payload(2, 0, 0, 16));
	let mut latest = h.replace_sink(newcomer);
	h.close(full);
	let events = drain(&mut h.subscriber);

	it_should("deliver the newcomer's connection although the data limit was reached");
	assert_eq!(lifecycle_of(&events, newcomer), vec![(1, handshake(2, 0))]);

	it_should("deliver the disconnection after all of that connection's notifications");
	let close_at = events
		.iter()
		.position(|e| matches!(e, Seen::Closed { peer } if *peer == full))
		.expect("the disconnection must be delivered");
	let last_data_at = events
		.iter()
		.rposition(|e| matches!(e, Seen::Data { peer, .. } if *peer == full))
		.expect("the notifications must be delivered");
	assert!(last_data_at < close_at);
	assert_eq!(sequences(&data_of(&events, full)), (0..20).collect::<Vec<_>>());
	assert_eq!(lifecycle_of(&events, full), vec![(2, Vec::new())]);

	it_should("discard only the notification that exceeded the data limit");
	assert!(data_of(&events, newcomer).is_empty());
	assert_eq!(h.dropped(), 1);

	it_should("send to the newcomer through its replaced sink only");
	h.send_to(newcomer, vec![9]);
	assert_eq!(latest.sent(), vec![vec![9]]);
	assert!(first.sent().is_empty());

	it_should("accept the newcomer's notifications once space is available");
	h.data(newcomer, payload(2, 0, 1, 16));
	let events = drain(&mut h.subscriber);
	assert_eq!(sequences(&data_of(&events, newcomer)), vec![1]);
}

#[test]
fn lifecycle_events_do_not_consume_data_capacity() {
	scenario("many peers validate and connect before sending notifications into a small buffer");
	let mut h = Harness::new(limits(3, NO_LIMIT, 3, NO_LIMIT));
	let peers: Vec<PeerId> = (0..10).map(|_| PeerId::random()).collect();
	let _answers: Vec<_> = peers.iter().map(|p| h.validate(*p, handshake(0, 0))).collect();
	let _ends: Vec<_> = peers.iter().map(|p| h.open(*p, handshake(0, 0))).collect();
	for (seq, peer) in peers.iter().take(3).enumerate() {
		h.data(*peer, payload(0, 0, seq as u32, 16));
	}
	let events = drain(&mut h.subscriber);

	it_should("deliver every validation request and every connection");
	assert_eq!(events.iter().filter(|e| matches!(e, Seen::Validate { .. })).count(), 10);
	assert_eq!(events.iter().filter(|e| matches!(e, Seen::Opened { .. })).count(), 10);

	it_should("still deliver as many notifications as the data limit allows");
	assert_eq!(events.iter().filter(|e| matches!(e, Seen::Data { .. })).count(), 3);
	assert_eq!(h.dropped(), 0);
}

fn lifecycle_of(events: &[Seen], peer: PeerId) -> Vec<(u8, Vec<u8>)> {
	events
		.iter()
		.filter(|e| e.peer() == peer && e.is_lifecycle())
		.map(|e| {
			let (kind, _, hs) = e.summary();
			(kind, hs)
		})
		.collect()
}

const CHURN: u32 = 10_000;

#[test]
fn reconnect_churn_of_an_observed_peer_stays_bounded() {
	scenario("a connected peer reconnects ten thousand times while the subscriber does not read");
	let mut h = Harness::new(limits(100, NO_LIMIT, 100, NO_LIMIT));
	let peer = PeerId::random();
	let mut original = h.open(peer, handshake(1, 0));
	drain(&mut h.subscriber);
	let mut previous = None;
	let mut latest = None;
	for epoch in 1..=CHURN {
		h.close(peer);
		previous = latest.take();
		latest = Some(h.open(peer, handshake(1, epoch)));
		h.data(peer, payload(1, epoch, 0, 16));
	}
	let events = drain(&mut h.subscriber);
	let lifecycle = lifecycle_of(&events, peer);

	it_should("buffer at most one disconnection and one connection for the peer");
	assert!(lifecycle.len() <= 2, "{} lifecycle events buffered", lifecycle.len());

	it_should("report the disconnection of the observed connection before the newest one");
	assert_eq!(lifecycle, vec![(2, Vec::new()), (1, handshake(1, CHURN))]);

	it_should("deliver exactly the newest connection's notifications, after it opened");
	let opened_at = events
		.iter()
		.position(|e| matches!(e, Seen::Opened { .. }))
		.expect("newest connection is delivered; qed");
	let data_at = events
		.iter()
		.position(|e| matches!(e, Seen::Data { .. }))
		.expect("newest connection's notification is delivered; qed");
	assert!(data_at > opened_at);
	assert_eq!(data_of(&events, peer), vec![payload(1, CHURN, 0, 16)]);

	it_should("send only through the newest connection's sink");
	h.send_to(peer, vec![5]);
	assert_eq!(latest.as_mut().expect("set; qed").sent(), vec![vec![5]]);
	assert!(previous.as_mut().expect("set; qed").sent().is_empty());
	assert!(original.sent().is_empty());
}

#[test]
fn reconnect_churn_ending_disconnected_leaves_the_peer_disconnected() {
	scenario("a connected peer reconnects many times and finally disconnects while unread");
	let mut h = Harness::new(limits(100, NO_LIMIT, 100, NO_LIMIT));
	let peer = PeerId::random();
	let _original = h.open(peer, handshake(1, 0));
	drain(&mut h.subscriber);
	let mut latest = None;
	for epoch in 1..=CHURN {
		h.close(peer);
		latest = Some(h.open(peer, handshake(1, epoch)));
		h.data(peer, payload(1, epoch, 0, 16));
	}
	h.close(peer);
	let events = drain(&mut h.subscriber);
	let lifecycle = lifecycle_of(&events, peer);

	it_should("buffer only the disconnection of the observed connection");
	assert!(lifecycle.len() <= 2, "{} lifecycle events buffered", lifecycle.len());
	assert_eq!(lifecycle, vec![(2, Vec::new())]);

	it_should("not deliver notifications of connections that were never observed");
	assert!(data_of(&events, peer).is_empty());

	it_should("no longer offer a way to send to the peer");
	assert!(h.subscriber.message_sink(&peer.into()).is_none());
	h.send_to(peer, vec![5]);
	assert!(latest.as_mut().expect("set; qed").sent().is_empty());
}

#[test]
fn reconnect_churn_of_an_unobserved_peer_stays_bounded() {
	scenario("a peer the subscriber never saw connects and reconnects many times while unread");
	let mut h = Harness::new(limits(100, NO_LIMIT, 100, NO_LIMIT));
	let peer = PeerId::random();
	let mut latest = h.open(peer, handshake(1, 0));
	for epoch in 1..=CHURN {
		h.data(peer, payload(1, epoch - 1, 0, 16));
		h.close(peer);
		latest = h.open(peer, handshake(1, epoch));
	}
	let events = drain(&mut h.subscriber);
	let lifecycle = lifecycle_of(&events, peer);

	it_should("buffer at most two lifecycle events for the peer");
	assert!(lifecycle.len() <= 2, "{} lifecycle events buffered", lifecycle.len());

	it_should("deliver only the newest connection");
	assert_eq!(lifecycle, vec![(1, handshake(1, CHURN))]);

	it_should("not deliver notifications of connections that were never observed");
	assert!(data_of(&events, peer).is_empty());

	it_should("send through the newest connection's sink");
	h.send_to(peer, vec![1]);
	assert_eq!(latest.sent(), vec![vec![1]]);
}

#[test]
fn sink_replacements_before_the_connection_is_observed_are_honoured() {
	scenario("a peer's sink is replaced twice before the subscriber reads its connection");
	let mut h = Harness::new(limits(2, NO_LIMIT, 2, NO_LIMIT));
	let peer = PeerId::random();
	let mut first = h.open(peer, handshake(1, 0));
	for seq in 0..10u32 {
		h.data(peer, payload(1, 0, seq, 16));
	}
	let mut second = h.replace_sink(peer);
	let mut third = h.replace_sink(peer);
	drain(&mut h.subscriber);

	it_should("send through the most recently reported sink");
	h.send_to(peer, vec![3]);
	assert_eq!(third.sent(), vec![vec![3]]);
	assert!(first.sent().is_empty());
	assert!(second.sent().is_empty());

	it_should("expose the most recently reported sink to protocols that send directly");
	let sink = h.subscriber.message_sink(&peer.into()).expect("peer is connected; qed");
	sink.send_sync_notification(vec![4]);
	assert_eq!(third.sent(), vec![vec![4]]);
}

#[test]
fn subscribers_are_bounded_independently_and_survive_overflow() {
	scenario(
		"two subscribers of the same protocol, one reading after every notification and one \
		 never reading, then the reading one goes away",
	);
	let (pair, mut reader) =
		notification_service_with_limits(PROTOCOL.into(), limits(1_000, NO_LIMIT, 10, NO_LIMIT));
	it_should("allow an additional subscriber");
	let idle = NotificationService::clone(reader.as_mut());
	assert!(idle.is_ok());
	let mut idle = idle.expect("checked; qed");
	let registry = Registry::new();
	let (mut producer, _commands) = pair.split();
	producer.set_metrics(NotificationMetrics::new(Some(&registry)));

	let peer = PeerId::random();
	let (sink, _end) = new_sink(peer);
	let _ = producer.report_substream_opened(peer, Direction::Inbound, handshake(1, 0), None, sink);

	let mut read = Vec::new();
	for seq in 0..100u32 {
		let _ = producer.report_notification_received(peer, payload(1, 0, seq, 16));
		read.extend(data_of(&drain(&mut reader), peer));
	}
	let idle_events = drain(&mut idle);

	it_should("deliver every notification to the reading subscriber");
	assert_eq!(sequences(&read), (0..100).collect::<Vec<_>>());

	it_should("bound only the idle subscriber's buffer");
	assert_eq!(data_of(&idle_events, peer).len(), 10);
	assert!(strictly_increasing(&sequences(&data_of(&idle_events, peer))));

	it_should("count only the idle subscriber's discarded notifications");
	assert_eq!(dropped_count(&registry), 90);

	it_should("keep delivering to the idle subscriber after the other subscriber is dropped");
	drop(reader);
	for seq in 100..105u32 {
		let _ = producer.report_notification_received(peer, payload(1, 0, seq, 16));
	}
	let events = drain(&mut idle);
	assert_eq!(sequences(&data_of(&events, peer)), vec![100, 101, 102, 103, 104]);
}

#[test]
fn default_limits_are_finite_and_usable() {
	scenario("a protocol uses the default limits");
	let defaults = NotificationBufferLimits::default();

	it_should("use default limits within the required ranges");
	assert!((1_024..=100_000).contains(&defaults.max_notifications));
	assert!((MIB..=64 * MIB).contains(&defaults.max_bytes));
	assert!((256..=defaults.max_notifications).contains(&defaults.max_notifications_per_peer));
	assert!((256 * KIB..=defaults.max_bytes).contains(&defaults.max_bytes_per_peer));

	let mut small = Harness::with_defaults();
	let peers: Vec<PeerId> = (0..4).map(|_| PeerId::random()).collect();
	let _ends: Vec<_> = peers
		.iter()
		.enumerate()
		.map(|(t, p)| small.open(*p, handshake(t as u8, 0)))
		.collect();
	for seq in 0..256u32 {
		for (tag, peer) in peers.iter().enumerate() {
			small.data(*peer, payload(tag as u8, 0, seq, KIB));
		}
	}
	let events = drain(&mut small.subscriber);
	it_should("deliver a moderate burst from several peers in full");
	for peer in &peers {
		assert_eq!(sequences(&data_of(&events, *peer)), (0..256).collect::<Vec<_>>());
	}
	assert_eq!(small.dropped(), 0);

	let mut flood = Harness::with_defaults();
	let peer = PeerId::random();
	let _end = flood.open(peer, handshake(1, 0));
	let sent = 150_000u32;
	let mut reported = 0u32;
	for seq in 0..sent {
		flood.data(peer, payload(1, 0, seq, 9));
		reported += 1;
	}
	it_should("accept every report without waiting for the subscriber");
	assert_eq!(reported, sent);
	let events = drain(&mut flood.subscriber);
	let expected = defaults
		.max_notifications_per_peer
		.min(defaults.max_notifications)
		.min(defaults.max_bytes_per_peer / 9)
		.min(defaults.max_bytes / 9);
	it_should("retain exactly what the default limits allow from a flooding peer");
	assert_eq!(data_of(&events, peer).len(), expected);
	assert_eq!(flood.dropped(), sent as u64 - expected as u64);

	let mut large = Harness::with_defaults();
	let peer = PeerId::random();
	let _end = large.open(peer, handshake(1, 0));
	for seq in 0..70u32 {
		large.data(peer, payload(1, 0, seq, MIB));
	}
	let events = drain(&mut large.subscriber);
	let expected = (defaults.max_bytes_per_peer / MIB)
		.min(defaults.max_bytes / MIB)
		.min(defaults.max_notifications_per_peer);
	it_should("retain exactly what the default byte limits allow from large notifications");
	assert_eq!(data_of(&events, peer).len(), expected);
	assert!(data_of(&events, peer).len() * MIB <= 64 * MIB);
}

/// Small deterministic pseudo random generator (xorshift64*).
struct Rng(u64);

impl Rng {
	fn new(seed: u64) -> Self {
		Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
	}

	fn next(&mut self) -> u64 {
		self.0 ^= self.0 >> 12;
		self.0 ^= self.0 << 25;
		self.0 ^= self.0 >> 27;
		self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
	}

	fn below(&mut self, bound: u64) -> u64 {
		self.next() % bound
	}
}

/// Producer-side truth about a peer in the randomized model.
#[derive(Default)]
struct ActualPeer {
	connected: bool,
	epoch: u32,
	seq: u32,
	latest_sink: Option<SinkEnd>,
	older_sinks: Vec<SinkEnd>,
}

/// Subscriber-side view of a peer in the randomized model.
#[derive(Default, Clone)]
struct ObservedPeer {
	connected: bool,
	epoch: Option<u32>,
	last_seq: Option<u32>,
}

struct ModelChecker {
	limits: NotificationBufferLimits,
	observed: HashMap<PeerId, ObservedPeer>,
	tags: HashMap<PeerId, u8>,
}

impl ModelChecker {
	/// Checks a batch of events read in one go. Returns a violation description, if any.
	fn check_batch(&mut self, events: &[Seen], full_drain: bool) -> Result<(), String> {
		let mut count_total = 0usize;
		let mut bytes_total = 0usize;
		let mut per_peer: HashMap<PeerId, (usize, usize, usize)> = HashMap::new();
		for event in events {
			let view = self.observed.entry(event.peer()).or_default();
			let tag = self.tags[&event.peer()];
			match event {
				Seen::Opened { handshake: hs, .. } => {
					if view.connected {
						return Err("two connections without a disconnection in between".into());
					}
					if hs[0] != tag {
						return Err("connection reported for the wrong peer".into());
					}
					view.connected = true;
					view.epoch = Some(handshake_epoch(hs));
					view.last_seq = None;
					per_peer.entry(event.peer()).or_default().2 += 1;
				},
				Seen::Closed { .. } => {
					if !view.connected {
						return Err("disconnection without an observed connection".into());
					}
					view.connected = false;
					per_peer.entry(event.peer()).or_default().2 += 1;
				},
				Seen::Data { payload: data, .. } => {
					if !view.connected {
						return Err("notification delivered while the peer is disconnected".into());
					}
					let (ptag, epoch, seq) = decode(data);
					if ptag != tag {
						return Err("notification attributed to the wrong peer".into());
					}
					if Some(epoch) != view.epoch {
						return Err("notification delivered within another connection".into());
					}
					if view.last_seq.map_or(false, |last| seq <= last) {
						return Err("notifications reordered or duplicated".into());
					}
					view.last_seq = Some(seq);
					count_total += 1;
					bytes_total += data.len();
					let entry = per_peer.entry(event.peer()).or_default();
					entry.0 += 1;
					entry.1 += data.len();
				},
				Seen::Validate { .. } => return Err("unexpected validation request".into()),
			}
		}
		if full_drain {
			if count_total > self.limits.max_notifications {
				return Err(format!("{count_total} notifications buffered"));
			}
			if bytes_total > self.limits.max_bytes {
				return Err(format!("{bytes_total} bytes buffered"));
			}
			for (count, bytes, lifecycle) in per_peer.values() {
				if *count > self.limits.max_notifications_per_peer {
					return Err(format!("{count} notifications buffered for one peer"));
				}
				if *bytes > self.limits.max_bytes_per_peer {
					return Err(format!("{bytes} bytes buffered for one peer"));
				}
				if *lifecycle > 2 {
					return Err(format!("{lifecycle} lifecycle events buffered for one peer"));
				}
			}
		}
		Ok(())
	}
}

fn run_model(seed: u64) -> Result<(), String> {
	let mut rng = Rng::new(seed);
	let lim = limits(
		3 + rng.below(10) as usize,
		50 + rng.below(350) as usize,
		2 + rng.below(7) as usize,
		30 + rng.below(220) as usize,
	);
	let mut h = Harness::new(lim.clone());
	let peers: Vec<PeerId> = (0..3).map(|_| PeerId::random()).collect();
	let mut actual: HashMap<PeerId, ActualPeer> = HashMap::new();
	let mut checker = ModelChecker {
		limits: lim,
		observed: HashMap::new(),
		tags: peers.iter().enumerate().map(|(t, p)| (*p, t as u8)).collect(),
	};
	let mut ended = false;

	for _ in 0..300 {
		let peer = peers[rng.below(peers.len() as u64) as usize];
		let tag = checker.tags[&peer];
		let state = actual.entry(peer).or_default();
		match rng.below(100) {
			0..=11 => {
				if state.connected {
					h.close(peer);
					state.connected = false;
				} else {
					state.epoch += 1;
					state.seq = 0;
					state.connected = true;
					if let Some(old) = state.latest_sink.take() {
						state.older_sinks.push(old);
					}
					state.latest_sink = Some(h.open(peer, handshake(tag, state.epoch)));
				}
			},
			12..=16 => {
				if state.connected {
					if let Some(old) = state.latest_sink.take() {
						state.older_sinks.push(old);
					}
					state.latest_sink = Some(h.replace_sink(peer));
				}
			},
			17..=79 => {
				if state.connected {
					let size = 9 + rng.below(52) as usize;
					h.data(peer, payload(tag, state.epoch, state.seq, size));
					state.seq += 1;
				}
			},
			80..=91 => {
				let (events, stream_ended) = drain_raw(&mut h.subscriber);
				ended |= stream_ended;
				checker.check_batch(&events, true)?;
			},
			_ => {
				let mut events = Vec::new();
				for _ in 0..rng.below(4) {
					match h.subscriber.next_event().now_or_never() {
						Some(Some(event)) => events.push(convert(event)),
						Some(None) => ended = true,
						None => break,
					}
				}
				checker.check_batch(&events, false)?;
			},
		}
	}

	let (events, stream_ended) = drain_raw(&mut h.subscriber);
	ended |= stream_ended;
	checker.check_batch(&events, true)?;
	if ended {
		return Err("the subscriber's event stream ended".into());
	}

	for peer in &peers {
		let state = actual.entry(*peer).or_default();
		let view = checker.observed.get(peer).cloned().unwrap_or_default();
		if view.connected != state.connected {
			return Err("final connection state differs from the reported one".into());
		}
		if state.connected && view.epoch != Some(state.epoch) {
			return Err("final connection differs from the newest one".into());
		}
		h.subscriber.send_sync_notification(&(*peer).into(), vec![0xEE]);
		let latest = state.latest_sink.as_mut().map(|s| s.sent()).unwrap_or_default();
		for older in state.older_sinks.iter_mut() {
			if !older.sent().is_empty() {
				return Err("sent through an outdated sink".into());
			}
		}
		if state.connected && latest != vec![vec![0xEE]] {
			return Err("did not send through the newest sink".into());
		}
		if !state.connected && !latest.is_empty() {
			return Err("sent to a disconnected peer".into());
		}
	}
	Ok(())
}

#[test]
fn randomized_peer_activity_keeps_every_delivery_guarantee() {
	scenario(
		"three peers connect, disconnect, replace sinks and send notifications at random while \
		 the subscriber reads at random moments, over many independent runs",
	);
	let failures: Vec<(u64, String)> = (0..400u64)
		.filter_map(|seed| run_model(seed).err().map(|error| (seed, error)))
		.collect();

	it_should(
		"respect limits, per-peer ordering, connection consistency, bounded lifecycle \
		 buffering and the latest sink in every run",
	);
	assert!(failures.is_empty(), "violations (seed, description): {failures:?}");
}
