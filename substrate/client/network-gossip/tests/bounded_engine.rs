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

//! Behaviour tests for the gossip engine under slow, idle and fast subscribers.

use codec::Encode;
use futures::{
	channel::mpsc::{unbounded, Receiver, UnboundedReceiver, UnboundedSender},
	future::FusedFuture,
	FutureExt, Stream, StreamExt,
};
use sc_network::{
	config::MultiaddrWithPeerId,
	service::traits::{
		Direction, MessageSink, NotificationEvent, NotificationService,
		ValidationResult as SubstreamVerdict,
	},
	types::ProtocolName,
	Event, NetworkBlock, NetworkEventStream, NetworkPeers, ReputationChange, Roles,
};
use sc_network_common::role::ObservedRole;
use sc_network_gossip::{
	GossipEngine, MessageIntent, TopicNotification, ValidationResult, Validator, ValidatorContext,
};
use sc_network_sync::{SyncEvent, SyncEventStream};
use sc_network_types::{multiaddr::Multiaddr, PeerId};
use sp_runtime::{
	testing::H256,
	traits::{Block as BlockT, NumberFor},
};
use std::{
	collections::{HashMap, HashSet},
	pin::Pin,
	sync::{
		atomic::{AtomicBool, AtomicUsize, Ordering},
		Arc, Mutex, RwLock,
	},
	task::{Context, Poll, Wake, Waker},
	time::Duration,
};
use substrate_test_runtime_client::runtime::Block;

const POLL_BUDGET: usize = 1_024;
const IDLE_BACKLOG: usize = 1_024;
const DISCARDED_TOPIC: u8 = 0xFF;

fn scenario(text: &str) {
	println!("Scenario: {text}");
}

fn it_should(text: &str) {
	println!("It should {text}");
}

// ---------------------------------------------------------------------------------------------
// Messages and validators
// ---------------------------------------------------------------------------------------------

/// Message layout: `[topic tag, sender tag, sequence (u32 LE)]`.
fn message(topic: u8, sender: u8, seq: u32) -> Vec<u8> {
	let mut data = vec![topic, sender];
	data.extend_from_slice(&seq.to_le_bytes());
	data
}

fn parts(data: &[u8]) -> (u8, u8, u32) {
	(data[0], data[1], u32::from_le_bytes(data[2..6].try_into().expect("4 bytes; qed")))
}

fn topic(tag: u8) -> H256 {
	H256::repeat_byte(tag)
}

/// Keeps every message under the topic named by its first byte, except the discarded topic.
#[derive(Default)]
struct TopicValidator {
	connected: Mutex<Vec<PeerId>>,
	disconnected: Mutex<Vec<PeerId>>,
}

impl Validator<Block> for TopicValidator {
	fn new_peer(&self, _: &mut dyn ValidatorContext<Block>, who: &PeerId, _: ObservedRole) {
		self.connected.lock().expect("not poisoned; qed").push(*who);
	}

	fn peer_disconnected(&self, _: &mut dyn ValidatorContext<Block>, who: &PeerId) {
		self.disconnected.lock().expect("not poisoned; qed").push(*who);
	}

	fn validate(
		&self,
		_: &mut dyn ValidatorContext<Block>,
		_: &PeerId,
		data: &[u8],
	) -> ValidationResult<H256> {
		match data.first() {
			Some(tag) if *tag != DISCARDED_TOPIC => ValidationResult::ProcessAndKeep(topic(*tag)),
			_ => ValidationResult::Discard,
		}
	}
}

// ---------------------------------------------------------------------------------------------
// Network, sync and notification doubles
// ---------------------------------------------------------------------------------------------

#[derive(Default)]
struct Shared {
	reports: Mutex<Vec<(PeerId, i32)>>,
	reserved_added: Mutex<Vec<String>>,
	reserved_removed: Mutex<Vec<PeerId>>,
	sync_taken: AtomicUsize,
}

#[derive(Clone)]
struct TestNetwork {
	shared: Arc<Shared>,
}

#[async_trait::async_trait]
impl NetworkPeers for TestNetwork {
	fn set_authorized_peers(&self, _: HashSet<PeerId>) {}

	fn set_authorized_only(&self, _: bool) {}

	fn add_known_address(&self, _: PeerId, _: Multiaddr) {}

	fn report_peer(&self, peer_id: PeerId, cost_benefit: ReputationChange) {
		self.shared
			.reports
			.lock()
			.expect("not poisoned; qed")
			.push((peer_id, cost_benefit.value));
	}

	fn peer_reputation(&self, _: &PeerId) -> i32 {
		0
	}

	fn disconnect_peer(&self, _: PeerId, _: ProtocolName) {}

	fn accept_unreserved_peers(&self) {}

	fn deny_unreserved_peers(&self) {}

	fn add_reserved_peer(&self, _: MultiaddrWithPeerId) -> Result<(), String> {
		Ok(())
	}

	fn remove_reserved_peer(&self, _: PeerId) {}

	fn set_reserved_peers(&self, _: ProtocolName, _: HashSet<Multiaddr>) -> Result<(), String> {
		Ok(())
	}

	fn add_peers_to_reserved_set(
		&self,
		_: ProtocolName,
		peers: HashSet<Multiaddr>,
	) -> Result<(), String> {
		let mut added = self.shared.reserved_added.lock().expect("not poisoned; qed");
		added.extend(peers.iter().map(|addr| addr.to_string()));
		Ok(())
	}

	fn remove_peers_from_reserved_set(
		&self,
		_: ProtocolName,
		peers: Vec<PeerId>,
	) -> Result<(), String> {
		self.shared.reserved_removed.lock().expect("not poisoned; qed").extend(peers);
		Ok(())
	}

	fn sync_num_connected(&self) -> usize {
		0
	}

	fn peer_role(&self, _: PeerId, handshake: Vec<u8>) -> Option<ObservedRole> {
		<Roles as codec::DecodeAll>::decode_all(&mut &handshake[..])
			.ok()
			.map(ObservedRole::from)
	}

	async fn reserved_peers(&self) -> Result<Vec<PeerId>, ()> {
		Ok(Vec::new())
	}
}

impl NetworkEventStream for TestNetwork {
	fn event_stream(&self, _: &'static str) -> Pin<Box<dyn Stream<Item = Event> + Send>> {
		Box::pin(futures::stream::pending())
	}
}

impl NetworkBlock<<Block as BlockT>::Hash, NumberFor<Block>> for TestNetwork {
	fn announce_block(&self, _: <Block as BlockT>::Hash, _: Option<Vec<u8>>) {}

	fn new_best_block_imported(&self, _: <Block as BlockT>::Hash, _: NumberFor<Block>) {}
}

#[derive(Clone)]
struct TestSync {
	shared: Arc<Shared>,
	events: Arc<Mutex<Option<UnboundedReceiver<SyncEvent>>>>,
}

impl SyncEventStream for TestSync {
	fn event_stream(&self, _: &'static str) -> Pin<Box<dyn Stream<Item = SyncEvent> + Send>> {
		let shared = self.shared.clone();
		let events = self.events.lock().expect("not poisoned; qed").take();
		match events {
			Some(rx) => Box::pin(rx.inspect(move |_| {
				shared.sync_taken.fetch_add(1, Ordering::SeqCst);
			})),
			None => Box::pin(futures::stream::pending()),
		}
	}
}

impl NetworkBlock<<Block as BlockT>::Hash, NumberFor<Block>> for TestSync {
	fn announce_block(&self, _: <Block as BlockT>::Hash, _: Option<Vec<u8>>) {}

	fn new_best_block_imported(&self, _: <Block as BlockT>::Hash, _: NumberFor<Block>) {}
}

#[derive(Debug)]
struct TestNotifications {
	events: UnboundedReceiver<NotificationEvent>,
	taken: Arc<AtomicUsize>,
	protocol: ProtocolName,
}

#[async_trait::async_trait]
impl NotificationService for TestNotifications {
	async fn open_substream(&mut self, _: PeerId) -> Result<(), ()> {
		Ok(())
	}

	async fn close_substream(&mut self, _: PeerId) -> Result<(), ()> {
		Ok(())
	}

	fn send_sync_notification(&mut self, _: &PeerId, _: Vec<u8>) {}

	async fn send_async_notification(
		&mut self,
		_: &PeerId,
		_: Vec<u8>,
	) -> Result<(), sc_network::error::Error> {
		Ok(())
	}

	async fn set_handshake(&mut self, _: Vec<u8>) -> Result<(), ()> {
		Ok(())
	}

	fn try_set_handshake(&mut self, _: Vec<u8>) -> Result<(), ()> {
		Ok(())
	}

	async fn next_event(&mut self) -> Option<NotificationEvent> {
		let event = self.events.next().await;
		if event.is_some() {
			self.taken.fetch_add(1, Ordering::SeqCst);
		}
		event
	}

	fn clone(&mut self) -> Result<Box<dyn NotificationService>, ()> {
		Err(())
	}

	fn protocol(&self) -> &ProtocolName {
		&self.protocol
	}

	fn message_sink(&self, _: &PeerId) -> Option<Box<dyn MessageSink>> {
		None
	}
}

// ---------------------------------------------------------------------------------------------
// Driving the engine
// ---------------------------------------------------------------------------------------------

#[derive(Default)]
struct WakeFlag(AtomicBool);

impl Wake for WakeFlag {
	fn wake(self: Arc<Self>) {
		self.0.store(true, Ordering::SeqCst);
	}

	fn wake_by_ref(self: &Arc<Self>) {
		self.0.store(true, Ordering::SeqCst);
	}
}

/// A local consumer of one topic.
struct Subscriber {
	rx: Receiver<TopicNotification>,
	received: Vec<TopicNotification>,
	ended: bool,
}

impl Subscriber {
	fn new(rx: Receiver<TopicNotification>) -> Self {
		Self { rx, received: Vec::new(), ended: false }
	}

	/// Reads everything currently available; returns how many messages were read.
	fn read_available(&mut self) -> usize {
		let mut count = 0;
		loop {
			match self.rx.next().now_or_never() {
				Some(Some(notification)) => {
					self.received.push(notification);
					count += 1;
				},
				Some(None) => {
					self.ended = true;
					return count;
				},
				None => return count,
			}
		}
	}

	fn messages(&self) -> Vec<(u8, u8, u32)> {
		self.received.iter().map(|n| parts(&n.message)).collect()
	}
}

struct Harness {
	engine: GossipEngine<Block>,
	notifications: Option<UnboundedSender<NotificationEvent>>,
	sync: Option<UnboundedSender<SyncEvent>>,
	shared: Arc<Shared>,
	notifications_taken: Arc<AtomicUsize>,
	pushed: usize,
	wake: Arc<WakeFlag>,
	finished: bool,
}

/// Why driving the engine until nothing is left to do failed.
#[derive(Debug)]
#[allow(dead_code)]
enum DriveError {
	Finished,
	PollTookTooMuch(usize),
	Stalled { remaining: usize },
	BusyLoop,
	TooManyPolls { remaining: usize },
	SubscriberEnded,
}

impl Harness {
	fn new(validator: Arc<dyn Validator<Block>>) -> Self {
		let shared = Arc::new(Shared::default());
		let (notifications_tx, notifications_rx) = unbounded();
		let (sync_tx, sync_rx) = unbounded();
		let notifications_taken = Arc::new(AtomicUsize::new(0));
		let protocol = ProtocolName::from("/bounded-gossip/1");
		let engine = GossipEngine::<Block>::new(
			TestNetwork { shared: shared.clone() },
			TestSync { shared: shared.clone(), events: Arc::new(Mutex::new(Some(sync_rx))) },
			Box::new(TestNotifications {
				events: notifications_rx,
				taken: notifications_taken.clone(),
				protocol: protocol.clone(),
			}),
			protocol,
			validator,
			None,
		);
		Self {
			engine,
			notifications: Some(notifications_tx),
			sync: Some(sync_tx),
			shared,
			notifications_taken,
			pushed: 0,
			wake: Arc::new(WakeFlag::default()),
			finished: false,
		}
	}

	fn push(&mut self, event: NotificationEvent) {
		self.notifications
			.as_ref()
			.expect("notification stream open; qed")
			.unbounded_send(event)
			.expect("engine keeps its event source; qed");
		self.pushed += 1;
	}

	fn push_sync(&mut self, event: SyncEvent) {
		self.sync
			.as_ref()
			.expect("sync stream open; qed")
			.unbounded_send(event)
			.expect("engine keeps its sync source; qed");
		self.pushed += 1;
	}

	fn connect(&mut self, peer: PeerId) {
		self.push(NotificationEvent::NotificationStreamOpened {
			peer,
			direction: Direction::Inbound,
			handshake: Roles::FULL.encode(),
			negotiated_fallback: None,
		});
	}

	fn disconnect(&mut self, peer: PeerId) {
		self.push(NotificationEvent::NotificationStreamClosed { peer });
	}

	fn receive(&mut self, peer: PeerId, data: Vec<u8>) {
		self.push(NotificationEvent::NotificationReceived { peer, notification: data });
	}

	fn taken(&self) -> usize {
		self.notifications_taken.load(Ordering::SeqCst) +
			self.shared.sync_taken.load(Ordering::SeqCst)
	}

	fn remaining(&self) -> usize {
		self.pushed - self.taken()
	}

	/// Polls the engine once; returns how many inbound events that poll took.
	fn poll_once(&mut self) -> (Poll<()>, usize) {
		self.wake.0.store(false, Ordering::SeqCst);
		let waker = Waker::from(self.wake.clone());
		let mut cx = Context::from_waker(&waker);
		let before = self.taken();
		let result = self.engine.poll_unpin(&mut cx);
		if result.is_ready() {
			self.finished = true;
		}
		(result, self.taken() - before)
	}

	fn woken(&self) -> bool {
		self.wake.0.load(Ordering::SeqCst)
	}

	/// Polls the engine, letting `readers` read after every poll, until every inbound event was
	/// taken and the readers stop receiving messages. Once there is nothing left to do, the engine
	/// must stop scheduling itself.
	fn drive(
		&mut self,
		readers: &mut [&mut Subscriber],
		max_polls: usize,
	) -> Result<(), DriveError> {
		const QUIET_ROUNDS: usize = 5;
		let mut quiet_rounds = 0;
		let mut quiet_wakes = 0;
		for _ in 0..max_polls {
			let (result, took) = self.poll_once();
			if result.is_ready() {
				return Err(DriveError::Finished);
			}
			if took > POLL_BUDGET {
				return Err(DriveError::PollTookTooMuch(took));
			}
			let mut read = 0;
			for reader in readers.iter_mut() {
				read += reader.read_available();
				if reader.ended {
					return Err(DriveError::SubscriberEnded);
				}
			}
			let remaining = self.remaining();
			if remaining > 0 && !self.woken() {
				return Err(DriveError::Stalled { remaining });
			}
			if remaining == 0 && read == 0 {
				quiet_rounds += 1;
				// A timer may legitimately fire once; repeated wakes with nothing to do are a
				// busy loop.
				if self.woken() {
					quiet_wakes += 1;
				}
				if quiet_rounds >= QUIET_ROUNDS {
					return if quiet_wakes > 1 { Err(DriveError::BusyLoop) } else { Ok(()) };
				}
			} else {
				quiet_rounds = 0;
				quiet_wakes = 0;
			}
		}
		Err(DriveError::TooManyPolls { remaining: self.remaining() })
	}

	fn subscribe(&mut self, tag: u8) -> Subscriber {
		Subscriber::new(self.engine.messages_for(topic(tag)))
	}

	/// Reads everything a subscriber can still obtain, polling the engine in between.
	fn exhaust(&mut self, subscriber: &mut Subscriber) -> Result<(), DriveError> {
		self.drive(&mut [subscriber], 100_000)
	}

	fn negative_reports(&self, peer: PeerId) -> usize {
		self.shared
			.reports
			.lock()
			.expect("not poisoned; qed")
			.iter()
			.filter(|(p, value)| *p == peer && *value < 0)
			.count()
	}
}

fn per_sender_in_order(messages: &[(u8, u8, u32)]) -> bool {
	let mut last: HashMap<u8, u32> = HashMap::new();
	messages.iter().all(|(_, sender, seq)| {
		let ok = last.get(sender).map_or(true, |prev| seq > prev);
		last.insert(*sender, *seq);
		ok
	})
}

// ---------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------

#[test]
fn a_single_poll_does_bounded_work_and_asks_to_be_polled_again() {
	scenario("a peer floods fifty thousand messages for a topic nobody subscribed to");
	let mut h = Harness::new(Arc::new(TopicValidator::default()));
	let peer = PeerId::random();
	h.connect(peer);
	for seq in 0..50_000u32 {
		h.receive(peer, message(1, 1, seq));
	}

	let (result, took) = h.poll_once();
	it_should("take at most the per-poll budget of inbound events in one poll");
	assert!(took <= POLL_BUDGET, "one poll took {took} inbound events");

	it_should("yield while work remains and ask to be polled again");
	assert!(result.is_pending());
	assert!(h.remaining() > 0);
	assert!(h.woken());

	it_should("process the whole flood over further polls without stalling");
	let outcome = h.drive(&mut [], 200_000);
	assert!(outcome.is_ok(), "{outcome:?}");
	assert_eq!(h.remaining(), 0);
}

#[test]
fn sync_peer_floods_count_towards_the_poll_budget() {
	scenario("five thousand sync peers connect at once");
	let mut h = Harness::new(Arc::new(TopicValidator::default()));
	let peers: Vec<PeerId> = (0..5_000).map(|_| PeerId::random()).collect();
	for peer in &peers {
		h.push_sync(SyncEvent::PeerConnected { peer_id: *peer, roles: Roles::FULL });
	}

	let (result, took) = h.poll_once();
	it_should("take at most the per-poll budget of sync events in one poll");
	assert!(took <= POLL_BUDGET, "one poll took {took} inbound events");

	it_should("yield while sync events remain and ask to be polled again");
	assert!(result.is_pending());
	assert!(h.remaining() > 0);
	assert!(h.woken());

	let outcome = h.drive(&mut [], 200_000);
	it_should("add every sync peer to the reserved set");
	assert!(outcome.is_ok(), "{outcome:?}");
	let added = h.shared.reserved_added.lock().expect("not poisoned; qed").clone();
	let added_ids: HashSet<String> = added
		.iter()
		.filter_map(|addr| addr.rsplit('/').next().map(str::to_owned))
		.collect();
	assert_eq!(added.len(), peers.len());
	assert!(peers.iter().all(|peer| added_ids.contains(&peer.to_base58())));
}

#[test]
fn reading_subscribers_receive_every_valid_message_exactly_once_in_order() {
	scenario(
		"three peers send messages for three subscribed topics, one unsubscribed topic and \
		 invalid messages, while every subscriber reads after each poll",
	);
	let mut h = Harness::new(Arc::new(TopicValidator::default()));
	let peers: Vec<PeerId> = (0..3).map(|_| PeerId::random()).collect();
	for peer in &peers {
		h.connect(*peer);
	}
	let mut subs: Vec<Subscriber> = (1..=3).map(|tag| h.subscribe(tag)).collect();
	let mut expected: HashMap<u8, HashSet<(u8, u8, u32)>> = HashMap::new();
	for seq in 0..2_000u32 {
		for (sender, peer) in peers.iter().enumerate() {
			let tag = 1 + ((seq as usize + sender) % 4) as u8;
			let tag = if seq % 13 == 0 { DISCARDED_TOPIC } else { tag };
			h.receive(*peer, message(tag, sender as u8, seq));
			if (1..=3).contains(&tag) {
				expected.entry(tag).or_default().insert((tag, sender as u8, seq));
			}
		}
	}

	let outcome = {
		let mut readers: Vec<&mut Subscriber> = subs.iter_mut().collect();
		h.drive(&mut readers, 200_000)
	};
	it_should("take every inbound event without stalling");
	assert!(outcome.is_ok(), "{outcome:?}");

	for (index, sub) in subs.iter().enumerate() {
		let tag = index as u8 + 1;
		let got = sub.messages();
		it_should("deliver every valid message of the topic exactly once");
		let unique: HashSet<_> = got.iter().copied().collect();
		assert_eq!(unique.len(), got.len(), "duplicates delivered for topic {tag}");
		assert_eq!(&unique, expected.get(&tag).expect("topic has messages; qed"));

		it_should("preserve each sender's order");
		assert!(per_sender_in_order(&got));
	}
}

#[test]
fn an_idle_subscriber_does_not_block_anything_else() {
	scenario(
		"a subscriber never reads while its topic is flooded and, meanwhile, another topic is \
		 used, peers ask to be admitted, connect and disconnect, and sync peers change",
	);
	let validator = Arc::new(TopicValidator::default());
	let mut h = Harness::new(validator.clone());
	let flooder = PeerId::random();
	let talker = PeerId::random();
	let leaving = PeerId::random();
	let joining = PeerId::random();
	let asking = PeerId::random();
	let sync_joining = PeerId::random();
	let sync_leaving = PeerId::random();
	for peer in [flooder, talker, leaving] {
		h.connect(peer);
	}
	let mut idle = h.subscribe(1);
	let mut reader = h.subscribe(2);

	let mut expected_reader = Vec::new();
	let mut emit = |h: &mut Harness, range: std::ops::Range<u32>| {
		for seq in range {
			h.receive(flooder, message(1, 1, seq));
			if seq % 5 == 0 {
				h.receive(talker, message(2, 2, seq));
				expected_reader.push((2u8, 2u8, seq));
			}
		}
	};
	emit(&mut h, 0..5_000);
	let (verdict_tx, mut verdict_rx) = tokio::sync::oneshot::channel();
	h.push(NotificationEvent::ValidateInboundSubstream {
		peer: asking,
		handshake: Roles::FULL.encode(),
		result_tx: verdict_tx,
	});
	h.connect(joining);
	h.disconnect(leaving);
	h.push_sync(SyncEvent::PeerConnected { peer_id: sync_joining, roles: Roles::FULL });
	h.push_sync(SyncEvent::PeerDisconnected(sync_leaving));
	emit(&mut h, 5_000..10_000);

	let outcome = h.drive(&mut [&mut reader], 500_000);
	it_should("take every inbound event within a bounded number of polls");
	assert!(outcome.is_ok(), "{outcome:?}");

	it_should("deliver every message of the other topic to its reading subscriber, in order");
	assert_eq!(reader.messages(), expected_reader);

	it_should("answer the admission request");
	assert_eq!(verdict_rx.try_recv().ok(), Some(SubstreamVerdict::Accept));

	it_should("report the connection and the disconnection to the validator");
	assert!(validator.connected.lock().expect("not poisoned; qed").contains(&joining));
	assert!(validator.disconnected.lock().expect("not poisoned; qed").contains(&leaving));

	it_should("apply sync peer changes to the reserved peer set");
	let added = h.shared.reserved_added.lock().expect("not poisoned; qed").clone();
	assert!(added.iter().any(|addr| addr.contains(&sync_joining.to_base58())));
	let removed = h.shared.reserved_removed.lock().expect("not poisoned; qed").clone();
	assert!(removed.contains(&sync_leaving));

	it_should("stay running");
	assert!(!h.finished);
	assert!(!h.engine.is_terminated());

	let outcome = h.exhaust(&mut idle);
	it_should("let the idle subscriber read at most the bounded backlog, in order");
	assert!(outcome.is_ok(), "{outcome:?}");
	assert!(!idle.ended);
	assert!(
		idle.received.len() <= IDLE_BACKLOG,
		"idle subscriber could read {} messages",
		idle.received.len()
	);
	assert!(per_sender_in_order(&idle.messages()));
}

#[test]
fn a_reading_subscriber_is_unaffected_by_an_idle_one_on_the_same_topic() {
	scenario("two subscribers share a topic; one reads after every poll, the other never reads");
	let mut h = Harness::new(Arc::new(TopicValidator::default()));
	let peers: Vec<PeerId> = (0..2).map(|_| PeerId::random()).collect();
	for peer in &peers {
		h.connect(*peer);
	}
	let mut reader = h.subscribe(1);
	let mut idle = h.subscribe(1);
	for seq in 0..2_500u32 {
		for (sender, peer) in peers.iter().enumerate() {
			h.receive(*peer, message(1, sender as u8, seq));
		}
	}

	let outcome = h.drive(&mut [&mut reader], 200_000);
	it_should("take every inbound event without stalling");
	assert!(outcome.is_ok(), "{outcome:?}");

	it_should("deliver every message exactly once to the reading subscriber, in order");
	let got = reader.messages();
	let unique: HashSet<_> = got.iter().copied().collect();
	assert_eq!(got.len(), 5_000);
	assert_eq!(unique.len(), 5_000);
	assert!(per_sender_in_order(&got));

	let outcome = h.exhaust(&mut idle);
	it_should("bound what the idle subscriber can still read");
	assert!(outcome.is_ok(), "{outcome:?}");
	assert!(idle.received.len() <= IDLE_BACKLOG, "idle read {}", idle.received.len());
	assert!(per_sender_in_order(&idle.messages()));
}

#[test]
fn messages_discarded_for_every_subscriber_are_treated_as_never_received() {
	scenario(
		"a topic's only subscriber does not read while a peer sends three thousand messages; \
		 later the subscriber catches up and peers send the lost messages again",
	);
	let mut h = Harness::new(Arc::new(TopicValidator::default()));
	let original = PeerId::random();
	let other = PeerId::random();
	h.connect(original);
	h.connect(other);
	let mut first = h.subscribe(1);
	for seq in 0..3_000u32 {
		h.receive(original, message(1, 1, seq));
	}
	let outcome = h.drive(&mut [], 200_000);
	it_should("take the whole burst without stalling");
	assert!(outcome.is_ok(), "{outcome:?}");

	let outcome = h.exhaust(&mut first);
	it_should("bound what the subscriber can read after the burst");
	assert!(outcome.is_ok(), "{outcome:?}");
	let kept: HashSet<u32> = first.messages().iter().map(|(_, _, seq)| *seq).collect();
	assert_eq!(kept.len(), first.received.len(), "duplicates delivered");
	assert!(kept.len() <= IDLE_BACKLOG);
	let lost: Vec<u32> = (0..3_000).filter(|seq| !kept.contains(seq)).collect();
	assert!(!lost.is_empty());

	let mut late = h.subscribe(1);
	late.read_available();
	it_should("offer a new subscriber exactly the messages that were delivered");
	let late_initial: HashSet<u32> = late.messages().iter().map(|(_, _, seq)| *seq).collect();
	assert_eq!(late_initial, kept);
	assert_eq!(late.received.len(), kept.len());
	let late_initial_count = late.received.len();

	let reports_before = (h.negative_reports(original), h.negative_reports(other));
	let (resent_by_other, resent_by_original) = lost.split_at(lost.len() / 2);
	let replayed: Vec<u32> = kept.iter().copied().take(50).collect();
	for seq in resent_by_other {
		h.receive(other, message(1, 1, *seq));
	}
	for seq in resent_by_original {
		h.receive(original, message(1, 1, *seq));
	}
	for seq in &replayed {
		h.receive(other, message(1, 1, *seq));
	}
	let first_before = first.received.len();
	let outcome = h.drive(&mut [&mut first, &mut late], 200_000);
	it_should("take every resent message without stalling");
	assert!(outcome.is_ok(), "{outcome:?}");

	it_should("deliver every previously discarded message once, and nothing already delivered");
	let expected_lost: HashSet<u32> = lost.iter().copied().collect();
	for (sub, skip) in [(&first, first_before), (&late, late_initial_count)] {
		let fresh: Vec<u32> = sub.messages()[skip..].iter().map(|(_, _, seq)| *seq).collect();
		let fresh_set: HashSet<u32> = fresh.iter().copied().collect();
		assert_eq!(fresh.len(), fresh_set.len(), "duplicates delivered");
		assert_eq!(fresh_set, expected_lost);
	}

	it_should("not penalise peers for resending messages that were discarded");
	assert_eq!(h.negative_reports(original), reports_before.0);
	assert_eq!(h.negative_reports(other), reports_before.1);
}

#[test]
fn a_new_subscriber_obtains_a_large_retained_backlog_immediately() {
	scenario("five thousand messages are retained for a topic before anyone subscribes");
	let mut h = Harness::new(Arc::new(TopicValidator::default()));
	for seq in 0..5_000u32 {
		h.engine.register_gossip_message(topic(1), message(1, 0, seq));
	}
	let mut sub = h.subscribe(1);
	sub.read_available();

	it_should("let the new subscriber read every retained message right away");
	let got: HashSet<u32> = sub.messages().iter().map(|(_, _, seq)| *seq).collect();
	assert_eq!(sub.received.len(), 5_000);
	assert_eq!(got, (0..5_000).collect::<HashSet<_>>());
	assert!(!sub.ended);
}

#[test]
fn the_engine_only_completes_when_its_event_sources_end() {
	scenario(
		"subscribers overflow and go away while peers keep sending, then the notification \
		 source ends",
	);
	let mut h = Harness::new(Arc::new(TopicValidator::default()));
	let peer = PeerId::random();
	h.connect(peer);
	let mut idle = h.subscribe(1);
	let dropped = h.subscribe(1);
	let mut other_dropped = Some(h.subscribe(2));
	for seq in 0..5_000u32 {
		h.receive(peer, message(1 + (seq % 2) as u8, 1, seq));
		if seq == 2_000 {
			drop(other_dropped.take());
		}
	}
	drop(dropped);
	let outcome = h.drive(&mut [], 200_000);
	it_should("keep running through overflow and dropped subscribers");
	assert!(outcome.is_ok(), "{outcome:?}");
	assert!(!h.engine.is_terminated());
	let outcome = h.exhaust(&mut idle);
	it_should("keep running while the overflowed subscriber catches up");
	assert!(outcome.is_ok(), "{outcome:?}");
	assert!(!h.engine.is_terminated());

	h.notifications = None;
	let mut completed = false;
	for _ in 0..10_000 {
		if h.poll_once().0.is_ready() {
			completed = true;
			break;
		}
	}
	it_should("complete once the notification source has ended");
	assert!(completed);
	assert!(h.engine.is_terminated());
}

#[test]
fn the_engine_completes_when_the_sync_source_ends() {
	scenario("the sync event source ends while peers are connected and subscribers idle");
	let mut h = Harness::new(Arc::new(TopicValidator::default()));
	let peer = PeerId::random();
	h.connect(peer);
	let _idle = h.subscribe(1);
	for seq in 0..3_000u32 {
		h.receive(peer, message(1, 1, seq));
	}
	h.sync = None;
	let mut completed = false;
	for _ in 0..10_000 {
		if h.poll_once().0.is_ready() {
			completed = true;
			break;
		}
	}
	it_should("complete once the sync source has ended");
	assert!(completed);
	assert!(h.engine.is_terminated());
}

/// Validator whose predicates hold a shared lock, detecting any validator call made while a
/// predicate is alive.
#[derive(Default)]
struct DisciplinedValidator {
	state: RwLock<()>,
	violations: AtomicUsize,
	validate_calls: AtomicUsize,
	new_peer_calls: AtomicUsize,
	disconnected_calls: AtomicUsize,
	allowed_calls: AtomicUsize,
	expired_calls: AtomicUsize,
}

impl DisciplinedValidator {
	fn check_exclusive(&self) {
		if self.state.try_write().is_err() {
			self.violations.fetch_add(1, Ordering::SeqCst);
		}
	}
}

impl Validator<Block> for DisciplinedValidator {
	fn new_peer(&self, context: &mut dyn ValidatorContext<Block>, who: &PeerId, _: ObservedRole) {
		self.check_exclusive();
		self.new_peer_calls.fetch_add(1, Ordering::SeqCst);
		context.send_message(who, message(9, 0, 0));
	}

	fn peer_disconnected(&self, _: &mut dyn ValidatorContext<Block>, _: &PeerId) {
		self.check_exclusive();
		self.disconnected_calls.fetch_add(1, Ordering::SeqCst);
	}

	fn validate(
		&self,
		context: &mut dyn ValidatorContext<Block>,
		sender: &PeerId,
		data: &[u8],
	) -> ValidationResult<H256> {
		self.check_exclusive();
		self.validate_calls.fetch_add(1, Ordering::SeqCst);
		let tag = data[0];
		context.broadcast_message(topic(tag), data.to_vec(), false);
		context.send_topic(sender, topic(tag), false);
		ValidationResult::ProcessAndKeep(topic(tag))
	}

	fn message_expired<'a>(&'a self) -> Box<dyn FnMut(H256, &[u8]) -> bool + 'a> {
		self.check_exclusive();
		self.expired_calls.fetch_add(1, Ordering::SeqCst);
		let guard = self.state.read().expect("not poisoned; qed");
		Box::new(move |_, _| {
			let _held = &guard;
			false
		})
	}

	fn message_allowed<'a>(
		&'a self,
	) -> Box<dyn FnMut(&PeerId, MessageIntent, &H256, &[u8]) -> bool + 'a> {
		self.check_exclusive();
		self.allowed_calls.fetch_add(1, Ordering::SeqCst);
		let guard = self.state.read().expect("not poisoned; qed");
		Box::new(move |_, _, _, _| {
			let _held = &guard;
			true
		})
	}
}

#[test]
fn validator_is_never_called_while_one_of_its_predicates_is_in_use() {
	scenario(
		"peers connect, gossip, receive broadcasts and disconnect, local messages are gossiped \
		 and periodic maintenance runs, with a validator whose predicates hold its state",
	);
	let validator = Arc::new(DisciplinedValidator::default());
	let mut h = Harness::new(validator.clone());
	let peers: Vec<PeerId> = (0..3).map(|_| PeerId::random()).collect();
	for peer in &peers {
		h.connect(*peer);
	}
	let mut sub = h.subscribe(1);
	for seq in 0..300u32 {
		for (sender, peer) in peers.iter().enumerate() {
			h.receive(*peer, message(1 + (seq % 2) as u8, sender as u8, seq));
		}
	}
	h.engine.gossip_message(topic(1), message(1, 9, 1), false);
	h.engine.broadcast_topic(topic(2), true);
	h.engine.send_topic(&peers[0], topic(1), true);
	let outcome = h.drive(&mut [&mut sub], 100_000);
	it_should("process the gossip activity without stalling");
	assert!(outcome.is_ok(), "{outcome:?}");
	std::thread::sleep(Duration::from_millis(1_300));
	let outcome = h.drive(&mut [&mut sub], 100_000);
	it_should("run periodic maintenance without stalling");
	assert!(outcome.is_ok(), "{outcome:?}");
	h.disconnect(peers[2]);
	let outcome = h.drive(&mut [&mut sub], 100_000);
	it_should("process the disconnection without stalling");
	assert!(outcome.is_ok(), "{outcome:?}");

	it_should("exercise every kind of validator callback");
	assert!(validator.validate_calls.load(Ordering::SeqCst) > 0);
	assert!(validator.new_peer_calls.load(Ordering::SeqCst) > 0);
	assert!(validator.disconnected_calls.load(Ordering::SeqCst) > 0);
	assert!(validator.allowed_calls.load(Ordering::SeqCst) > 0);
	assert!(validator.expired_calls.load(Ordering::SeqCst) > 0);

	it_should("never call the validator while one of its predicates is alive");
	assert_eq!(validator.violations.load(Ordering::SeqCst), 0);
}

#[test]
fn the_engine_can_be_driven_from_another_thread() {
	scenario("the engine is moved to a different thread and driven there");
	let mut h = Harness::new(Arc::new(TopicValidator::default()));
	let peer = PeerId::random();
	h.connect(peer);
	h.receive(peer, message(1, 1, 0));
	let mut sub = h.subscribe(1);
	let mut engine = h.engine;
	let handle = std::thread::spawn(move || {
		let waker = Waker::from(Arc::new(WakeFlag::default()));
		let mut cx = Context::from_waker(&waker);
		let mut ready = false;
		for _ in 0..16 {
			ready |= engine.poll_unpin(&mut cx).is_ready();
		}
		(engine, ready)
	});
	let (_engine, ready) = handle.join().expect("driving thread does not panic; qed");
	sub.read_available();

	it_should("keep running and deliver messages when driven from another thread");
	assert!(!ready);
	assert_eq!(sub.messages(), vec![(1, 1, 0)]);
}
