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

//! Behaviour tests for bounded network event broadcast channels.

use super::{channel, OutChannels, Receiver};
use crate::event::Event;

use futures::{FutureExt, StreamExt};
use prometheus_endpoint::Registry;
use sc_network_types::PeerId;

const EVENTS_METRIC: &str = "substrate_sub_libp2p_out_events_events_total";

fn scenario(text: &str) {
	println!("Scenario: {text}");
}

fn it_should(text: &str) {
	println!("It should {text}");
}

/// An event that identifies its position in the send order.
fn numbered(peers: &[PeerId], index: usize) -> Event {
	Event::PeersDiscovered(vec![peers[index]])
}

fn peers(count: usize) -> Vec<PeerId> {
	(0..count).map(|_| PeerId::random()).collect()
}

/// Reads everything currently available and maps each event back to its send position.
/// Returns the positions and whether the stream ended.
fn drain(receiver: &mut Receiver, peers: &[PeerId]) -> (Vec<usize>, bool) {
	let mut out = Vec::new();
	loop {
		match receiver.next().now_or_never() {
			Some(Some(Event::PeersDiscovered(found))) => {
				out.push(peers.iter().position(|p| found.first() == Some(p)).unwrap_or(usize::MAX))
			},
			Some(Some(_)) => out.push(usize::MAX),
			Some(None) => return (out, true),
			None => return (out, false),
		}
	}
}

fn dropped(registry: &Registry, channel: &str) -> u64 {
	registry
		.gather()
		.iter()
		.filter(|family| family.get_name() == EVENTS_METRIC)
		.flat_map(|family| family.get_metric().iter())
		.filter(|metric| {
			let labels = metric.get_label();
			labels.iter().any(|l| l.get_name() == "action" && l.get_value() == "dropped") &&
				labels.iter().any(|l| l.get_name() == "name" && l.get_value() == channel)
		})
		.map(|metric| metric.get_counter().get_value() as u64)
		.sum()
}

fn strictly_increasing(values: &[usize]) -> bool {
	values.windows(2).all(|w| w[0] < w[1])
}

#[test]
fn burst_beyond_capacity_keeps_exactly_capacity_events_in_order() {
	scenario("a hundred network events are broadcast to a listener that does not read");
	let registry = Registry::new();
	let mut channels = OutChannels::new(Some(&registry)).expect("fresh registry; qed");
	let (tx, mut rx) = channel("bounded-idle", 10);
	channels.push(tx);
	let ids = peers(100);
	for index in 0..100 {
		channels.send(numbered(&ids, index));
	}
	it_should("count discarded events as they are broadcast, before anything is read");
	assert_eq!(dropped(&registry, "bounded-idle"), 90);
	let (seen, ended) = drain(&mut rx, &ids);

	it_should("keep the listener's stream open");
	assert!(!ended);

	it_should("retain exactly the channel capacity of events, in send order, without repeats");
	assert_eq!(seen.len(), 10);
	assert!(strictly_increasing(&seen));
	assert!(seen.iter().all(|index| *index < 100));

	it_should("count every discarded event for that listener");
	assert_eq!(dropped(&registry, "bounded-idle"), 90);
}

#[test]
fn full_listener_stays_registered_and_receives_later_events() {
	scenario("a listener overflows, then catches up and more events are broadcast");
	let registry = Registry::new();
	let mut channels = OutChannels::new(Some(&registry)).expect("fresh registry; qed");
	let (tx, mut rx) = channel("bounded-recovering", 10);
	channels.push(tx);
	let ids = peers(105);
	for index in 0..100 {
		channels.send(numbered(&ids, index));
	}
	let _ = drain(&mut rx, &ids);
	for index in 100..105 {
		channels.send(numbered(&ids, index));
	}
	let (seen, ended) = drain(&mut rx, &ids);

	it_should("deliver every later event after the listener caught up");
	assert!(!ended);
	assert_eq!(seen, vec![100, 101, 102, 103, 104]);

	it_should("not count events that were delivered");
	assert_eq!(dropped(&registry, "bounded-recovering"), 90);
}

#[test]
fn events_within_capacity_are_never_discarded() {
	scenario(
		"exactly as many events as the capacity are broadcast to a listener that does not read",
	);
	let registry = Registry::new();
	let mut channels = OutChannels::new(Some(&registry)).expect("fresh registry; qed");
	let (tx, mut rx) = channel("bounded-exact", 10);
	channels.push(tx);
	let ids = peers(10);
	for index in 0..10 {
		channels.send(numbered(&ids, index));
	}
	let (seen, _) = drain(&mut rx, &ids);

	it_should("deliver all of them in order and discard nothing");
	assert_eq!(seen, (0..10).collect::<Vec<_>>());
	assert_eq!(dropped(&registry, "bounded-exact"), 0);
}

#[test]
fn listeners_are_bounded_independently() {
	scenario("one listener reads after every event while another never reads");
	let registry = Registry::new();
	let mut channels = OutChannels::new(Some(&registry)).expect("fresh registry; qed");
	let (reader_tx, mut reader) = channel("bounded-reader", 10);
	let (idle_tx, mut idle) = channel("bounded-lazy", 10);
	channels.push(reader_tx);
	channels.push(idle_tx);
	let ids = peers(100);
	let mut read = Vec::new();
	for index in 0..100 {
		channels.send(numbered(&ids, index));
		read.extend(drain(&mut reader, &ids).0);
	}
	let (idle_seen, _) = drain(&mut idle, &ids);

	it_should("deliver every event to the reading listener");
	assert_eq!(read, (0..100).collect::<Vec<_>>());
	assert_eq!(dropped(&registry, "bounded-reader"), 0);

	it_should("bound only the idle listener");
	assert_eq!(idle_seen.len(), 10);
	assert!(strictly_increasing(&idle_seen));
	assert_eq!(dropped(&registry, "bounded-lazy"), 90);
}

#[test]
fn dropped_listener_does_not_affect_others() {
	scenario("one of two listeners goes away and broadcasting continues well beyond capacity");
	let registry = Registry::new();
	let mut channels = OutChannels::new(Some(&registry)).expect("fresh registry; qed");
	let (gone_tx, gone) = channel("bounded-gone", 10);
	let (kept_tx, mut kept) = channel("bounded-kept", 10);
	channels.push(gone_tx);
	channels.push(kept_tx);
	drop(gone);
	let ids = peers(30);
	for index in 0..5 {
		channels.send(numbered(&ids, index));
	}
	let (first, _) = drain(&mut kept, &ids);
	for index in 5..30 {
		channels.send(numbered(&ids, index));
	}
	let (second, ended) = drain(&mut kept, &ids);

	it_should("keep delivering to the remaining listener within its capacity");
	assert!(!ended);
	assert_eq!(first, vec![0, 1, 2, 3, 4]);
	assert_eq!(second.len(), 10);
	assert!(strictly_increasing(&second));
	assert!(second.iter().all(|index| (5..30).contains(index)));

	it_should("stop holding events for the listener that went away");
	assert_eq!(dropped(&registry, "bounded-gone"), 0);
	assert_eq!(dropped(&registry, "bounded-kept"), 15);
}

#[test]
fn initial_events_are_delivered_in_full_regardless_of_capacity() {
	scenario("a late listener is given more initial events than its capacity");
	let registry = Registry::new();
	let mut channels = OutChannels::new(Some(&registry)).expect("fresh registry; qed");
	let (tx, mut rx) = channel("bounded-late", 4);
	let ids = peers(30);
	channels.push_with(tx, (0..12).map(|index| numbered(&ids, index)));
	let (initial, ended) = drain(&mut rx, &ids);

	it_should("register the listener and deliver every initial event in order");
	assert!(!ended);
	assert_eq!(initial, (0..12).collect::<Vec<_>>());

	it_should("deliver later events once the initial ones were read");
	for index in 12..15 {
		channels.send(numbered(&ids, index));
	}
	assert_eq!(drain(&mut rx, &ids).0, vec![12, 13, 14]);

	it_should("apply the capacity to later bursts");
	for index in 15..30 {
		channels.send(numbered(&ids, index));
	}
	let (burst, _) = drain(&mut rx, &ids);
	assert_eq!(burst.len(), 4);
	assert!(strictly_increasing(&burst));
	assert_eq!(dropped(&registry, "bounded-late"), 11);
}

#[test]
fn initial_events_come_before_later_events() {
	scenario("events are broadcast before a late listener reads its initial events");
	let mut channels = OutChannels::new(None).expect("no registry; qed");
	let (tx, mut rx) = channel("bounded-ordered", 100);
	let ids = peers(8);
	channels.push_with(tx, (0..5).map(|index| numbered(&ids, index)));
	for index in 5..8 {
		channels.send(numbered(&ids, index));
	}

	it_should("deliver the initial events first, then the later ones, in order");
	assert_eq!(drain(&mut rx, &ids).0, (0..8).collect::<Vec<_>>());
}
