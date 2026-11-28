# Bounded delivery of network events to protocol consumers

Resolves paritytech/polkadot-sdk#557. Inbound network events must not accumulate without bound when
a consumer, notably `sc_network_gossip::GossipEngine`, is slower than the network.

Scope: the libp2p notification service, the network event broadcast channels
(`sc_network::service::out_events`) and `GossipEngine`. The litep2p backend and the sync event
streams are out of scope. Behaviour not described here is unchanged, including every public
signature not listed under **Contracts**. Existing tests that contradict these requirements are to
be updated.

## Terms

- **Subscriber**: one `NotificationService` handle, the original or one obtained with `clone`.
- **Buffered**: reported by the network but not yet returned by the subscriber's `next_event`.
- **Data event**: `NotificationEvent::NotificationReceived`. All other events are **lifecycle
  events**.
- **Connection**: from a reported substream open of a peer to the next reported close of that peer.

## 1. Notification service

### Contracts

- `NotificationBufferLimits`: public struct implementing `Default`, `Clone`, `Debug`, with public
  `usize` fields `max_notifications`, `max_bytes`, `max_notifications_per_peer`,
  `max_bytes_per_peer`.
- `notification_service_with_limits(protocol: ProtocolName, limits: NotificationBufferLimits) ->
  (ProtocolHandlePair, Box<dyn NotificationService>)`, next to `notification_service`, which
  behaves as if given `NotificationBufferLimits::default()`.
- Counter `substrate_sub_libp2p_notifications_dropped_total{protocol}`, registered by
  `NotificationMetrics::new` and fed through `ProtocolHandle::set_metrics`.
- These crate-internal items keep their names and signatures: `ProtocolHandlePair::split`,
  `ProtocolHandle::{report_incoming_substream, report_substream_opened, report_substream_closed,
  report_notification_received, report_notification_sink_replaced}`, `ValidationCallResult` and
  `NotificationsSink::new`.

### Requirements

- **NS-1** For each subscriber, buffered data events never exceed `max_notifications` in number or
  `max_bytes` in payload, and for each peer never exceed `max_notifications_per_peer` in number or
  `max_bytes_per_peer` in payload.
- **NS-2** When buffering a newly reported data event would break NS-1, that event is discarded as
  it is reported; buffered events are kept. No other data event is discarded, except those reported
  during a connection the subscriber never observes. After a burst of equally sized notifications
  with no reads, exactly as many are returned as the tightest limit allows. Events reported after
  the subscriber read are buffered again.
- **NS-3** Lifecycle events, including admission requests and their answers, are never discarded
  because of data limits and do not count towards them.
- **NS-4** Per peer, a subscriber observes opens and closes alternately starting with an open. Data
  is returned only between the open and the close of the connection it was reported in, and in
  reported order.
- **NS-5** However often a peer reconnects while the subscriber does not read, at most two opens
  and closes of that peer are buffered. Once the subscriber has read everything, its view of the
  peer (connected or not, handshake, sink used for sending) matches the latest reported state.
- **NS-6** Subscribers are limited independently. Dropping one stops delivery to it only.
- **NS-7** Each data event discarded under NS-1 increments the dropped counter by exactly one when
  it is discarded. Data of connections the subscriber never observes is not counted.
- **NS-8** `NotificationBufferLimits::default()` satisfies: `max_notifications` in [1 024, 100 000];
  `max_bytes` in [1 MiB, 64 MiB]; `max_notifications_per_peer` in [256, `max_notifications`];
  `max_bytes_per_peer` in [256 KiB, `max_bytes`].

## 2. Network event broadcast channels

### Contracts

- `out_events::{channel, OutChannels, Receiver}` keep their names. `channel(name, capacity)`:
  `capacity` is the maximum number of undelivered events of the receiver.
- Events discarded by `OutChannels::send` increment
  `substrate_sub_libp2p_out_events_events_total{action="dropped", name=<channel name>, ..}`.

### Requirements

- **OE-1** Undelivered events of a receiver never exceed `capacity`. An event is discarded for a
  receiver only if it would. Events are yielded in send order, and a receiver that reads again
  receives later events.
- **OE-2** Receivers are limited independently; a dropped receiver affects no other.
- **OE-3** `push_with` registers the receiver and yields all initial events, in order and before
  later events, however many there are.
- **OE-4** Every discarded event increments the dropped counter of its channel by one, when it is
  discarded.

## 3. Gossip engine

### Contracts

- Public API, `Send`/`Unpin` and `FusedFuture` behaviour of `GossipEngine` are unchanged. Methods
  stay synchronous.

### Requirements

- **GE-1** A single poll takes at most 1 024 inbound events from the notification and sync streams
  together. If it returns `Pending` while events remain, the engine is woken without needing a new
  event.
- **GE-2** However many subscribers never read, the engine keeps taking inbound events and keeps
  answering admission requests, reporting peer connections and disconnections to the validator,
  applying sync peer changes to the reserved set, and delivering messages to subscribers that read.
- **GE-3** A subscriber that never reads can read at most its initial messages plus 1 024 messages.
  Further messages are discarded for it only.
- **GE-4** A subscriber that reads after every poll receives every valid message of its topic
  exactly once, in arrival order per sender, whatever other subscribers do.
- **GE-5** A message discarded for every subscriber of its topic is treated as never received: not
  retained for new subscribers, not considered known, and a later copy from any peer is processed as
  new without penalising the sender. Delivered messages remain known and, if kept, retained.
- **GE-6** A new subscriber can read every retained message of its topic, however many.
