# Bounded delivery of network events to protocol consumers

Resolves paritytech/polkadot-sdk#557: inbound network events must not accumulate without bound when
a consumer (notably `sc_network_gossip::GossipEngine`) is slower than the network.

## Scope

1. The per-protocol notification service of the libp2p backend (`sc-network`): the producer side
   (`ProtocolHandle`) and the consumer side (`NotificationService` handles).
2. Network event broadcast channels (`sc_network::service::out_events`).
3. `sc_network_gossip::GossipEngine`.

The litep2p backend and the event streams of `SyncingEngine` are out of scope.

## Definitions

- **Subscriber**: one `NotificationService` handle, either the one returned at creation or one
  obtained through `NotificationService::clone`.
- **Data event**: an inbound notification (`NotificationEvent::NotificationReceived`).
- **Lifecycle event**: a substream validation request, a substream open, a substream close, or a
  notification sink replacement.
- **Buffered**: reported by the producer but not yet returned to the subscriber by `next_event`.
- **Connection**: the period between a reported substream open for a peer and the following
  reported substream close for that peer.
- **Inbound event** (gossip): an item obtained from the notification service or from the sync event
  stream.

## 1. Notification service

### Contracts

- `NotificationBufferLimits`: public struct; implements `Default`, `Clone`, `Debug`; public `usize`
  fields `max_notifications`, `max_bytes`, `max_notifications_per_peer`, `max_bytes_per_peer`.
- `notification_service_with_limits(protocol: ProtocolName, limits: NotificationBufferLimits) ->
  (ProtocolHandlePair, Box<dyn NotificationService>)`, reachable from the module that provides
  `notification_service`.
- `notification_service(protocol)` behaves as `notification_service_with_limits(protocol,
  NotificationBufferLimits::default())`.
- Prometheus counter `substrate_sub_libp2p_notifications_dropped_total` with label `protocol`,
  registered by `NotificationMetrics::new` and fed through `ProtocolHandle::set_metrics`.
- Signatures of the existing `ProtocolHandle` methods and of the `NotificationService` trait are
  unchanged.

### Requirements

- **NS-1 Bounded data.** For each subscriber, at all times: buffered data events ≤
  `max_notifications`; their total payload length ≤ `max_bytes`; for each peer, its buffered data
  events ≤ `max_notifications_per_peer` and their total payload length ≤ `max_bytes_per_peer`.
- **NS-2 No premature discard.** A data event is discarded only when keeping it would violate NS-1.
  After a burst of equally sized notifications (size `s`) with no reads, the subscriber returns
  exactly as many as the tightest applicable limit allows (byte limits do not constrain `s = 0`).
- **NS-3 Order.** Data events of the same peer are returned in the order reported, at most once.
  Discarded events are never returned.
- **NS-4 Lifecycle events are not subject to data limits.** They are never discarded because of
  data limits, do not count towards them, and are returned while data limits are reached. A
  validation request reaches the subscriber and the subscriber's answer reaches the producer.
- **NS-5 Per-peer consistency.** For each peer, the subscriber observes substream opens and closes
  alternately, starting with an open. A data event is returned only while its peer is observed as
  connected, and only if it was reported during the connection the subscriber observed as open.
  Data of a connection is returned before the close of that connection.
- **NS-6 Bounded lifecycle under churn.** However often a peer reconnects while the subscriber does
  not read, at most 2 opens and closes of that peer are buffered for the subscriber. A connection
  whose open was never returned may be omitted entirely, together with its data. When the
  subscriber has read everything, its view of each peer matches the latest reported state:
  connected or not, the handshake of the current connection, and the sink used for sending. If a
  connection the subscriber observed was followed by a newer one, the subscriber observes a close
  before the newer open.
- **NS-7 Sink replacement.** After the subscriber has read everything, sending to a peer
  (`send_sync_notification`, `send_async_notification`, `message_sink`) uses the most recently
  reported sink of the peer's current connection, whether or not data was discarded.
- **NS-8 Subscriber independence.** Limits apply to each subscriber separately. One subscriber's
  backlog never causes discards for another subscriber.
- **NS-9 Survival.** Reaching a limit never ends a subscriber's event stream or unregisters it.
  After reading, it receives subsequently reported events. Only dropping a subscriber stops
  delivery to it, without affecting others.
- **NS-10 Non-blocking producer.** Producer calls return without waiting for any subscriber.
- **NS-11 Accounting.** When metrics are set, the dropped counter for the protocol increases by
  exactly one for every data event discarded for a subscriber under NS-1/NS-2. Data omitted under
  NS-6 need not be counted.
- **NS-12 Defaults.** `NotificationBufferLimits::default()`: `max_notifications` in
  [1 024, 100 000]; `max_bytes` in [1 MiB, 64 MiB]; `max_notifications_per_peer` in
  [256, `max_notifications`]; `max_bytes_per_peer` in [256 KiB, `max_bytes`].

## 2. Network event broadcast channels

### Contracts

- `channel(name, capacity)`: the second parameter is the maximum number of undelivered events per
  receiver.
- `OutChannels::{new, push, push_with, send}`: existing signatures.
- Counter `substrate_sub_libp2p_out_events_events_total` (existing labels `event_name`, `action`,
  `name`) counts discarded events with `action = "dropped"` and `name` = the channel name.

### Requirements

- **OE-1 Bounded.** Events delivered through `send` never leave more than `capacity` undelivered
  events for a receiver.
- **OE-2 No premature discard.** An event is discarded for a receiver only when `capacity` would be
  exceeded. After a burst of `k > capacity` events with no reads, the receiver yields exactly
  `capacity` events.
- **OE-3 Order.** A receiver yields events in send order, at most once.
- **OE-4 Survival.** A full receiver stays registered and, after reading, receives later events. A
  receiver is unregistered only after it is dropped. Dropped receivers do not affect others.
- **OE-5 Initial events.** With `push_with`, the receiver is always registered and yields every
  initial event in order, before any later event, regardless of `capacity`.
- **OE-6 Non-blocking.** `send` returns without waiting for any receiver.
- **OE-7 Accounting.** Every discarded event increments the dropped counter of its channel by one.

## 3. Gossip engine

### Contracts

- The existing public API of `GossipEngine` (`new`, `messages_for`, `register_gossip_message`,
  `gossip_message`, `broadcast_topic`, `send_topic`, `send_message`, `report`, `announce`,
  `mark_message_known`, `take_notification_service`, `Future`, `FusedFuture`) keeps its
  signatures. Its methods stay synchronous.
- `GossipEngine` is `Send` and `Unpin`.
- The `Validator`, `ValidatorContext`, `ValidationResult` and `TopicNotification` contracts are
  unchanged.

### Requirements

- **GE-1 Bounded poll.** A single poll takes at most 1 024 inbound events. If a poll returns
  `Pending` while inbound events remain available, a further poll is requested without any new
  inbound event, at the latest once the subscribers that read have read all their available
  messages.
- **GE-2 Progress despite non-reading subscribers.** While one or more subscribers never read, the
  engine keeps doing all of the following, and takes every available inbound event within a
  bounded number of polls:
  - answers substream validation requests;
  - reports peer connections and disconnections to the validator;
  - applies sync peer events to the reserved peer set;
  - delivers messages to subscribers that read.
- **GE-3 Bounded backlog.** A subscriber that does not read can never read more than its initial
  messages plus 1 024 messages. Messages beyond that are discarded for that subscriber.
- **GE-4 No loss for reading subscribers.** A subscriber that reads all its available messages
  whenever a poll of the engine returns receives every valid message of its topic exactly once,
  in arrival order per sender, regardless of other subscribers of the same or other topics.
- **GE-5 Discarded messages are not seen.** A message discarded for every subscriber of its topic
  under GE-3 is treated as never received:
  - it is not retained for new subscribers;
  - it is not considered known;
  - a later copy from any peer is processed as new and delivered, and its sender is not
    penalised for a duplicate.

  Delivered messages stay known: later copies are not delivered again. Delivered messages
  validated as `ProcessAndKeep` stay retained.
- **GE-6 Initial messages.** A new subscriber can immediately read every retained message of its
  topic, whatever their number, without failure.
- **GE-7 Termination.** The engine completes when the notification event stream or the sync event
  stream ends, within a bounded number of polls, and only then. Discards, slow subscribers and
  dropped subscribers never complete it.
- **GE-8 Validator call discipline.** While a predicate returned by `Validator::message_allowed` or
  `Validator::message_expired` is alive, no other `Validator` method is invoked.
