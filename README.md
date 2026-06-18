# Gossip-Based Membership and Broadcast Protocol Evaluation

This project implements and evaluates several combinations of **membership** and **broadcast** protocols for distributed systems using the Babel framework.

The main objective is to study how the underlying membership protocol influences broadcast reliability, latency, redundancy, and maintenance overhead.

## Protocols

### Membership protocols

The project supports three membership configurations:

- **Full Membership**
  - Every node knows all other nodes.
  - Provides a dense overlay and high path redundancy.
  - Mainly used as a baseline.

- **Cyclon**
  - Maintains a bounded random partial view.
  - Periodically exchanges neighbour samples through shuffle operations.
  - Uses neighbour ages to select the oldest peer for each shuffle.

- **HyParView**
  - Maintains a small **active view** used for communication and a larger **passive view** used as backup.
  - Supports joining, forwarding joins, active-view repair, neighbour negotiation, disconnect handling, and periodic passive-view shuffling.
  - Designed to recover quickly from failures and churn.

### Broadcast protocols

- **Flood**
  - Forwards every new message to all known neighbours except the immediate sender.
  - Uses message identifiers to avoid repeated delivery and infinite forwarding loops.

- **Eager-Push Gossip**
  - Forwards each new message to a random subset of neighbours.
  - The subset size is controlled by a fixed fanout.

- **Hybrid Adaptive Gossip**
  - Sends the full message to eager targets.
  - Sends an `IHAVE` announcement to lazy targets.
  - Missing messages are retrieved using a `REQUEST` message.
  - Dynamically adjusts the eager fanout according to the observed duplicate rate.

## Project Architecture

Each node runs:

1. one membership protocol;
2. one broadcast protocol;
3. the application protocol.

The membership layer creates and manages the network channel. It notifies the broadcast layer when the channel is ready, when a neighbour becomes available, and when a neighbour becomes unavailable.

```text
Application
    |
    | BroadcastRequest
    v
Broadcast protocol
    |
    | uses current neighbours
    v
Membership overlay
    |
    | TCP channel
    v
Remote nodes
```

## Broadcast Flow

### Flood

```text
1. The application creates a broadcast request.
2. The local node creates a FloodMessage.
3. The message is processed as a newly received message.
4. If the message identifier is new:
   - deliver it to the application;
   - forward it to every neighbour except the sender.
5. If the identifier was already seen, ignore the message.
```

### Eager-Push Gossip

```text
1. The application creates a broadcast request.
2. The local node creates an EagerPushMessage.
3. If the identifier is new:
   - deliver the message;
   - select up to `fanout` random neighbours;
   - send the full message to those neighbours.
4. Duplicate messages are ignored.
```

### Hybrid Adaptive Gossip

```text
1. The application creates a broadcast request.
2. The local node creates a GossipFullMessage.
3. If the full message is received for the first time:
   - store its identifier;
   - cache the full message;
   - deliver it to the application;
   - send the full message to eager targets;
   - send IHAVE notifications to lazy targets.
4. A node that receives IHAVE:
   - ignores it if it already has the message;
   - otherwise sends REQUEST to the announcer.
5. A node that receives REQUEST:
   - retrieves the full message from its cache;
   - sends it to the requester.
6. Duplicate full messages are counted.
7. After `adapt_window` full-message observations:
   - reduce eager fanout if duplicate rate is above 50%;
   - increase eager fanout if duplicate rate is below 20%;
   - keep it unchanged otherwise.
```

The duplicate rate is computed as:

```text
duplicate rate = duplicate full messages
                 -----------------------
                 new full messages + duplicate full messages
```

## Membership Flow

### Cyclon

Each node maintains a partial view of `Host -> age` entries.

At every shuffle period:

1. increment the age of all entries;
2. select the oldest neighbour;
3. build a random sample;
4. add the local node to the sample;
5. send a `ShuffleRequest`;
6. receive a `ShuffleReply`;
7. merge the received sample into the local view;
8. replace sampled entries when the view is full.

Cyclon uses:

- `ShuffleRequest`
- `ShuffleReply`

### HyParView

Each node maintains:

- `activeView`: neighbours currently used by the broadcast protocol;
- `passiveView`: backup peers;
- connection and pending-operation sets.

#### Join

1. A joining node connects to a known contact.
2. It sends `JoinMessage`.
3. The contacted node adds it to the active view.
4. The contacted node forwards `ForwardJoinMessage` through its active view.
5. Intermediate nodes may add the joining node to their passive view.
6. The final node adds it to its active view.

#### Active-view repair

When an active neighbour fails:

1. remove it from the active view;
2. place it in the passive view when appropriate;
3. choose a passive candidate;
4. open a connection;
5. send a high-priority `NeighborMessage`;
6. process the corresponding `NeighborReplyMessage`.

#### Shuffle

Periodically:

1. select an active neighbour;
2. build a sample from active and passive views;
3. send a `ShuffleMessage` with a TTL;
4. forward the shuffle through the active overlay;
5. the final node sends `ShuffleReplyMessage` to the origin;
6. merge received peers into the passive view.

HyParView uses:

- `JoinMessage`
- `ForwardJoinMessage`
- `DisconnectMessage`
- `NeighborMessage`
- `NeighborReplyMessage`
- `ShuffleMessage`
- `ShuffleReplyMessage`

## Configuration

```properties
#### Membership

protocol.membership.samplesize=6
protocol.membership.sampletime=2000
protocol_metrics_interval=-1
channel_metrics_interval=-1

# Cyclon
protocol.membership.cyclon.maxN=8
protocol.membership.cyclon.shuffle_time=2000
protocol.membership.cyclon.subset_size=4

# HyParView
protocol.membership.hyparview.active_size=5
protocol.membership.hyparview.passive_size=20
protocol.membership.hyparview.arwl=5
protocol.membership.hyparview.prwl=2
protocol.membership.hyparview.shuffle_ttl=3
protocol.membership.hyparview.shuffle_period=4000
protocol.membership.hyparview.shuffle_k_active=3
protocol.membership.hyparview.shuffle_k_passive=4

#### Broadcast

# Eager-Push
protocol.broadcast.eagerpush.fanout=5

# Hybrid Adaptive Gossip
protocol.broadcast.hybrid.eager_fanout=3
protocol.broadcast.hybrid.lazy_fanout=2
protocol.broadcast.hybrid.min_eager_fanout=2
protocol.broadcast.hybrid.max_eager_fanout=5
protocol.broadcast.hybrid.adapt_window=12

#### Application

app.payload.size=20
app.broadcast.period=3000
app.op.folder=/home/distalg06/operations
app.op.start.file=go.flag
app.of.fail.file=fail.flag
app.op.file.pool=1000
```

## Important Parameters

| Protocol   | Parameter          | Meaning                                             |
| ---------- | ------------------ | --------------------------------------------------- |
| Cyclon     | `maxN`             | Maximum partial-view size                           |
| Cyclon     | `shuffle_time`     | Time between shuffles in milliseconds               |
| Cyclon     | `subset_size`      | Number of entries exchanged                         |
| HyParView  | `active_size`      | Maximum active-view size                            |
| HyParView  | `passive_size`     | Maximum passive-view size                           |
| HyParView  | `arwl`             | Active random-walk length                           |
| HyParView  | `prwl`             | TTL at which a joining node enters the passive view |
| HyParView  | `shuffle_ttl`      | Number of overlay hops for a shuffle                |
| HyParView  | `shuffle_period`   | Time between shuffles in milliseconds               |
| Eager-Push | `fanout`           | Number of full-message targets                      |
| Hybrid     | `eager_fanout`     | Initial eager fanout                                |
| Hybrid     | `lazy_fanout`      | Number of `IHAVE` targets                           |
| Hybrid     | `min_eager_fanout` | Minimum adaptive eager fanout                       |
| Hybrid     | `max_eager_fanout` | Maximum adaptive eager fanout                       |
| Hybrid     | `adapt_window`     | Number of full-message observations per adaptation  |

## Experimental Combinations

| Broadcast              | Membership      |
| ---------------------- | --------------- |
| Flood                  | Full Membership |
| Flood                  | Cyclon          |
| Flood                  | HyParView       |
| Eager-Push             | Full Membership |
| Eager-Push             | Cyclon          |
| Eager-Push             | HyParView       |
| Hybrid Adaptive Gossip | Full Membership |
| Hybrid Adaptive Gossip | Cyclon          |
| Hybrid Adaptive Gossip | HyParView       |

## Metrics

### Reliability

```text
reliability = nodes that delivered the message
              --------------------------------
              total number of nodes
```

Report average, minimum, and maximum reliability.

### Latency

```text
latency = delivery timestamp - creation timestamp
```

Report average, minimum, and maximum latency.

### Broadcast redundancy

```text
redundancy = duplicate full-message receptions
             ---------------------------------
             first-time full-message receptions
```

For the hybrid protocol, report `IHAVE` and `REQUEST` messages separately because they are control messages rather than duplicate payload transmissions.

### Membership overhead

```text
membership overhead = total membership control messages
                      ---------------------------------
                      experiment duration in seconds
```

Count each network transmission once, preferably at the sending side. Do not sum both sender and receiver log entries for the same message.

For Cyclon, count:

- sent `ShuffleRequest`;
- sent `ShuffleReply`.

For HyParView, count:

- sent `JoinMessage`;
- sent `ForwardJoinMessage`;
- sent `DisconnectMessage`;
- sent `NeighborMessage`;
- sent `NeighborReplyMessage`;
- sent `ShuffleMessage`;
- sent `ShuffleReplyMessage`.

Full Membership normally has no periodic overlay-maintenance messages. State whether transport connections and heartbeat traffic are included or excluded.

For byte overhead:

```text
membership byte overhead = total membership-control bytes
                           ------------------------------
                           experiment duration in seconds
```

## Building and Running

The exact commands depend on the repository build configuration. For a Maven project:

```bash
mvn clean package
```

A typical experiment should:

1. create one folder per node;
2. start the bootstrap node;
3. start the remaining nodes with the correct contact address;
4. wait for the overlay to stabilize;
5. create the start flag;
6. execute the workload for the configured duration;
7. stop all processes;
8. collect `app.log` and `message.log`.

Each protocol package normally contains its protocol class, message classes, timers, requests, and notifications where applicable.

## Authors

- Alexandre Santos
- Miguel Mestre

Developed for the Distributed Algorithms course at the Department of Computer Science, NOVA School of Science and Technology.
