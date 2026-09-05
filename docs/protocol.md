# Wire protocol

ShardKV uses a length-prefixed binary protocol over TCP. Client KV calls and
Raft RPCs share the same port and framing. Integers are unsigned and
big-endian. There is no HTTP or gRPC on the node-to-node or client path.

## Frame

```
offset  size  field
0       4     payload length (bytes after this header)
4       4     message type
8       N     payload
```

Maximum payload size is `SHARD_MAX_PAYLOAD_BYTES` (default 16 MiB). Larger
frames are closed.

## Message types

| Value | Name | Direction |
|------:|------|-----------|
| 1 | `KV_GET_REQ` | client → node |
| 2 | `KV_GET_RESP` | node → client |
| 3 | `KV_PUT_REQ` | client → node |
| 4 | `KV_PUT_RESP` | node → client |
| 5 | `KV_DELETE_REQ` | client → node |
| 6 | `KV_DELETE_RESP` | node → client |
| 7 | `RAFT_VOTE_REQ` | node → node |
| 8 | `RAFT_VOTE_RESP` | node → node |
| 9 | `RAFT_APPEND_REQ` | node → node |
| 10 | `RAFT_APPEND_RESP` | node → node |
| 11 | `PING_REQ` | client → node |
| 12 | `PING_RESP` | node → client |

Strings are `uint32 length` followed by raw bytes (not NUL-terminated). They
may contain any byte, including `|`.

## Status codes

Used by KV responses:

| Value | Name | Meaning |
|------:|------|---------|
| 0 | `OK` | Request completed. For GET, `found` says whether the key exists. |
| 1 | `NOT_FOUND` | Reserved. GET uses `OK` + `found=false`. |
| 2 | `NOT_LEADER` | This node cannot accept the operation; `leaderId` may be set. |
| 3 | `TIMEOUT` | Commit wait or leadership confirmation timed out. |
| 4 | `OVERLOADED` | Bounded request queue was full; retry. |
| 5 | `ERROR` | Unexpected failure. |

## KV payloads

`KV_GET_REQ`: `key`

`KV_GET_RESP`: `status` `leaderId` `found` `value`

`KV_PUT_REQ`: `key` `value`

`KV_PUT_RESP`: `status` `leaderId`

`KV_DELETE_REQ`: `key`

`KV_DELETE_RESP`: `status` `leaderId`

`PING_REQ`: empty

`PING_RESP`: `nodeId` `leaderId` `term`  
`leaderId` is `0` if this node does not currently know a leader.

## Raft payloads

Vote and Append messages include `shardId` so a later multi-group server can
multiplex on one port. Until sharding is wired, senders set `shardId` to `0`.

`RAFT_VOTE_REQ`: `shardId` `peerId` `term` `candidateId` `lastLogIndex` `lastLogTerm` `preVote`

`RAFT_VOTE_RESP`: `shardId` `term` `voteGranted`

`RAFT_APPEND_REQ`: `shardId` `peerId` `term` `leaderId` `prevLogIndex` `prevLogTerm` `entryCount` then `term` `index` `command` per entry, then `leaderCommit`

`RAFT_APPEND_RESP`: `shardId` `term` `success` `matchIndex`

Heartbeats are AppendEntries with zero entries, which is the Raft heartbeat
mechanism. There is no separate heartbeat opcode.

## Behaviour

- Writes (`PUT`/`DELETE`) are accepted only by the leader, appended to the Raft
  log, and acknowledged after the entry is committed and applied.
- Strongly consistent reads (`GET`) are served only by a leader that has
  confirmed a quorum (`confirmLeadership`), unless `SHARD_ALLOW_STALE_READS`
  is true, in which case any node may read local state (which can be stale).
- One TCP connection is request/response and processed in order. RaftClient
  keeps one connection per peer and serializes RPCs to that peer.

## Not yet on the wire

`JOIN`, `LEAVE`, `SNAPSHOT`, `GET_METADATA`, and `CLUSTER STATUS` are planned
and not implemented. Do not document them as available.
