#pragma once

#include "raft/RaftLog.h"
#include <cstdint>
#include <string>
#include <vector>

namespace shard {

enum class MessageType : uint32_t {
  KV_GET_REQ = 1,
  KV_GET_RESP = 2,
  KV_PUT_REQ = 3,
  KV_PUT_RESP = 4,
  KV_DELETE_REQ = 5,
  KV_DELETE_RESP = 6,

  RAFT_VOTE_REQ = 7,
  RAFT_VOTE_RESP = 8,
  RAFT_APPEND_REQ = 9,
  RAFT_APPEND_RESP = 10,

  PING_REQ = 11,
  PING_RESP = 12,
};

enum class StatusCode : uint32_t {
  OK = 0,
  NOT_FOUND = 1,
  NOT_LEADER = 2,
  TIMEOUT = 3,
  OVERLOADED = 4,
  ERROR = 5,
};

const char *statusName(StatusCode code);

// 8-byte frame: big-endian payload length, then message type.
struct MessageHeader {
  uint32_t length;
  uint32_t type;
};

constexpr uint32_t kHeaderBytes = 8;
constexpr uint32_t kDefaultMaxPayloadBytes = 16 * 1024 * 1024;

class Buffer {
public:
  Buffer() = default;
  Buffer(const std::vector<uint8_t> &data) : data_(data), pos_(0) {}
  Buffer(std::vector<uint8_t> &&data) : data_(std::move(data)), pos_(0) {}

  void writeUint32(uint32_t v);
  void writeUint64(uint64_t v);
  void writeBool(bool v);
  void writeString(const std::string &v);

  uint32_t readUint32();
  uint64_t readUint64();
  bool readBool();
  std::string readString();

  const std::vector<uint8_t> &data() const { return data_; }
  size_t remaining() const { return data_.size() - pos_; }

private:
  std::vector<uint8_t> data_;
  size_t pos_ = 0;
};

std::vector<uint8_t> encodeFrame(MessageType type,
                                 const std::vector<uint8_t> &payload);

struct KVGetReq {
  std::string key;
};
struct KVGetResp {
  uint32_t status;
  uint32_t leaderId;
  bool found;
  std::string value;
};
struct KVPutReq {
  std::string key;
  std::string value;
};
struct KVPutResp {
  uint32_t status;
  uint32_t leaderId;
};
struct KVDeleteReq {
  std::string key;
};
struct KVDeleteResp {
  uint32_t status;
  uint32_t leaderId;
};

struct PingReq {};
struct PingResp {
  uint32_t nodeId;
  uint32_t leaderId; // 0 if unknown
  uint64_t term;
};

struct RaftVoteReq {
  uint32_t shardId;
  uint32_t peerId;
  uint64_t term;
  uint32_t candidateId;
  uint64_t lastLogIndex;
  uint64_t lastLogTerm;
  bool preVote;
};

struct RaftVoteResp {
  uint32_t shardId;
  uint64_t term;
  bool voteGranted;
};

struct RaftAppendReq {
  uint32_t shardId;
  uint32_t peerId;
  uint64_t term;
  uint32_t leaderId;
  uint64_t prevLogIndex;
  uint64_t prevLogTerm;
  std::vector<RaftLogEntry> entries;
  uint64_t leaderCommit;
};

struct RaftAppendResp {
  uint32_t shardId;
  uint64_t term;
  bool success;
  uint64_t matchIndex;
};

std::vector<uint8_t> serializeKVGetReq(const KVGetReq &m);
KVGetReq deserializeKVGetReq(Buffer &buf);

std::vector<uint8_t> serializeKVGetResp(const KVGetResp &m);
KVGetResp deserializeKVGetResp(Buffer &buf);

std::vector<uint8_t> serializeKVPutReq(const KVPutReq &m);
KVPutReq deserializeKVPutReq(Buffer &buf);

std::vector<uint8_t> serializeKVPutResp(const KVPutResp &m);
KVPutResp deserializeKVPutResp(Buffer &buf);

std::vector<uint8_t> serializeKVDeleteReq(const KVDeleteReq &m);
KVDeleteReq deserializeKVDeleteReq(Buffer &buf);

std::vector<uint8_t> serializeKVDeleteResp(const KVDeleteResp &m);
KVDeleteResp deserializeKVDeleteResp(Buffer &buf);

std::vector<uint8_t> serializePingReq(const PingReq &m);
PingReq deserializePingReq(Buffer &buf);

std::vector<uint8_t> serializePingResp(const PingResp &m);
PingResp deserializePingResp(Buffer &buf);

std::vector<uint8_t> serializeRaftVoteReq(const RaftVoteReq &m);
RaftVoteReq deserializeRaftVoteReq(Buffer &buf);

std::vector<uint8_t> serializeRaftVoteResp(const RaftVoteResp &m);
RaftVoteResp deserializeRaftVoteResp(Buffer &buf);

std::vector<uint8_t> serializeRaftAppendReq(const RaftAppendReq &m);
RaftAppendReq deserializeRaftAppendReq(Buffer &buf);

std::vector<uint8_t> serializeRaftAppendResp(const RaftAppendResp &m);
RaftAppendResp deserializeRaftAppendResp(Buffer &buf);

} // namespace shard
