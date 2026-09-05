#include "net/Protocol.h"
#include <cstring>
#include <stdexcept>

namespace shard {

void Buffer::writeUint32(uint32_t v) {
  uint8_t buf[4];
  buf[0] = (v >> 24) & 0xFF;
  buf[1] = (v >> 16) & 0xFF;
  buf[2] = (v >> 8) & 0xFF;
  buf[3] = v & 0xFF;
  data_.insert(data_.end(), buf, buf + 4);
}

void Buffer::writeUint64(uint64_t v) {
  uint8_t buf[8];
  buf[0] = (v >> 56) & 0xFF;
  buf[1] = (v >> 48) & 0xFF;
  buf[2] = (v >> 40) & 0xFF;
  buf[3] = (v >> 32) & 0xFF;
  buf[4] = (v >> 24) & 0xFF;
  buf[5] = (v >> 16) & 0xFF;
  buf[6] = (v >> 8) & 0xFF;
  buf[7] = v & 0xFF;
  data_.insert(data_.end(), buf, buf + 8);
}

void Buffer::writeBool(bool v) {
  data_.push_back(v ? 1 : 0);
}

void Buffer::writeString(const std::string &v) {
  writeUint32(v.size());
  data_.insert(data_.end(), v.begin(), v.end());
}

uint32_t Buffer::readUint32() {
  if (remaining() < 4) throw std::runtime_error("Buffer underflow");
  uint32_t v = (static_cast<uint32_t>(data_[pos_]) << 24) |
               (static_cast<uint32_t>(data_[pos_ + 1]) << 16) |
               (static_cast<uint32_t>(data_[pos_ + 2]) << 8) |
               (static_cast<uint32_t>(data_[pos_ + 3]));
  pos_ += 4;
  return v;
}

uint64_t Buffer::readUint64() {
  if (remaining() < 8) throw std::runtime_error("Buffer underflow");
  uint64_t v = (static_cast<uint64_t>(data_[pos_]) << 56) |
               (static_cast<uint64_t>(data_[pos_ + 1]) << 48) |
               (static_cast<uint64_t>(data_[pos_ + 2]) << 40) |
               (static_cast<uint64_t>(data_[pos_ + 3]) << 32) |
               (static_cast<uint64_t>(data_[pos_ + 4]) << 24) |
               (static_cast<uint64_t>(data_[pos_ + 5]) << 16) |
               (static_cast<uint64_t>(data_[pos_ + 6]) << 8) |
               (static_cast<uint64_t>(data_[pos_ + 7]));
  pos_ += 8;
  return v;
}

bool Buffer::readBool() {
  if (remaining() < 1) throw std::runtime_error("Buffer underflow");
  bool v = data_[pos_] != 0;
  pos_ += 1;
  return v;
}

std::string Buffer::readString() {
  uint32_t len = readUint32();
  if (remaining() < len) throw std::runtime_error("Buffer underflow");
  std::string v(data_.begin() + pos_, data_.begin() + pos_ + len);
  pos_ += len;
  return v;
}

std::vector<uint8_t> serializeKVGetReq(const KVGetReq& m) {
  Buffer buf; buf.writeString(m.key); return buf.data();
}
KVGetReq deserializeKVGetReq(Buffer& buf) {
  return {buf.readString()};
}

const char *statusName(StatusCode code) {
  switch (code) {
  case StatusCode::OK:
    return "OK";
  case StatusCode::NOT_FOUND:
    return "NOT_FOUND";
  case StatusCode::NOT_LEADER:
    return "NOT_LEADER";
  case StatusCode::TIMEOUT:
    return "TIMEOUT";
  case StatusCode::OVERLOADED:
    return "OVERLOADED";
  case StatusCode::ERROR:
    return "ERROR";
  }
  return "UNKNOWN";
}

std::vector<uint8_t> encodeFrame(MessageType type,
                                 const std::vector<uint8_t> &payload) {
  Buffer buf;
  buf.writeUint32(static_cast<uint32_t>(payload.size()));
  buf.writeUint32(static_cast<uint32_t>(type));
  auto out = buf.data();
  out.insert(out.end(), payload.begin(), payload.end());
  return out;
}

std::vector<uint8_t> serializeKVGetResp(const KVGetResp &m) {
  Buffer buf;
  buf.writeUint32(m.status);
  buf.writeUint32(m.leaderId);
  buf.writeBool(m.found);
  buf.writeString(m.value);
  return buf.data();
}
KVGetResp deserializeKVGetResp(Buffer &buf) {
  KVGetResp m;
  m.status = buf.readUint32();
  m.leaderId = buf.readUint32();
  m.found = buf.readBool();
  m.value = buf.readString();
  return m;
}

std::vector<uint8_t> serializeKVPutReq(const KVPutReq& m) {
  Buffer buf; buf.writeString(m.key); buf.writeString(m.value); return buf.data();
}
KVPutReq deserializeKVPutReq(Buffer& buf) {
  return {buf.readString(), buf.readString()};
}

std::vector<uint8_t> serializeKVPutResp(const KVPutResp &m) {
  Buffer buf;
  buf.writeUint32(m.status);
  buf.writeUint32(m.leaderId);
  return buf.data();
}
KVPutResp deserializeKVPutResp(Buffer &buf) {
  return {buf.readUint32(), buf.readUint32()};
}

std::vector<uint8_t> serializeKVDeleteReq(const KVDeleteReq& m) {
  Buffer buf; buf.writeString(m.key); return buf.data();
}
KVDeleteReq deserializeKVDeleteReq(Buffer& buf) {
  return {buf.readString()};
}

std::vector<uint8_t> serializeKVDeleteResp(const KVDeleteResp &m) {
  Buffer buf;
  buf.writeUint32(m.status);
  buf.writeUint32(m.leaderId);
  return buf.data();
}
KVDeleteResp deserializeKVDeleteResp(Buffer &buf) {
  return {buf.readUint32(), buf.readUint32()};
}

std::vector<uint8_t> serializePingReq(const PingReq &) { return {}; }
PingReq deserializePingReq(Buffer &) { return {}; }

std::vector<uint8_t> serializePingResp(const PingResp &m) {
  Buffer buf;
  buf.writeUint32(m.nodeId);
  buf.writeUint32(m.leaderId);
  buf.writeUint64(m.term);
  return buf.data();
}
PingResp deserializePingResp(Buffer &buf) {
  return {buf.readUint32(), buf.readUint32(), buf.readUint64()};
}

std::vector<uint8_t> serializeRaftVoteReq(const RaftVoteReq& m) {
  Buffer buf;
  buf.writeUint32(m.shardId);
  buf.writeUint32(m.peerId);
  buf.writeUint64(m.term);
  buf.writeUint32(m.candidateId);
  buf.writeUint64(m.lastLogIndex);
  buf.writeUint64(m.lastLogTerm);
  buf.writeBool(m.preVote);
  return buf.data();
}
RaftVoteReq deserializeRaftVoteReq(Buffer& buf) {
  RaftVoteReq m;
  m.shardId = buf.readUint32();
  m.peerId = buf.readUint32();
  m.term = buf.readUint64();
  m.candidateId = buf.readUint32();
  m.lastLogIndex = buf.readUint64();
  m.lastLogTerm = buf.readUint64();
  m.preVote = buf.readBool();
  return m;
}

std::vector<uint8_t> serializeRaftVoteResp(const RaftVoteResp& m) {
  Buffer buf; 
  buf.writeUint32(m.shardId);
  buf.writeUint64(m.term); 
  buf.writeBool(m.voteGranted); 
  return buf.data();
}
RaftVoteResp deserializeRaftVoteResp(Buffer& buf) {
  return {buf.readUint32(), buf.readUint64(), buf.readBool()};
}

std::vector<uint8_t> serializeRaftAppendReq(const RaftAppendReq& m) {
  Buffer buf;
  buf.writeUint32(m.shardId);
  buf.writeUint32(m.peerId);
  buf.writeUint64(m.term);
  buf.writeUint32(m.leaderId);
  buf.writeUint64(m.prevLogIndex);
  buf.writeUint64(m.prevLogTerm);
  buf.writeUint32(m.entries.size());
  for (const auto& e : m.entries) {
    buf.writeUint64(e.term);
    buf.writeUint64(e.index);
    buf.writeString(e.command);
  }
  buf.writeUint64(m.leaderCommit);
  return buf.data();
}
RaftAppendReq deserializeRaftAppendReq(Buffer& buf) {
  RaftAppendReq m;
  m.shardId = buf.readUint32();
  m.peerId = buf.readUint32();
  m.term = buf.readUint64();
  m.leaderId = buf.readUint32();
  m.prevLogIndex = buf.readUint64();
  m.prevLogTerm = buf.readUint64();
  uint32_t count = buf.readUint32();
  for (uint32_t i = 0; i < count; ++i) {
    uint64_t term = buf.readUint64();
    uint64_t index = buf.readUint64();
    std::string command = buf.readString();
    m.entries.emplace_back(term, index, command);
  }
  m.leaderCommit = buf.readUint64();
  return m;
}

std::vector<uint8_t> serializeRaftAppendResp(const RaftAppendResp& m) {
  Buffer buf;
  buf.writeUint32(m.shardId);
  buf.writeUint64(m.term);
  buf.writeBool(m.success);
  buf.writeUint64(m.matchIndex);
  return buf.data();
}
RaftAppendResp deserializeRaftAppendResp(Buffer& buf) {
  return {buf.readUint32(), buf.readUint64(), buf.readBool(), buf.readUint64()};
}

} // namespace shard
