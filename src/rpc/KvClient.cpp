#include "rpc/KvClient.h"

namespace shard {

KvClient::KvClient() = default;
KvClient::~KvClient() = default;

bool KvClient::connect(const std::string &address) {
  std::lock_guard<std::mutex> lock(mutex_);
  return client_.connect(address);
}

void KvClient::disconnect() {
  std::lock_guard<std::mutex> lock(mutex_);
  client_.disconnect();
}

bool KvClient::isConnected() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return client_.isConnected();
}

void KvClient::setTimeout(int ms) {
  std::lock_guard<std::mutex> lock(mutex_);
  client_.setTimeout(ms);
}

bool KvClient::rpc(MessageType reqType, const std::vector<uint8_t> &payload,
                   MessageType expectedResp, Buffer &out) {
  MessageHeader reqHeader{static_cast<uint32_t>(payload.size()),
                          static_cast<uint32_t>(reqType)};
  MessageHeader respHeader{};
  std::vector<uint8_t> respPayload;
  if (!client_.sendRequest(reqHeader, payload, respHeader, respPayload)) {
    client_.disconnect();
    return false;
  }
  if (respHeader.type != static_cast<uint32_t>(expectedResp)) {
    client_.disconnect();
    return false;
  }
  out = Buffer(std::move(respPayload));
  return true;
}

KvPingResult KvClient::ping() {
  std::lock_guard<std::mutex> lock(mutex_);
  KvPingResult result;
  Buffer buf;
  if (!rpc(MessageType::PING_REQ, serializePingReq({}), MessageType::PING_RESP,
           buf)) {
    return result;
  }
  auto resp = deserializePingResp(buf);
  result.ok = true;
  result.nodeId = resp.nodeId;
  result.leaderId = resp.leaderId;
  result.term = resp.term;
  return result;
}

KvGetResult KvClient::get(const std::string &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  KvGetResult result;
  Buffer buf;
  if (!rpc(MessageType::KV_GET_REQ, serializeKVGetReq({key}),
           MessageType::KV_GET_RESP, buf)) {
    return result;
  }
  auto resp = deserializeKVGetResp(buf);
  result.status = static_cast<StatusCode>(resp.status);
  result.leaderId = resp.leaderId;
  result.found = resp.found;
  result.value = resp.value;
  return result;
}

KvWriteResult KvClient::put(const std::string &key, const std::string &value) {
  std::lock_guard<std::mutex> lock(mutex_);
  KvWriteResult result;
  Buffer buf;
  if (!rpc(MessageType::KV_PUT_REQ, serializeKVPutReq({key, value}),
           MessageType::KV_PUT_RESP, buf)) {
    return result;
  }
  auto resp = deserializeKVPutResp(buf);
  result.status = static_cast<StatusCode>(resp.status);
  result.leaderId = resp.leaderId;
  return result;
}

KvWriteResult KvClient::del(const std::string &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  KvWriteResult result;
  Buffer buf;
  if (!rpc(MessageType::KV_DELETE_REQ, serializeKVDeleteReq({key}),
           MessageType::KV_DELETE_RESP, buf)) {
    return result;
  }
  auto resp = deserializeKVDeleteResp(buf);
  result.status = static_cast<StatusCode>(resp.status);
  result.leaderId = resp.leaderId;
  return result;
}

} // namespace shard
