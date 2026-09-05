#include "net/Protocol.h"
#include <gtest/gtest.h>
#include <stdexcept>

using namespace shard;

TEST(ProtocolTest, KvRoundTrip) {
  auto getReq = serializeKVGetReq({"user/1"});
  Buffer getReqBuf(getReq);
  EXPECT_EQ(deserializeKVGetReq(getReqBuf).key, "user/1");

  KVGetResp getResp{static_cast<uint32_t>(StatusCode::OK), 2, true, "abc"};
  Buffer getRespBuf(serializeKVGetResp(getResp));
  auto got = deserializeKVGetResp(getRespBuf);
  EXPECT_EQ(got.status, static_cast<uint32_t>(StatusCode::OK));
  EXPECT_EQ(got.leaderId, 2u);
  EXPECT_TRUE(got.found);
  EXPECT_EQ(got.value, "abc");

  auto putReq = serializeKVPutReq({"k", "v|pipe"});
  Buffer putReqBuf(putReq);
  auto put = deserializeKVPutReq(putReqBuf);
  EXPECT_EQ(put.key, "k");
  EXPECT_EQ(put.value, "v|pipe");

  Buffer putRespBuf(serializeKVPutResp(
      {static_cast<uint32_t>(StatusCode::NOT_LEADER), 7}));
  auto putResp = deserializeKVPutResp(putRespBuf);
  EXPECT_EQ(putResp.status, static_cast<uint32_t>(StatusCode::NOT_LEADER));
  EXPECT_EQ(putResp.leaderId, 7u);
}

TEST(ProtocolTest, PingAndFrame) {
  auto frame = encodeFrame(MessageType::PING_REQ, {});
  ASSERT_GE(frame.size(), 8u);
  Buffer header(std::vector<uint8_t>(frame.begin(), frame.begin() + 8));
  EXPECT_EQ(header.readUint32(), 0u);
  EXPECT_EQ(header.readUint32(), static_cast<uint32_t>(MessageType::PING_REQ));

  PingResp ping{3, 3, 11};
  Buffer pingBuf(serializePingResp(ping));
  auto decoded = deserializePingResp(pingBuf);
  EXPECT_EQ(decoded.nodeId, 3u);
  EXPECT_EQ(decoded.leaderId, 3u);
  EXPECT_EQ(decoded.term, 11u);
}

TEST(ProtocolTest, BufferUnderflowThrows) {
  Buffer empty;
  EXPECT_THROW(empty.readUint32(), std::runtime_error);
  Buffer tiny{std::vector<uint8_t>{0, 0, 0, 4, 'a'}};
  EXPECT_THROW(tiny.readString(), std::runtime_error);
}

TEST(ProtocolTest, StatusNames) {
  EXPECT_STREQ(statusName(StatusCode::OK), "OK");
  EXPECT_STREQ(statusName(StatusCode::NOT_LEADER), "NOT_LEADER");
  EXPECT_STREQ(statusName(StatusCode::OVERLOADED), "OVERLOADED");
}
