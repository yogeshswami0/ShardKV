#include "raft/Command.h"
#include <gtest/gtest.h>

using namespace shard;

TEST(CommandTest, PutRoundTrips) {
  auto cmd = Command::put("mykey", "myvalue");
  auto decoded = Command::decode(cmd.encode());

  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->type, Command::Type::PUT);
  EXPECT_EQ(decoded->key, "mykey");
  EXPECT_EQ(decoded->value, "myvalue");
}

TEST(CommandTest, DeleteRoundTrips) {
  auto decoded = Command::decode(Command::del("gone").encode());

  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->type, Command::Type::DELETE);
  EXPECT_EQ(decoded->key, "gone");
  EXPECT_TRUE(decoded->value.empty());
}

// The format is length-prefixed precisely so the delimiter may appear in the
// data. A split-on-'|' parser would corrupt all of these.
TEST(CommandTest, DelimitersInsideKeyAndValueSurvive) {
  auto cmd = Command::put("key|with|pipes", "value|with|pipes|too");
  auto decoded = Command::decode(cmd.encode());

  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->key, "key|with|pipes");
  EXPECT_EQ(decoded->value, "value|with|pipes|too");
}

TEST(CommandTest, EmptyKeyAndValueRoundTrip) {
  auto decoded = Command::decode(Command::put("", "").encode());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_TRUE(decoded->key.empty());
  EXPECT_TRUE(decoded->value.empty());
}

TEST(CommandTest, BinaryDataRoundTrips) {
  std::string binary("a\0b\1c\xff", 6);
  auto decoded = Command::decode(Command::put(binary, binary).encode());

  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->key, binary);
  EXPECT_EQ(decoded->value, binary);
}

TEST(CommandTest, LargeValueRoundTrips) {
  std::string large(100000, 'q');
  auto decoded = Command::decode(Command::put("k", large).encode());
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->value, large);
}

// Malformed input must be reported, not thrown. The previous decoder called
// std::stoull inline in the apply callback, so a bad command threw out of the
// apply thread and stopped the state machine permanently.
TEST(CommandTest, MalformedInputIsRejectedWithoutThrowing) {
  const std::vector<std::string> bad = {
      "",
      "PUT",
      "PUT|",
      "PUT|abc|key|0|",          // non-numeric length
      "PUT|999999|key|0|",       // length past end of buffer
      "BOGUS|1|k|1|v",           // unknown op
      "PUT|1|k",                 // truncated
      "PUT|-1|k|0|",             // negative length
      "PUT|99999999999999999999999999|k|0|", // overflows
      "|1|k|1|v",                // empty op
  };

  for (const auto &input : bad) {
    EXPECT_NO_THROW({
      auto decoded = Command::decode(input);
      EXPECT_FALSE(decoded.has_value()) << "accepted malformed input: " << input;
    }) << "threw on input: "
       << input;
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
