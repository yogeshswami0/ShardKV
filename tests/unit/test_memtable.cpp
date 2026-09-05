#include "storage/MemTable.h"
#include <gtest/gtest.h>

using namespace shard;

class MemTableTest : public ::testing::Test {
protected:
  void SetUp() override {
    // 1 MB max size for testing
    memtable = std::make_unique<MemTable>(1024 * 1024);
  }

  std::unique_ptr<MemTable> memtable;
};

TEST_F(MemTableTest, PutAndGet) {
  memtable->put("key1", "value1", 1);
  memtable->put("key2", "value2", 2);

  auto result1 = memtable->get("key1");
  ASSERT_TRUE(result1.has_value());
  EXPECT_EQ(result1->value, "value1");
  EXPECT_EQ(result1->sequenceNumber, 1);
  EXPECT_FALSE(result1->deleted);

  auto result2 = memtable->get("key2");
  ASSERT_TRUE(result2.has_value());
  EXPECT_EQ(result2->value, "value2");
}

TEST_F(MemTableTest, UpdateExistingKey) {
  memtable->put("key", "value1", 1);
  memtable->put("key", "value2", 2);

  auto result = memtable->get("key");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->value, "value2");
  EXPECT_EQ(result->sequenceNumber, 2);
}

TEST_F(MemTableTest, DeleteKey) {
  memtable->put("key", "value", 1);
  memtable->remove("key", 2);

  auto result = memtable->get("key");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->deleted);
  EXPECT_EQ(result->sequenceNumber, 2);
}

TEST_F(MemTableTest, DeleteNonExistentKey) {
  memtable->remove("nonexistent", 1);

  auto result = memtable->get("nonexistent");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->deleted);
}

TEST_F(MemTableTest, GetNonExistentKey) {
  auto result = memtable->get("nonexistent");
  EXPECT_FALSE(result.has_value());
}

TEST_F(MemTableTest, ShouldFlush) {
  // Create small memtable
  MemTable small(100); // 100 bytes

  EXPECT_FALSE(small.shouldFlush());

  // Add data until it should flush
  std::string bigValue(50, 'x');
  small.put("key1", bigValue, 1);
  small.put("key2", bigValue, 2);

  EXPECT_TRUE(small.shouldFlush());
}

TEST_F(MemTableTest, Freeze) {
  memtable->put("key", "value", 1);
  memtable->freeze();

  EXPECT_TRUE(memtable->isFrozen());

  // Writing to frozen table should throw
  EXPECT_THROW(memtable->put("key2", "value2", 2), std::runtime_error);
}

TEST_F(MemTableTest, Iterator) {
  memtable->put("c", "3", 1);
  memtable->put("a", "1", 2);
  memtable->put("b", "2", 3);

  // Iterator should return keys in sorted order
  std::vector<std::string> keys;
  auto allEntries = memtable->getAllEntries();
  for (auto it = allEntries.begin(); it != allEntries.end(); ++it) {
    keys.push_back(it->first);
  }

  ASSERT_EQ(keys.size(), 3);
  EXPECT_EQ(keys[0], "a");
  EXPECT_EQ(keys[1], "b");
  EXPECT_EQ(keys[2], "c");
}

TEST_F(MemTableTest, Clear) {
  memtable->put("key", "value", 1);
  EXPECT_TRUE(memtable->get("key").has_value());

  memtable->clear();

  EXPECT_FALSE(memtable->get("key").has_value());
  EXPECT_EQ(memtable->entryCount(), 0);
  EXPECT_FALSE(memtable->isFrozen());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
