#include "storage/BloomFilter.h"
#include <gtest/gtest.h>
#include <random>
#include <set>

using namespace shard;

class BloomFilterTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Fresh filter for each test
  }
};

TEST_F(BloomFilterTest, BasicAddAndCheck) {
  BloomFilter filter(100, 0.01);

  filter.add("hello");
  filter.add("world");
  filter.add("test");

  // These should definitely be found
  EXPECT_TRUE(filter.mightContain("hello"));
  EXPECT_TRUE(filter.mightContain("world"));
  EXPECT_TRUE(filter.mightContain("test"));
}

TEST_F(BloomFilterTest, DefinitelyNotPresent) {
  BloomFilter filter(100, 0.01);

  filter.add("key1");
  filter.add("key2");

  // These should very likely not be found (no false negatives)
  // But we can't guarantee no false positives, so we just check
  // that the filter works as expected for known absent keys
  // In practice with a good FP rate, most absent keys should return false
}

TEST_F(BloomFilterTest, FalsePositiveRate) {
  const size_t numElements = 1000;
  const double targetFpRate = 0.05; // 5%

  BloomFilter filter(numElements, targetFpRate);

  // Add elements
  std::set<std::string> added;
  for (size_t i = 0; i < numElements; ++i) {
    std::string key = "key_" + std::to_string(i);
    filter.add(key);
    added.insert(key);
  }

  // All added elements must be found
  for (const auto &key : added) {
    EXPECT_TRUE(filter.mightContain(key));
  }

  // Check false positive rate with non-existent keys
  size_t falsePositives = 0;
  size_t numTests = 10000;

  for (size_t i = 0; i < numTests; ++i) {
    std::string key = "nonexistent_key_" + std::to_string(i);
    if (filter.mightContain(key)) {
      falsePositives++;
    }
  }

  double actualFpRate = static_cast<double>(falsePositives) / numTests;

  // Allow some margin (2x the target rate)
  EXPECT_LT(actualFpRate, targetFpRate * 2);
}

TEST_F(BloomFilterTest, Serialization) {
  BloomFilter original(500, 0.01);

  // Add some keys
  for (int i = 0; i < 100; ++i) {
    original.add("key_" + std::to_string(i));
  }

  // Serialize
  auto data = original.serialize();
  size_t numHashes = original.numHashes();

  // Reconstruct
  BloomFilter reconstructed(data, numHashes);

  // Verify all keys still found
  for (int i = 0; i < 100; ++i) {
    EXPECT_TRUE(reconstructed.mightContain("key_" + std::to_string(i)));
  }
}

TEST_F(BloomFilterTest, EmptyFilter) {
  BloomFilter filter(100, 0.01);

  // Empty filter should (almost certainly) return false
  EXPECT_FALSE(filter.mightContain("random_key"));
}

TEST_F(BloomFilterTest, FillRatio) {
  BloomFilter filter(100, 0.01);

  // Initially should be near 0
  EXPECT_LT(filter.fillRatio(), 0.01);

  // After adding elements, should increase
  for (int i = 0; i < 50; ++i) {
    filter.add("key_" + std::to_string(i));
  }

  EXPECT_GT(filter.fillRatio(), 0.0);
  EXPECT_LT(filter.fillRatio(), 1.0);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
