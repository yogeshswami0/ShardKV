#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace shard {

/**
 * A state-machine command carried in a Raft log entry.
 *
 * The encoder used to live in KVServiceImpl and the decoder was written out by
 * hand inside a lambda in main.cpp. Keeping them apart meant the two could
 * drift, and the decoder called std::stoull on unvalidated input, a malformed
 * command threw out of the apply thread and killed it.
 *
 * Wire format: op|keyLen|key|valueLen|value
 * Key and value are length-prefixed, so they may contain '|' freely.
 */
struct Command {
  enum class Type { PUT, DELETE };

  Type type = Type::PUT;
  std::string key;
  std::string value; // empty for DELETE

  static Command put(const std::string &key, const std::string &value) {
    return Command{Type::PUT, key, value};
  }

  static Command del(const std::string &key) {
    return Command{Type::DELETE, key, ""};
  }

  std::string encode() const;

  // Returns nullopt on any malformed input rather than throwing.
  static std::optional<Command> decode(const std::string &data);

  bool operator==(const Command &other) const {
    return type == other.type && key == other.key && value == other.value;
  }
};

} // namespace shard
