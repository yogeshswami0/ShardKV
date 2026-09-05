#include "raft/Command.h"

#include <cstdlib>

namespace shard {

namespace {

// Parses a decimal length field terminated by '|'. Returns false on anything
// that is not a plain non-negative number within bounds.
bool parseLength(const std::string &data, size_t &pos, size_t &out) {
  size_t pipe = data.find('|', pos);
  if (pipe == std::string::npos || pipe == pos) {
    return false;
  }

  size_t value = 0;
  for (size_t i = pos; i < pipe; ++i) {
    char c = data[i];
    if (c < '0' || c > '9') {
      return false;
    }
    // Guard against overflow before it happens.
    if (value > (SIZE_MAX - static_cast<size_t>(c - '0')) / 10) {
      return false;
    }
    value = value * 10 + static_cast<size_t>(c - '0');
  }

  if (value > data.size()) {
    return false; // cannot possibly fit
  }

  out = value;
  pos = pipe + 1;
  return true;
}

} // namespace

std::string Command::encode() const {
  std::string out;
  out.reserve(16 + key.size() + value.size());

  out.append(type == Type::PUT ? "PUT" : "DELETE");
  out.push_back('|');
  out.append(std::to_string(key.size()));
  out.push_back('|');
  out.append(key);
  out.push_back('|');
  out.append(std::to_string(value.size()));
  out.push_back('|');
  out.append(value);

  return out;
}

std::optional<Command> Command::decode(const std::string &data) {
  size_t pos = 0;

  size_t pipe = data.find('|', pos);
  if (pipe == std::string::npos) {
    return std::nullopt;
  }

  std::string op = data.substr(pos, pipe - pos);
  pos = pipe + 1;

  Command cmd;
  if (op == "PUT") {
    cmd.type = Type::PUT;
  } else if (op == "DELETE") {
    cmd.type = Type::DELETE;
  } else {
    return std::nullopt;
  }

  size_t keyLen = 0;
  if (!parseLength(data, pos, keyLen)) {
    return std::nullopt;
  }
  if (pos + keyLen > data.size()) {
    return std::nullopt;
  }
  cmd.key = data.substr(pos, keyLen);
  pos += keyLen;

  // Separator between key and the value length.
  if (pos >= data.size() || data[pos] != '|') {
    return std::nullopt;
  }
  pos++;

  size_t valueLen = 0;
  if (!parseLength(data, pos, valueLen)) {
    return std::nullopt;
  }
  if (pos + valueLen > data.size()) {
    return std::nullopt;
  }
  cmd.value = data.substr(pos, valueLen);

  return cmd;
}

} // namespace shard
