#pragma once

#include <string>

namespace shard {

/**
 * Portable durability primitives.
 *
 * The distinction that matters: a write() followed by a stream flush only moves
 * bytes into the OS page cache. They survive a process crash, but not a machine
 * crash. Only an explicit sync pushes them to stable media.
 */

/**
 * Force a file descriptor's data to stable media.
 *
 * On macOS plain fsync() only hands the data to the drive; the drive is still
 * free to hold it in a volatile write cache. F_FULLFSYNC is the only call that
 * forces it through, so that is what we use there.
 *
 * On Linux fdatasync() is preferred: it skips the metadata update when only the
 * data changed, which is the common case for an append-only log.
 *
 * @return true on success, false on error (errno is set)
 */
bool durableSync(int fd);

/**
 * Force a directory's entries to stable media.
 *
 * Creating or renaming a file is not durable until its parent directory is
 * itself synced, after a crash the file's data can be intact while the
 * directory entry pointing at it is gone.
 */
bool syncDirectory(const std::string &dirPath);

/**
 * Atomically install a file at its final path: sync the temp file's contents,
 * rename it into place, then sync the parent directory.
 *
 * rename(2) is atomic, so a reader either sees the complete old file or the
 * complete new one, never a half-written one.
 */
bool atomicInstall(const std::string &tmpPath, const std::string &finalPath);

/**
 * Directory component of a path ("." when there is no separator).
 */
std::string parentDirectory(const std::string &path);

} // namespace shard
