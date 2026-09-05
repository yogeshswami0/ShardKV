cmake_minimum_required(VERSION 3.16)
project(shard-kv VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Build options
option(BUILD_TESTS "Build test suite" ON)
option(BUILD_BENCHMARKS "Build performance benchmarks" ON)

# Sanitizers
set(SHARD_SANITIZE "none" CACHE STRING "Sanitizer: none, thread, address")
set_property(CACHE SHARD_SANITIZE PROPERTY STRINGS none thread address)

if(SHARD_SANITIZE STREQUAL "thread")
    add_compile_options(-fsanitize=thread -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=thread)
    message(STATUS "Building with ThreadSanitizer")
elseif(SHARD_SANITIZE STREQUAL "address")
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=address,undefined)
    message(STATUS "Building with AddressSanitizer + UBSan")
elseif(NOT SHARD_SANITIZE STREQUAL "none")
    message(FATAL_ERROR "SHARD_SANITIZE must be none, thread, or address")
endif()

find_package(Threads REQUIRED)

if(MSVC)
    add_compile_options(/W3)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# Core library (storage + raft + net)
set(CORE_SOURCES
    src/config/Config.cpp
    src/util/Logger.cpp
    src/util/Crc32.cpp
    src/util/Durability.cpp
    src/util/ThreadPool.cpp
    src/storage/BloomFilter.cpp
    src/storage/MemTable.cpp
    src/storage/WAL.cpp
    src/storage/SSTableWriter.cpp
    src/storage/SSTableReader.cpp
    src/storage/Compaction.cpp
    src/storage/StorageEngine.cpp
    src/raft/RaftLog.cpp
    src/raft/Command.cpp
    src/raft/Election.cpp
    src/raft/Replication.cpp
    src/raft/RaftNode.cpp
    src/net/Protocol.cpp
    src/net/TcpServer.cpp
    src/net/TcpClient.cpp
    src/rpc/Server.cpp
    src/rpc/RaftClient.cpp
    src/rpc/KvClient.cpp
)

add_library(shard_core STATIC ${CORE_SOURCES})
target_include_directories(shard_core PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)
target_link_libraries(shard_core PUBLIC
    Threads::Threads
)
if(WIN32)
    target_link_libraries(shard_core PUBLIC ws2_32)
endif()

# Main executable
add_executable(shard-kv src/main.cpp)
target_link_libraries(shard-kv PRIVATE shard_core)

# Tests
if(BUILD_TESTS)
    enable_testing()
    
    include(FetchContent)
    FetchContent_Declare(
      googletest
      URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
    )
    # For Windows: Prevent overriding the parent project's compiler/linker settings
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    add_executable(test_bloom_filter tests/unit/test_bloom_filter.cpp)
    target_link_libraries(test_bloom_filter PRIVATE shard_core gtest_main)
    add_test(NAME BloomFilterTest COMMAND test_bloom_filter)
    
    add_executable(test_memtable tests/unit/test_memtable.cpp)
    target_link_libraries(test_memtable PRIVATE shard_core gtest_main)
    add_test(NAME MemTableTest COMMAND test_memtable)
    
    if(NOT WIN32)
      add_executable(test_wal tests/unit/test_wal.cpp)
      target_link_libraries(test_wal PRIVATE shard_core gtest_main)
      add_test(NAME WALTest COMMAND test_wal)
    endif()
    
    add_executable(test_sstable tests/unit/test_sstable.cpp)
    target_link_libraries(test_sstable PRIVATE shard_core gtest_main)
    add_test(NAME SSTableTest COMMAND test_sstable)
    
    add_executable(test_crash_recovery tests/integration/test_crash_recovery.cpp)
    target_link_libraries(test_crash_recovery PRIVATE shard_core gtest_main)
    add_test(NAME CrashRecoveryTest COMMAND test_crash_recovery)
    
    add_executable(test_compaction tests/unit/test_compaction.cpp)
    target_link_libraries(test_compaction PRIVATE shard_core gtest_main)
    add_test(NAME CompactionTest COMMAND test_compaction)
    
    add_executable(test_raft_log tests/unit/test_raft_log.cpp)
    target_link_libraries(test_raft_log PRIVATE shard_core gtest_main)
    add_test(NAME RaftLogTest COMMAND test_raft_log)
    
    add_executable(test_raft_state tests/unit/test_raft_state.cpp)
    target_link_libraries(test_raft_state PRIVATE shard_core gtest_main)
    add_test(NAME RaftHardStateTest COMMAND test_raft_state)
    
    add_executable(test_raft_cluster tests/unit/test_raft_cluster.cpp)
    target_link_libraries(test_raft_cluster PRIVATE shard_core gtest_main)
    add_test(NAME RaftClusterTest COMMAND test_raft_cluster)
    
    add_executable(test_command tests/unit/test_command.cpp)
    target_link_libraries(test_command PRIVATE shard_core gtest_main)
    add_test(NAME CommandTest COMMAND test_command)
    
    add_executable(test_storage_engine tests/integration/test_storage_engine.cpp)
    target_link_libraries(test_storage_engine PRIVATE shard_core gtest_main)
    add_test(NAME StorageEngineTest COMMAND test_storage_engine)

    add_executable(test_protocol tests/unit/test_protocol.cpp)
    target_link_libraries(test_protocol PRIVATE shard_core gtest_main)
    add_test(NAME ProtocolTest COMMAND test_protocol)

    add_executable(test_tcp_kv tests/integration/test_tcp_kv.cpp)
    target_link_libraries(test_tcp_kv PRIVATE shard_core gtest_main)
    add_test(NAME TcpKvTest COMMAND test_tcp_kv)
endif()

# Benchmarks
if(BUILD_BENCHMARKS)
    if(EXISTS "${CMAKE_SOURCE_DIR}/tests/benchmark/bench_throughput.cpp")
        add_executable(bench_throughput tests/benchmark/bench_throughput.cpp)
        target_link_libraries(bench_throughput PRIVATE shard_core)
    else()
        message(STATUS "Benchmark source not found, skipping")
    endif()
endif()

# Installation
install(TARGETS shard-kv DESTINATION bin)
