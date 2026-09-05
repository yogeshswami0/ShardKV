#pragma once

#ifdef _WIN32
#include <io.h>
#include <time.h>

#define fsync _commit
using ssize_t = intptr_t;

inline struct tm* localtime_r(const time_t* timer, struct tm* buf) {
    if (localtime_s(buf, timer) == 0) return buf;
    return nullptr;
}

#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#undef ERROR

inline ssize_t pread(int fd, void *buf, size_t count, int64_t offset) {
    HANDLE h = (HANDLE)_get_osfhandle(fd);
    if (h == INVALID_HANDLE_VALUE) return -1;
    OVERLAPPED overlapped = {0};
    overlapped.Offset = (DWORD)(offset & 0xFFFFFFFF);
    overlapped.OffsetHigh = (DWORD)(offset >> 32);
    DWORD bytesRead = 0;
    if (ReadFile(h, buf, (DWORD)count, &bytesRead, &overlapped)) {
        return bytesRead;
    }
    if (GetLastError() == ERROR_HANDLE_EOF) return 0;
    return -1;
}

#else
#include <unistd.h>
#ifndef O_BINARY
#define O_BINARY 0
#endif
#endif
