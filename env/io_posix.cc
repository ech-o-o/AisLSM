//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef ROCKSDB_LIB_IO_POSIX
#include "env/io_posix.h"

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>

#include <algorithm>
#if defined(OS_LINUX)
#include <linux/fs.h>
#ifndef FALLOC_FL_KEEP_SIZE
#include <linux/falloc.h>
#endif
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#ifdef OS_LINUX
#include <sys/statfs.h>
#include <sys/sysmacros.h>
#include <sched.h>
#endif
#include "monitoring/iostats_context_imp.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/slice.h"
#include "test_util/sync_point.h"
#include "util/autovector.h"
#include "util/coding.h"
#include "util/string_util.h"
#include "rocksdb/file_system.h"
#include "db/version_set.h"
#include <chrono>
#if defined(OS_LINUX) && !defined(F_SET_RW_HINT)
#define F_LINUX_SPECIFIC_BASE 1024
#define F_SET_RW_HINT (F_LINUX_SPECIFIC_BASE + 12)
#endif

namespace ROCKSDB_NAMESPACE {

extern Urings urings;
extern std::atomic<int> uring_counter;

//zl: Add some functions 
bool Urings::init_queues(uint16_t compaction_num, uint8_t log_num, uint16_t compaction_depth, uint8_t log_depth)
{
  int init_lib = true;
  this->compaction_queue_size = compaction_num;
  this->log_queue_size = log_num;
  this->compaction_queue_depth = compaction_depth;
  this->log_queue_depth = log_depth;
  // FIX(bug#9): value-initialize both arrays (trailing "()") so unfilled slots
  // are nullptr, not garbage -- clear_all() dereferenced indeterminate
  // pointers when io_uring_queue_init failed partway (e.g. kernel without
  // io_uring), crashing at DB open.
  this->compaction_urings = new struct uring_queue* [compaction_num]();
  this->log_urings = new struct uring_queue* [log_num]();
  for(uint16_t i = 0; i < compaction_num; ++i)
    {
      struct uring_queue* qptr;
      qptr = new struct uring_queue();
      qptr->running = false;
      qptr->id = i;
      qptr->sync_count = 0;
      if(io_uring_queue_init(this->compaction_queue_depth, &(qptr->uring), 0) != 0)
      {
        delete qptr;  // FIX(bug#9): was leaked on the failure path
        init_lib = false;
        break;
      }
      compaction_urings[i] = qptr;
      //compaction_urings->push(std::move(qptr));
    }
    // Failed to init, then delete all elements.
    if(!init_lib)
    {
      clear_all(uring_type::uring_compaction_type);
      clear_all(uring_type::uring_log_type);  // FIX(bug#9): free the log array too
      return false;
    }
    for(uint8_t i = 0; i < log_num; ++i)
    {
      struct uring_queue* qptr;
      qptr = new struct uring_queue();
      qptr->running = false;
      qptr->sync_count = 0;
      qptr->id = i;
      if(io_uring_queue_init(this->log_queue_depth, &(qptr->uring), 0) != 0)
      {
        delete qptr;  // FIX(bug#9): was leaked on the failure path
        init_lib = false;
        break;
      }
      log_urings[i] = qptr;
    }
    if(!init_lib)
    {
      clear_all(uring_type::uring_log_type);
      clear_all(uring_type::uring_compaction_type);  // FIX(bug#9): free both
      return false;
    }
    this->init = true;
    return true;
}
struct uring_queue* Urings::get_empty_element(uint32_t id)
{
  // FIX (bug #1, data race): the original code did a NON-atomic check-then-set
  //   while(uptr->running) { ... }  uptr->running.store(true);
  // The load (in the while) and the store(true) are separate atomic ops with no
  // compare_exchange between them, so two concurrent compaction threads (Run()
  // holds no DB mutex_) could both observe the same slot free and both claim it,
  // ending up sharing one io_uring / fds vector / sync_count -> heap corruption,
  // lost fsyncs, double-close. (It also always started at slot 0, concentrating
  // contention there.)
  // Fix: claim a slot with a single lock-free compare_exchange that atomically
  // transitions free(false) -> running(true); only one thread can win. Start
  // probing at id%size to spread load across slots. No mutex on this hot path.
  const uint32_t size = this->compaction_queue_size;
  uint32_t counter_for_while = 0;
  while(true)
  {
    uint32_t index = (id + counter_for_while) % size;
    struct uring_queue* uptr = this->compaction_urings[index];
    // FIX(bug#5): quarantine -- never reuse a queue whose async fsync failed;
    // its inputs must stay unreclaimed so durability is preserved.
    if(!uptr->sync_failed.load(std::memory_order_acquire))
    {
      bool expected = false;
      if(uptr->running.compare_exchange_strong(expected, true,
             std::memory_order_acq_rel, std::memory_order_relaxed))
      {
        uptr->producer_done.store(false, std::memory_order_release);
        uptr->job_id.store(id, std::memory_order_release);
        return uptr;
      }
      if(counter_for_while > 64 &&
         uptr->producer_done.load(std::memory_order_acquire))
      {
        // FIX(bug#7): all probed slots busy. Reclaim ("steal") this one ONLY if
        // its producer has finished submitting (producer_done), and do the
        // drain under urings.mtx so it cannot race with the legitimate
        // consumer's wait_for_queue in CompactionJob::Run (which also holds
        // urings.mtx). The old code drained an actively-producing queue with no
        // lock, reaping its CQEs and closing its fds mid-flight.
        std::lock_guard<std::mutex> lk(this->mtx);
        this->wait_for_queue(uptr);
        expected = false;
        if(uptr->running.compare_exchange_strong(expected, true,
               std::memory_order_acq_rel, std::memory_order_relaxed))
        {
          uptr->producer_done.store(false, std::memory_order_release);
          uptr->job_id.store(id, std::memory_order_release);
          return uptr;
        }
      }
    }
    counter_for_while += 1;
    if(counter_for_while > 64)
    {
      sched_yield();  // all slots busy: yield instead of burning the core
    }
  }
}


struct uring_queue* Urings::get_empty_element_for_log()
{
  // FIX(bug#3): the old fallback reaped a CQE from a still-busy ring and
  // reused it. That stole the exact completion a flush thread (builder.cc)
  // was counting on -> the flush blocked forever in io_uring_wait_cqe, and
  // two threads reaped one single-consumer completion queue with no lock.
  // New logic: claim ONLY a genuinely free ring via compare-and-swap; if all
  // rings are busy, yield and retry. This waits for a flush to release a ring
  // (builder.cc stores running=false after draining), which the kernel always
  // eventually enables, so there is no deadlock -- just backpressure, which is
  // the correct behavior when memtables are sealed faster than flushes drain.
  uint32_t lap = 0;
  while(true)
  {
    for(uint16_t i = 0; i < this->log_queue_size; ++i)
    {
      struct uring_queue* uptr = this->log_urings[i];
      bool expected = false;
      if(!uptr->sync_failed.load(std::memory_order_acquire) &&
         uptr->running.compare_exchange_strong(expected, true,
             std::memory_order_acq_rel, std::memory_order_relaxed))
      {
        uptr->producer_done.store(false, std::memory_order_release);
        return uptr;
      }
    }
    lap += 1;
    if(lap > 4)
    {
      sched_yield();  // all log rings in flight: wait for a flush to release one
    }
  }
}

void Urings::clear_all(uring_type queue_type)
{
  switch(queue_type)
  {
    case uring_type::uring_compaction_type:
      if(this->compaction_urings)
      {
        for(int i = 0; i < this->compaction_queue_size; ++i)
        {
          if(!this->compaction_urings[i]) continue;  // FIX(bug#9): partial init
          this->compaction_urings[i]->store_filenumber.clear();
          io_uring_queue_exit(&(this->compaction_urings[i]->uring));
          delete this->compaction_urings[i];  // FIX: per-slot object was leaked
        }
        delete[] this->compaction_urings;  // FIX: was scalar delete on new[] (UB)
        this->compaction_urings = nullptr;
        this->compaction_queue_size = 0;
        this->compaction_queue_depth = 0;
        this->init = false;
        this->reserve_input.clear();
      }
      break;
    case uring_type::uring_log_type:
      if(this->log_urings)  // FIX: guard against null array (e.g. partial init)
      {
        for(int i = 0; i < this->log_queue_size; ++i)
        {
          if(!this->log_urings[i]) continue;  // FIX(bug#9): partial init
          io_uring_queue_exit(&(this->log_urings[i]->uring));
          delete this->log_urings[i];  // FIX: per-slot object was leaked
        }
        delete[] this->log_urings;  // FIX: was scalar delete on new[] (UB)
        this->log_urings = nullptr;
        this->log_queue_size = 0;
        this->log_queue_depth = 0;
      }
      break;
    default:
      break;
  }
}


/* Drain all in-flight fsync completions of this queue, close the deferred
 * fds, promote the queue's recorded input files for deletion, and release the
 * queue for reuse.
 * CONTRACT: the caller MUST hold urings.mtx. Both call sites comply -- the
 * consumer loop in CompactionJob::Run (lock_guard at its top) and the reclaim
 * fallback in get_empty_element. Holding urings.mtx serializes concurrent
 * drains of the same single-consumer completion queue and protects the
 * reserve_input/no_ref/ToBeDeteleted maps mutated below (same discipline as
 * PurgeObsoleteFiles in db_impl_files.cc). Lock order: urings.mtx -> qmtx. */
struct uring_queue* Urings::wait_for_queue(struct uring_queue* uptr)
{
  if(!uptr->running) return uptr;
  {
    std::lock_guard<std::mutex> qlk(uptr->qmtx);
    // FIX(bug#7, TOCTOU closure): re-validate UNDER qmtx that the producer has
    // finished. The steal path checks producer_done before locking, but the
    // queue may have been released and re-claimed in between; qmtx's acquire
    // pairs with the producer's release in ASync/AFsync, so this load cannot
    // be stale in the dangerous direction. Draining a queue whose producer is
    // still submitting is wrong for every caller, so bail out universally.
    if(!uptr->producer_done.load(std::memory_order_acquire))
    {
      return uptr;
    }
    if(uptr->sync_count > 0)
    {
      struct io_uring_cqe* cqe;
      for(uint32_t i = 0; i < uptr->sync_count; ++i)
      {
        int ret = io_uring_wait_cqe(&uptr->uring, &cqe);
        if(ret < 0)
        {
          // FIX(bug#5): surface, don't swallow. The queue stays running=true
          // and sync_failed=true -> quarantined (never claimed again), so its
          // recorded inputs below are never promoted for deletion.
          printf("invalid io_uring_wait_cqe\n");
          uptr->sync_failed.store(true, std::memory_order_release);
          return nullptr;
        }
        // FIX(bug#5): a completed-but-FAILED async fsync (res<0, e.g. EIO)
        // must not be treated as durable. Read res before cqe_seen recycles it.
        int res = cqe->res;
        io_uring_cqe_seen(&uptr->uring, cqe);
        if(res < 0)
        {
          fprintf(stderr,
                  "Pome: async fsync failed, cqe->res=%d (job_id=%u)\n",
                  res, uptr->job_id.load(std::memory_order_relaxed));
          uptr->sync_failed.store(true, std::memory_order_release);
        }
      }
      uptr->sync_count = 0;

      while(!uptr->fds.empty())
      {
        int fd = uptr->fds.back();
        close(fd);
        uptr->fds.pop_back();
      }
    }
  }
  if(uptr->sync_failed.load(std::memory_order_acquire))
  {
    // Quarantine: keep running=true and keep store_filenumber unpromoted so
    // the parental SSTs of this queue's outputs are retained (durability net).
    return uptr;
  }
  // FIX(bug#11): promote this queue's recorded input files NOW that all of its
  // fsyncs are confirmed complete, instead of relying on a future compaction
  // whose job_id guard rarely matches (queues are reused and job_id gets
  // overwritten long before bottom-level outputs are consumed). The old
  // behavior left reserve_input/no_ref growing without bound and obsolete SSTs
  // unreclaimed until shutdown. Caller holds urings.mtx (see contract).
  for(auto it = uptr->store_filenumber.begin();
      it != uptr->store_filenumber.end();)
  {
    auto nr = no_ref.find(*it);
    if(nr != no_ref.end())
    {
      ToBeDeteleted.insert(std::make_pair(nr->first, std::move(nr->second)));
      no_ref.erase(nr);
    }
    auto ri = reserve_input.find(*it);
    if(ri != reserve_input.end())
    {
      reserve_input.erase(ri);
    }
    it = uptr->store_filenumber.erase(it);
  }
  if(uptr->fds.empty())
  {
    uptr->running.store(false);
  }

  return uptr;
}
float Urings::getWalWriteSpeed(){
  auto start = std::chrono::steady_clock::now();
  float time = std::chrono::duration_cast<std::chrono::milliseconds>(start - this->now).count();
  return this->WalWriteByte / time;
}
void Urings::resetWalWriteSpeed(){
  float speed = getWalWriteSpeed();
  this->now = std::chrono::steady_clock::now();
  this->WalWriteByte = 0;
  
}
  void Urings::setUpArray(size_t num){
  arrayNum = num;
}
float Urings::getTopXSpeed(){
  float sum = 0;
  for(auto e: this->SpeedArray){
    sum += e;
  }
  return sum / this->SpeedArray.size();
}
//  = std::chrono::system_clock::now();
std::string IOErrorMsg(const std::string& context,
                       const std::string& file_name) {
  if (file_name.empty()) {
    return context;
  }
  return context + ": " + file_name;
}

// file_name can be left empty if it is not unkown.
IOStatus IOError(const std::string& context, const std::string& file_name,
                 int err_number) {
  switch (err_number) {
    case ENOSPC: {
      IOStatus s = IOStatus::NoSpace(IOErrorMsg(context, file_name),
                                     errnoStr(err_number).c_str());
      s.SetRetryable(true);
      return s;
    }
    case ESTALE:
      return IOStatus::IOError(IOStatus::kStaleFile);
    case ENOENT:
      return IOStatus::PathNotFound(IOErrorMsg(context, file_name),
                                    errnoStr(err_number).c_str());
    default:
      return IOStatus::IOError(IOErrorMsg(context, file_name),
                               errnoStr(err_number).c_str());
  }
}

// A wrapper for fadvise, if the platform doesn't support fadvise,
// it will simply return 0.
int Fadvise(int fd, off_t offset, size_t len, int advice) {
#ifdef OS_LINUX
  return posix_fadvise(fd, offset, len, advice);
#else
  (void)fd;
  (void)offset;
  (void)len;
  (void)advice;
  return 0;  // simply do nothing.
#endif
}

// A wrapper for fadvise, if the platform doesn't support fadvise,
// it will simply return 0.
int Madvise(void* addr, size_t len, int advice) {
#ifdef OS_LINUX
  return posix_madvise(addr, len, advice);
#else
  (void)addr;
  (void)len;
  (void)advice;
  return 0;  // simply do nothing.
#endif
}

namespace {

// On MacOS (and probably *BSD), the posix write and pwrite calls do not support
// buffers larger than 2^31-1 bytes. These two wrappers fix this issue by
// cutting the buffer in 1GB chunks. We use this chunk size to be sure to keep
// the writes aligned.

bool PosixWrite(int fd, const char* buf, size_t nbyte) {
  const size_t kLimit1Gb = 1UL << 30;

  const char* src = buf;
  size_t left = nbyte;

  while (left != 0) {
    size_t bytes_to_write = std::min(left, kLimit1Gb);

    ssize_t done = write(fd, src, bytes_to_write);
    if (done < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    left -= done;
    src += done;
  }
  return true;
}

bool PosixPositionedWrite(int fd, const char* buf, size_t nbyte, off_t offset) {
  const size_t kLimit1Gb = 1UL << 30;

  const char* src = buf;
  size_t left = nbyte;

  while (left != 0) {
    size_t bytes_to_write = std::min(left, kLimit1Gb);

    ssize_t done = pwrite(fd, src, bytes_to_write, offset);
    if (done < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    left -= done;
    offset += done;
    src += done;
  }

  return true;
}

#ifdef ROCKSDB_RANGESYNC_PRESENT

#if !defined(ZFS_SUPER_MAGIC)
// The magic number for ZFS was not exposed until recently. It should be fixed
// forever so we can just copy the magic number here.
#define ZFS_SUPER_MAGIC 0x2fc12fc1
#endif

bool IsSyncFileRangeSupported(int fd) {
  // This function tracks and checks for cases where we know `sync_file_range`
  // definitely will not work properly despite passing the compile-time check
  // (`ROCKSDB_RANGESYNC_PRESENT`). If we are unsure, or if any of the checks
  // fail in unexpected ways, we allow `sync_file_range` to be used. This way
  // should minimize risk of impacting existing use cases.
  struct statfs buf;
  int ret = fstatfs(fd, &buf);
  assert(ret == 0);
  if (ret == 0 && buf.f_type == ZFS_SUPER_MAGIC) {
    // Testing on ZFS showed the writeback did not happen asynchronously when
    // `sync_file_range` was called, even though it returned success. Avoid it
    // and use `fdatasync` instead to preserve the contract of `bytes_per_sync`,
    // even though this'll incur extra I/O for metadata.
    return false;
  }

  ret = sync_file_range(fd, 0 /* offset */, 0 /* nbytes */, 0 /* flags */);
  assert(!(ret == -1 && errno != ENOSYS));
  if (ret == -1 && errno == ENOSYS) {
    // `sync_file_range` is not implemented on all platforms even if
    // compile-time checks pass and a supported filesystem is in-use. For
    // example, using ext4 on WSL (Windows Subsystem for Linux),
    // `sync_file_range()` returns `ENOSYS`
    // ("Function not implemented").
    return false;
  }
  // None of the known cases matched, so allow `sync_file_range` use.
  return true;
}

#undef ZFS_SUPER_MAGIC

#endif  // ROCKSDB_RANGESYNC_PRESENT

}  // anonymous namespace

/*
 * PosixSequentialFile
 */
PosixSequentialFile::PosixSequentialFile(const std::string& fname, FILE* file,
                                         int fd, size_t logical_block_size,
                                         const EnvOptions& options)
    : filename_(fname),
      file_(file),
      fd_(fd),
      use_direct_io_(options.use_direct_reads),
      logical_sector_size_(logical_block_size) {
  assert(!options.use_direct_reads || !options.use_mmap_reads);
}

PosixSequentialFile::~PosixSequentialFile() {
  if (!use_direct_io()) {
    assert(file_);
    fclose(file_);
  } else {
    assert(fd_);
    close(fd_);
  }
}

IOStatus PosixSequentialFile::Read(size_t n, const IOOptions& /*opts*/,
                                   Slice* result, char* scratch,
                                   IODebugContext* /*dbg*/) {
  assert(result != nullptr && !use_direct_io());
  IOStatus s;
  size_t r = 0;
  do {
    clearerr(file_);
    r = fread_unlocked(scratch, 1, n, file_);
  } while (r == 0 && ferror(file_) && errno == EINTR);
  *result = Slice(scratch, r);
  if (r < n) {
    if (feof(file_)) {
      // We leave status as ok if we hit the end of the file
      // We also clear the error so that the reads can continue
      // if a new data is written to the file
      clearerr(file_);
    } else {
      // A partial read with an error: return a non-ok status
      s = IOError("While reading file sequentially", filename_, errno);
    }
  }
  return s;
}

IOStatus PosixSequentialFile::PositionedRead(uint64_t offset, size_t n,
                                             const IOOptions& /*opts*/,
                                             Slice* result, char* scratch,
                                             IODebugContext* /*dbg*/) {
  assert(use_direct_io());
  assert(IsSectorAligned(offset, GetRequiredBufferAlignment()));
  assert(IsSectorAligned(n, GetRequiredBufferAlignment()));
  assert(IsSectorAligned(scratch, GetRequiredBufferAlignment()));

  IOStatus s;
  ssize_t r = -1;
  size_t left = n;
  char* ptr = scratch;
  while (left > 0) {
    r = pread(fd_, ptr, left, static_cast<off_t>(offset));
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ptr += r;
    offset += r;
    left -= r;
    if (!IsSectorAligned(r, GetRequiredBufferAlignment())) {
      // Bytes reads don't fill sectors. Should only happen at the end
      // of the file.
      break;
    }
  }
  if (r < 0) {
    // An error: return a non-ok status
    s = IOError("While pread " + std::to_string(n) + " bytes from offset " +
                    std::to_string(offset),
                filename_, errno);
  }
  *result = Slice(scratch, (r < 0) ? 0 : n - left);
  return s;
}

IOStatus PosixSequentialFile::Skip(uint64_t n) {
  if (fseek(file_, static_cast<long int>(n), SEEK_CUR)) {
    return IOError("While fseek to skip " + std::to_string(n) + " bytes",
                   filename_, errno);
  }
  return IOStatus::OK();
}

IOStatus PosixSequentialFile::InvalidateCache(size_t offset, size_t length) {
#ifndef OS_LINUX
  (void)offset;
  (void)length;
  return IOStatus::OK();
#else
  if (!use_direct_io()) {
    // free OS pages
    int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
    if (ret != 0) {
      return IOError("While fadvise NotNeeded offset " +
                         std::to_string(offset) + " len " +
                         std::to_string(length),
                     filename_, errno);
    }
  }
  return IOStatus::OK();
#endif
}

/*
 * PosixRandomAccessFile
 */
#if defined(OS_LINUX)
size_t PosixHelper::GetUniqueIdFromFile(int fd, char* id, size_t max_size) {
  if (max_size < kMaxVarint64Length * 3) {
    return 0;
  }

  struct stat buf;
  int result = fstat(fd, &buf);
  if (result == -1) {
    return 0;
  }

  long version = 0;
  result = ioctl(fd, FS_IOC_GETVERSION, &version);
  TEST_SYNC_POINT_CALLBACK("GetUniqueIdFromFile:FS_IOC_GETVERSION", &result);
  if (result == -1) {
    return 0;
  }
  uint64_t uversion = (uint64_t)version;

  char* rid = id;
  rid = EncodeVarint64(rid, buf.st_dev);
  rid = EncodeVarint64(rid, buf.st_ino);
  rid = EncodeVarint64(rid, uversion);
  assert(rid >= id);
  return static_cast<size_t>(rid - id);
}
#endif

#if defined(OS_MACOSX) || defined(OS_AIX)
size_t PosixHelper::GetUniqueIdFromFile(int fd, char* id, size_t max_size) {
  if (max_size < kMaxVarint64Length * 3) {
    return 0;
  }

  struct stat buf;
  int result = fstat(fd, &buf);
  if (result == -1) {
    return 0;
  }

  char* rid = id;
  rid = EncodeVarint64(rid, buf.st_dev);
  rid = EncodeVarint64(rid, buf.st_ino);
  rid = EncodeVarint64(rid, buf.st_gen);
  assert(rid >= id);
  return static_cast<size_t>(rid - id);
}
#endif

#ifdef OS_LINUX
std::string RemoveTrailingSlash(const std::string& path) {
  std::string p = path;
  if (p.size() > 1 && p.back() == '/') {
    p.pop_back();
  }
  return p;
}

Status LogicalBlockSizeCache::RefAndCacheLogicalBlockSize(
    const std::vector<std::string>& directories) {
  std::vector<std::string> dirs;
  dirs.reserve(directories.size());
  for (auto& d : directories) {
    dirs.emplace_back(RemoveTrailingSlash(d));
  }

  std::map<std::string, size_t> dir_sizes;
  {
    ReadLock lock(&cache_mutex_);
    for (const auto& dir : dirs) {
      if (cache_.find(dir) == cache_.end()) {
        dir_sizes.emplace(dir, 0);
      }
    }
  }

  Status s;
  for (auto& dir_size : dir_sizes) {
    s = get_logical_block_size_of_directory_(dir_size.first, &dir_size.second);
    if (!s.ok()) {
      return s;
    }
  }

  WriteLock lock(&cache_mutex_);
  for (const auto& dir : dirs) {
    auto& v = cache_[dir];
    v.ref++;
    auto dir_size = dir_sizes.find(dir);
    if (dir_size != dir_sizes.end()) {
      v.size = dir_size->second;
    }
  }
  return s;
}

void LogicalBlockSizeCache::UnrefAndTryRemoveCachedLogicalBlockSize(
    const std::vector<std::string>& directories) {
  std::vector<std::string> dirs;
  dirs.reserve(directories.size());
  for (auto& dir : directories) {
    dirs.emplace_back(RemoveTrailingSlash(dir));
  }

  WriteLock lock(&cache_mutex_);
  for (const auto& dir : dirs) {
    auto it = cache_.find(dir);
    if (it != cache_.end() && !(--(it->second.ref))) {
      cache_.erase(it);
    }
  }
}

size_t LogicalBlockSizeCache::GetLogicalBlockSize(const std::string& fname,
                                                  int fd) {
  std::string dir = fname.substr(0, fname.find_last_of("/"));
  if (dir.empty()) {
    dir = "/";
  }
  {
    ReadLock lock(&cache_mutex_);
    auto it = cache_.find(dir);
    if (it != cache_.end()) {
      return it->second.size;
    }
  }
  return get_logical_block_size_of_fd_(fd);
}
#endif

Status PosixHelper::GetLogicalBlockSizeOfDirectory(const std::string& directory,
                                                   size_t* size) {
  int fd = open(directory.c_str(), O_DIRECTORY | O_RDONLY);
  if (fd == -1) {
    close(fd);
    return Status::IOError("Cannot open directory " + directory);
  }
  *size = PosixHelper::GetLogicalBlockSizeOfFd(fd);
  close(fd);
  return Status::OK();
}

size_t PosixHelper::GetLogicalBlockSizeOfFd(int fd) {
#ifdef OS_LINUX
  struct stat buf;
  int result = fstat(fd, &buf);
  if (result == -1) {
    return kDefaultPageSize;
  }
  if (major(buf.st_dev) == 0) {
    // Unnamed devices (e.g. non-device mounts), reserved as null device number.
    // These don't have an entry in /sys/dev/block/. Return a sensible default.
    return kDefaultPageSize;
  }

  // Reading queue/logical_block_size does not require special permissions.
  const int kBufferSize = 100;
  char path[kBufferSize];
  char real_path[PATH_MAX + 1];
  snprintf(path, kBufferSize, "/sys/dev/block/%u:%u", major(buf.st_dev),
           minor(buf.st_dev));
  if (realpath(path, real_path) == nullptr) {
    return kDefaultPageSize;
  }
  std::string device_dir(real_path);
  if (!device_dir.empty() && device_dir.back() == '/') {
    device_dir.pop_back();
  }
  // NOTE: sda3 and nvme0n1p1 do not have a `queue/` subdir, only the parent sda
  // and nvme0n1 have it.
  // $ ls -al '/sys/dev/block/8:3'
  // lrwxrwxrwx. 1 root root 0 Jun 26 01:38 /sys/dev/block/8:3 ->
  // ../../block/sda/sda3
  // $ ls -al '/sys/dev/block/259:4'
  // lrwxrwxrwx 1 root root 0 Jan 31 16:04 /sys/dev/block/259:4 ->
  // ../../devices/pci0000:17/0000:17:00.0/0000:18:00.0/nvme/nvme0/nvme0n1/nvme0n1p1
  size_t parent_end = device_dir.rfind('/', device_dir.length() - 1);
  if (parent_end == std::string::npos) {
    return kDefaultPageSize;
  }
  size_t parent_begin = device_dir.rfind('/', parent_end - 1);
  if (parent_begin == std::string::npos) {
    return kDefaultPageSize;
  }
  std::string parent =
      device_dir.substr(parent_begin + 1, parent_end - parent_begin - 1);
  std::string child = device_dir.substr(parent_end + 1, std::string::npos);
  if (parent != "block" &&
      (child.compare(0, 4, "nvme") || child.find('p') != std::string::npos)) {
    device_dir = device_dir.substr(0, parent_end);
  }
  std::string fname = device_dir + "/queue/logical_block_size";
  FILE* fp;
  size_t size = 0;
  fp = fopen(fname.c_str(), "r");
  if (fp != nullptr) {
    char* line = nullptr;
    size_t len = 0;
    if (getline(&line, &len, fp) != -1) {
      sscanf(line, "%zu", &size);
    }
    free(line);
    fclose(fp);
  }
  if (size != 0 && (size & (size - 1)) == 0) {
    return size;
  }
#endif
  (void)fd;
  return kDefaultPageSize;
}

/*
 * PosixRandomAccessFile
 *
 * pread() based random-access
 */
PosixRandomAccessFile::PosixRandomAccessFile(
    const std::string& fname, int fd, size_t logical_block_size,
    const EnvOptions& options
#if defined(ROCKSDB_IOURING_PRESENT)
    ,
    ThreadLocalPtr* thread_local_io_urings
#endif
    )
    : filename_(fname),
      fd_(fd),
      use_direct_io_(options.use_direct_reads),
      logical_sector_size_(logical_block_size)
#if defined(ROCKSDB_IOURING_PRESENT)
      ,
      thread_local_io_urings_(thread_local_io_urings)
#endif
{
  assert(!options.use_direct_reads || !options.use_mmap_reads);
  assert(!options.use_mmap_reads);
}

PosixRandomAccessFile::~PosixRandomAccessFile() { close(fd_); }

IOStatus PosixRandomAccessFile::Read(uint64_t offset, size_t n,
                                     const IOOptions& /*opts*/, Slice* result,
                                     char* scratch,
                                     IODebugContext* /*dbg*/) const {
  if (use_direct_io()) {
    assert(IsSectorAligned(offset, GetRequiredBufferAlignment()));
    assert(IsSectorAligned(n, GetRequiredBufferAlignment()));
    assert(IsSectorAligned(scratch, GetRequiredBufferAlignment()));
  }
  IOStatus s;
  ssize_t r = -1;
  size_t left = n;
  char* ptr = scratch;
  while (left > 0) {
    r = pread(fd_, ptr, left, static_cast<off_t>(offset));
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ptr += r;
    offset += r;
    left -= r;
    if (use_direct_io() &&
        r % static_cast<ssize_t>(GetRequiredBufferAlignment()) != 0) {
      // Bytes reads don't fill sectors. Should only happen at the end
      // of the file.
      break;
    }
  }
  if (r < 0) {
    // An error: return a non-ok status
    s = IOError("While pread offset " + std::to_string(offset) + " len " +
                    std::to_string(n),
                filename_, errno);
  }
  *result = Slice(scratch, (r < 0) ? 0 : n - left);
  return s;
}

IOStatus PosixRandomAccessFile::MultiRead(FSReadRequest* reqs, size_t num_reqs,
                                          const IOOptions& options,
                                          IODebugContext* dbg) {
  if (use_direct_io()) {
    for (size_t i = 0; i < num_reqs; i++) {
      assert(IsSectorAligned(reqs[i].offset, GetRequiredBufferAlignment()));
      assert(IsSectorAligned(reqs[i].len, GetRequiredBufferAlignment()));
      assert(IsSectorAligned(reqs[i].scratch, GetRequiredBufferAlignment()));
    }
  }

#if defined(ROCKSDB_IOURING_PRESENT)
  struct io_uring* iu = nullptr;
  if (thread_local_io_urings_) {
    iu = static_cast<struct io_uring*>(thread_local_io_urings_->Get());
    if (iu == nullptr) {
      iu = CreateIOUring();
      if (iu != nullptr) {
        thread_local_io_urings_->Reset(iu);
      }
    }
  }

  // Init failed, platform doesn't support io_uring. Fall back to
  // serialized reads
  if (iu == nullptr) {
    return FSRandomAccessFile::MultiRead(reqs, num_reqs, options, dbg);
  }

  IOStatus ios = IOStatus::OK();

  struct WrappedReadRequest {
    FSReadRequest* req;
    struct iovec iov;
    size_t finished_len;
    explicit WrappedReadRequest(FSReadRequest* r) : req(r), finished_len(0) {}
  };

  autovector<WrappedReadRequest, 32> req_wraps;
  autovector<WrappedReadRequest*, 4> incomplete_rq_list;
  std::unordered_set<WrappedReadRequest*> wrap_cache;

  for (size_t i = 0; i < num_reqs; i++) {
    req_wraps.emplace_back(&reqs[i]);
  }

  size_t reqs_off = 0;
  while (num_reqs > reqs_off || !incomplete_rq_list.empty()) {
    size_t this_reqs = (num_reqs - reqs_off) + incomplete_rq_list.size();

    // If requests exceed depth, split it into batches
    if (this_reqs > kIoUringDepth) this_reqs = kIoUringDepth;

    assert(incomplete_rq_list.size() <= this_reqs);
    for (size_t i = 0; i < this_reqs; i++) {
      WrappedReadRequest* rep_to_submit;
      if (i < incomplete_rq_list.size()) {
        rep_to_submit = incomplete_rq_list[i];
      } else {
        rep_to_submit = &req_wraps[reqs_off++];
      }
      assert(rep_to_submit->req->len > rep_to_submit->finished_len);
      rep_to_submit->iov.iov_base =
          rep_to_submit->req->scratch + rep_to_submit->finished_len;
      rep_to_submit->iov.iov_len =
          rep_to_submit->req->len - rep_to_submit->finished_len;

      struct io_uring_sqe* sqe;
      sqe = io_uring_get_sqe(iu);
      io_uring_prep_readv(
          sqe, fd_, &rep_to_submit->iov, 1,
          rep_to_submit->req->offset + rep_to_submit->finished_len);
      io_uring_sqe_set_data(sqe, rep_to_submit);
      wrap_cache.emplace(rep_to_submit);
    }
    incomplete_rq_list.clear();

    ssize_t ret =
        io_uring_submit_and_wait(iu, static_cast<unsigned int>(this_reqs));
    TEST_SYNC_POINT_CALLBACK(
        "PosixRandomAccessFile::MultiRead:io_uring_submit_and_wait:return1",
        &ret);
    TEST_SYNC_POINT_CALLBACK(
        "PosixRandomAccessFile::MultiRead:io_uring_submit_and_wait:return2",
        iu);

    if (static_cast<size_t>(ret) != this_reqs) {
      fprintf(stderr, "ret = %ld this_reqs: %ld\n", (long)ret, (long)this_reqs);
      // If error happens and we submitted fewer than expected, it is an
      // exception case and we don't retry here. We should still consume
      // what is is submitted in the ring.
      for (ssize_t i = 0; i < ret; i++) {
        struct io_uring_cqe* cqe = nullptr;
        io_uring_wait_cqe(iu, &cqe);
        if (cqe != nullptr) {
          io_uring_cqe_seen(iu, cqe);
        }
      }
      return IOStatus::IOError("io_uring_submit_and_wait() requested " +
                               std::to_string(this_reqs) + " but returned " +
                               std::to_string(ret));
    }

    for (size_t i = 0; i < this_reqs; i++) {
      struct io_uring_cqe* cqe = nullptr;
      WrappedReadRequest* req_wrap;

      // We could use the peek variant here, but this seems safer in terms
      // of our initial wait not reaping all completions
      ret = io_uring_wait_cqe(iu, &cqe);
      TEST_SYNC_POINT_CALLBACK(
          "PosixRandomAccessFile::MultiRead:io_uring_wait_cqe:return", &ret);
      if (ret) {
        ios = IOStatus::IOError("io_uring_wait_cqe() returns " +
                                std::to_string(ret));

        if (cqe != nullptr) {
          io_uring_cqe_seen(iu, cqe);
        }
        continue;
      }

      req_wrap = static_cast<WrappedReadRequest*>(io_uring_cqe_get_data(cqe));
      // Reset cqe data to catch any stray reuse of it
      static_cast<struct io_uring_cqe*>(cqe)->user_data = 0xd5d5d5d5d5d5d5d5;
      // Check that we got a valid unique cqe data
      auto wrap_check = wrap_cache.find(req_wrap);
      if (wrap_check == wrap_cache.end()) {
        fprintf(stderr,
                "PosixRandomAccessFile::MultiRead: "
                "Bad cqe data from IO uring - %p\n",
                req_wrap);
        port::PrintStack();
        ios = IOStatus::IOError("io_uring_cqe_get_data() returned " +
                                std::to_string((uint64_t)req_wrap));
        continue;
      }
      wrap_cache.erase(wrap_check);

      FSReadRequest* req = req_wrap->req;
      size_t bytes_read = 0;
      bool read_again = false;
      UpdateResult(cqe, filename_, req->len, req_wrap->iov.iov_len,
                   false /*async_read*/, use_direct_io(),
                   GetRequiredBufferAlignment(), req_wrap->finished_len, req,
                   bytes_read, read_again);
      int32_t res = cqe->res;
      if (res >= 0) {
        if (bytes_read == 0) {
          if (read_again) {
            Slice tmp_slice;
            req->status =
                Read(req->offset + req_wrap->finished_len,
                     req->len - req_wrap->finished_len, options, &tmp_slice,
                     req->scratch + req_wrap->finished_len, dbg);
            req->result =
                Slice(req->scratch, req_wrap->finished_len + tmp_slice.size());
          }
          // else It means EOF so no need to do anything.
        } else if (bytes_read < req_wrap->iov.iov_len) {
          incomplete_rq_list.push_back(req_wrap);
        }
      }
      io_uring_cqe_seen(iu, cqe);
    }
    wrap_cache.clear();
  }
  return ios;
#else
  return FSRandomAccessFile::MultiRead(reqs, num_reqs, options, dbg);
#endif
}

IOStatus PosixRandomAccessFile::Prefetch(uint64_t offset, size_t n,
                                         const IOOptions& /*opts*/,
                                         IODebugContext* /*dbg*/) {
  IOStatus s;
  if (!use_direct_io()) {
    ssize_t r = 0;
#ifdef OS_LINUX
    r = readahead(fd_, offset, n);
#endif
#ifdef OS_MACOSX
    radvisory advice;
    advice.ra_offset = static_cast<off_t>(offset);
    advice.ra_count = static_cast<int>(n);
    r = fcntl(fd_, F_RDADVISE, &advice);
#endif
    if (r == -1) {
      s = IOError("While prefetching offset " + std::to_string(offset) +
                      " len " + std::to_string(n),
                  filename_, errno);
    }
  }
  return s;
}

#if defined(OS_LINUX) || defined(OS_MACOSX) || defined(OS_AIX)
size_t PosixRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  return PosixHelper::GetUniqueIdFromFile(fd_, id, max_size);
}
#endif

void PosixRandomAccessFile::Hint(AccessPattern pattern) {
  if (use_direct_io()) {
    return;
  }
  switch (pattern) {
    case kNormal:
      Fadvise(fd_, 0, 0, POSIX_FADV_NORMAL);
      break;
    case kRandom:
      Fadvise(fd_, 0, 0, POSIX_FADV_RANDOM);
      break;
    case kSequential:
      Fadvise(fd_, 0, 0, POSIX_FADV_SEQUENTIAL);
      break;
    case kWillNeed:
      Fadvise(fd_, 0, 0, POSIX_FADV_WILLNEED);
      break;
    case kWontNeed:
      Fadvise(fd_, 0, 0, POSIX_FADV_DONTNEED);
      break;
    default:
      assert(false);
      break;
  }
}

IOStatus PosixRandomAccessFile::InvalidateCache(size_t offset, size_t length) {
  if (use_direct_io()) {
    return IOStatus::OK();
  }
#ifndef OS_LINUX
  (void)offset;
  (void)length;
  return IOStatus::OK();
#else
  // free OS pages
  int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
  if (ret == 0) {
    return IOStatus::OK();
  }
  return IOError("While fadvise NotNeeded offset " + std::to_string(offset) +
                     " len " + std::to_string(length),
                 filename_, errno);
#endif
}

IOStatus PosixRandomAccessFile::ReadAsync(
    FSReadRequest& req, const IOOptions& /*opts*/,
    std::function<void(const FSReadRequest&, void*)> cb, void* cb_arg,
    void** io_handle, IOHandleDeleter* del_fn, IODebugContext* /*dbg*/) {
  if (use_direct_io()) {
    assert(IsSectorAligned(req.offset, GetRequiredBufferAlignment()));
    assert(IsSectorAligned(req.len, GetRequiredBufferAlignment()));
    assert(IsSectorAligned(req.scratch, GetRequiredBufferAlignment()));
  }

#if defined(ROCKSDB_IOURING_PRESENT)
  // io_uring_queue_init.
  struct io_uring* iu = nullptr;
  if (thread_local_io_urings_) {
    iu = static_cast<struct io_uring*>(thread_local_io_urings_->Get());
    if (iu == nullptr) {
      iu = CreateIOUring();
      if (iu != nullptr) {
        thread_local_io_urings_->Reset(iu);
      }
    }
  }

  // Init failed, platform doesn't support io_uring.
  if (iu == nullptr) {
    return IOStatus::NotSupported("ReadAsync");
  }

  // Allocate io_handle.
  IOHandleDeleter deletefn = [](void* args) -> void {
    delete (static_cast<Posix_IOHandle*>(args));
    args = nullptr;
  };

  // Initialize Posix_IOHandle.
  Posix_IOHandle* posix_handle =
      new Posix_IOHandle(iu, cb, cb_arg, req.offset, req.len, req.scratch,
                         use_direct_io(), GetRequiredBufferAlignment());
  posix_handle->iov.iov_base = req.scratch;
  posix_handle->iov.iov_len = req.len;

  *io_handle = static_cast<void*>(posix_handle);
  *del_fn = deletefn;

  // Step 3: io_uring_sqe_set_data
  struct io_uring_sqe* sqe;
  sqe = io_uring_get_sqe(iu);

  io_uring_prep_readv(sqe, fd_, /*sqe->addr=*/&posix_handle->iov,
                      /*sqe->len=*/1, /*sqe->offset=*/posix_handle->offset);

  // Sets sqe->user_data to posix_handle.
  io_uring_sqe_set_data(sqe, posix_handle);

  // Step 4: io_uring_submit
  ssize_t ret = io_uring_submit(iu);
  if (ret < 0) {
    fprintf(stderr, "io_uring_submit error: %ld\n", long(ret));
    return IOStatus::IOError("io_uring_submit() requested but returned " +
                             std::to_string(ret));
  }
  return IOStatus::OK();
#else
  (void)req;
  (void)cb;
  (void)cb_arg;
  (void)io_handle;
  (void)del_fn;
  return IOStatus::NotSupported("ReadAsync");
#endif
}

/*
 * PosixMmapReadableFile
 *
 * mmap() based random-access
 */
// base[0,length-1] contains the mmapped contents of the file.
PosixMmapReadableFile::PosixMmapReadableFile(const int fd,
                                             const std::string& fname,
                                             void* base, size_t length,
                                             const EnvOptions& options)
    : fd_(fd), filename_(fname), mmapped_region_(base), length_(length) {
#ifdef NDEBUG
  (void)options;
#endif
  fd_ = fd_ + 0;  // suppress the warning for used variables
  assert(options.use_mmap_reads);
  assert(!options.use_direct_reads);
}

PosixMmapReadableFile::~PosixMmapReadableFile() {
  int ret = munmap(mmapped_region_, length_);
  if (ret != 0) {
    fprintf(stdout, "failed to munmap %p length %" ROCKSDB_PRIszt " \n",
            mmapped_region_, length_);
  }
  close(fd_);
}

IOStatus PosixMmapReadableFile::Read(uint64_t offset, size_t n,
                                     const IOOptions& /*opts*/, Slice* result,
                                     char* /*scratch*/,
                                     IODebugContext* /*dbg*/) const {
  IOStatus s;
  if (offset > length_) {
    *result = Slice();
    return IOError("While mmap read offset " + std::to_string(offset) +
                       " larger than file length " + std::to_string(length_),
                   filename_, EINVAL);
  } else if (offset + n > length_) {
    n = static_cast<size_t>(length_ - offset);
  }
  *result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n);
  return s;
}

void PosixMmapReadableFile::Hint(AccessPattern pattern) {
  switch (pattern) {
    case kNormal:
      Madvise(mmapped_region_, length_, POSIX_MADV_NORMAL);
      break;
    case kRandom:
      Madvise(mmapped_region_, length_, POSIX_MADV_RANDOM);
      break;
    case kSequential:
      Madvise(mmapped_region_, length_, POSIX_MADV_SEQUENTIAL);
      break;
    case kWillNeed:
      Madvise(mmapped_region_, length_, POSIX_MADV_WILLNEED);
      break;
    case kWontNeed:
      Madvise(mmapped_region_, length_, POSIX_MADV_DONTNEED);
      break;
    default:
      assert(false);
      break;
  }
}

IOStatus PosixMmapReadableFile::InvalidateCache(size_t offset, size_t length) {
#ifndef OS_LINUX
  (void)offset;
  (void)length;
  return IOStatus::OK();
#else
  // free OS pages
  int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
  if (ret == 0) {
    return IOStatus::OK();
  }
  return IOError("While fadvise not needed. Offset " + std::to_string(offset) +
                     " len" + std::to_string(length),
                 filename_, errno);
#endif
}

/*
 * PosixMmapFile
 *
 * We preallocate up to an extra megabyte and use memcpy to append new
 * data to the file.  This is safe since we either properly close the
 * file before reading from it, or for log files, the reading code
 * knows enough to skip zero suffixes.
 */
IOStatus PosixMmapFile::UnmapCurrentRegion() {
  TEST_KILL_RANDOM("PosixMmapFile::UnmapCurrentRegion:0");
  if (base_ != nullptr) {
    int munmap_status = munmap(base_, limit_ - base_);
    if (munmap_status != 0) {
      return IOError("While munmap", filename_, munmap_status);
    }
    file_offset_ += limit_ - base_;
    base_ = nullptr;
    limit_ = nullptr;
    last_sync_ = nullptr;
    dst_ = nullptr;

    // Increase the amount we map the next time, but capped at 1MB
    if (map_size_ < (1 << 20)) {
      map_size_ *= 2;
    }
  }
  return IOStatus::OK();
}

IOStatus PosixMmapFile::MapNewRegion() {
#ifdef ROCKSDB_FALLOCATE_PRESENT
  assert(base_ == nullptr);
  TEST_KILL_RANDOM("PosixMmapFile::UnmapCurrentRegion:0");
  // we can't fallocate with FALLOC_FL_KEEP_SIZE here
  if (allow_fallocate_) {
    IOSTATS_TIMER_GUARD(allocate_nanos);
    int alloc_status = fallocate(fd_, 0, file_offset_, map_size_);
    if (alloc_status != 0) {
      // fallback to posix_fallocate
      alloc_status = posix_fallocate(fd_, file_offset_, map_size_);
    }
    if (alloc_status != 0) {
      return IOStatus::IOError("Error allocating space to file : " + filename_ +
                               "Error : " + errnoStr(alloc_status).c_str());
    }
  }

  TEST_KILL_RANDOM("PosixMmapFile::Append:1");
  void* ptr = mmap(nullptr, map_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_,
                   file_offset_);
  if (ptr == MAP_FAILED) {
    return IOStatus::IOError("MMap failed on " + filename_);
  }
  TEST_KILL_RANDOM("PosixMmapFile::Append:2");

  base_ = reinterpret_cast<char*>(ptr);
  limit_ = base_ + map_size_;
  dst_ = base_;
  last_sync_ = base_;
  return IOStatus::OK();
#else
  return IOStatus::NotSupported("This platform doesn't support fallocate()");
#endif
}

IOStatus PosixMmapFile::Msync() {
  if (dst_ == last_sync_) {
    return IOStatus::OK();
  }
  // Find the beginnings of the pages that contain the first and last
  // bytes to be synced.
  size_t p1 = TruncateToPageBoundary(last_sync_ - base_);
  size_t p2 = TruncateToPageBoundary(dst_ - base_ - 1);
  last_sync_ = dst_;
  TEST_KILL_RANDOM("PosixMmapFile::Msync:0");
  if (msync(base_ + p1, p2 - p1 + page_size_, MS_SYNC) < 0) {
    return IOError("While msync", filename_, errno);
  }
  return IOStatus::OK();
}

PosixMmapFile::PosixMmapFile(const std::string& fname, int fd, size_t page_size,
                             const EnvOptions& options)
    : filename_(fname),
      fd_(fd),
      page_size_(page_size),
      map_size_(Roundup(65536, page_size)),
      base_(nullptr),
      limit_(nullptr),
      dst_(nullptr),
      last_sync_(nullptr),
      file_offset_(0) {
#ifdef ROCKSDB_FALLOCATE_PRESENT
  allow_fallocate_ = options.allow_fallocate;
  fallocate_with_keep_size_ = options.fallocate_with_keep_size;
#else
  (void)options;
#endif
  assert((page_size & (page_size - 1)) == 0);
  assert(options.use_mmap_writes);
  assert(!options.use_direct_writes);
}

PosixMmapFile::~PosixMmapFile() {
  if (fd_ >= 0) {
    IOStatus s = PosixMmapFile::Close(IOOptions(), nullptr);
    s.PermitUncheckedError();
  }
}

IOStatus PosixMmapFile::Append(const Slice& data, const IOOptions& /*opts*/,
                               IODebugContext* /*dbg*/) {
  const char* src = data.data();
  size_t left = data.size();
  while (left > 0) {
    assert(base_ <= dst_);
    assert(dst_ <= limit_);
    size_t avail = limit_ - dst_;
    if (avail == 0) {
      IOStatus s = UnmapCurrentRegion();
      if (!s.ok()) {
        return s;
      }
      s = MapNewRegion();
      if (!s.ok()) {
        return s;
      }
      TEST_KILL_RANDOM("PosixMmapFile::Append:0");
    }

    size_t n = (left <= avail) ? left : avail;
    assert(dst_);
    memcpy(dst_, src, n);
    dst_ += n;
    src += n;
    left -= n;
  }
  return IOStatus::OK();
}

IOStatus PosixMmapFile::AClose(const IOOptions& /*opts*/,
                              IODebugContext* /*dbg*/) {
  IOStatus s;
  size_t unused = limit_ - dst_;

  s = UnmapCurrentRegion();
  if (!s.ok()) {
    s = IOError("While closing mmapped file", filename_, errno);
  } else if (unused > 0) {
    // Trim the extra space at the end of the file
    if (ftruncate(fd_, file_offset_ - unused) < 0) {
      s = IOError("While ftruncating mmaped file", filename_, errno);
    }
  }
  fd_ = -1;
  base_ = nullptr;
  limit_ = nullptr;
  return s;
}


IOStatus PosixMmapFile::Close(const IOOptions& /*opts*/,
                              IODebugContext* /*dbg*/) {
  IOStatus s;
  size_t unused = limit_ - dst_;

  s = UnmapCurrentRegion();
  if (!s.ok()) {
    s = IOError("While closing mmapped file", filename_, errno);
  } else if (unused > 0) {
    // Trim the extra space at the end of the file
    if (ftruncate(fd_, file_offset_ - unused) < 0) {
      s = IOError("While ftruncating mmaped file", filename_, errno);
    }
  }

  if (close(fd_) < 0) {
    if (s.ok()) {
      s = IOError("While closing mmapped file", filename_, errno);
    }
  }

  fd_ = -1;
  base_ = nullptr;
  limit_ = nullptr;
  return s;
}

IOStatus PosixMmapFile::Flush(const IOOptions& /*opts*/,
                              IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus PosixMmapFile::Sync(const IOOptions& /*opts*/,
                             IODebugContext* /*dbg*/) {
#ifdef HAVE_FULLFSYNC
  if (::fcntl(fd_, F_FULLFSYNC) < 0) {
    return IOError("while fcntl(F_FULLSYNC) mmapped file", filename_, errno);
  }
#else   // HAVE_FULLFSYNC
  if (fdatasync(fd_) < 0) {
    return IOError("While fdatasync mmapped file", filename_, errno);
  }
#endif  // HAVE_FULLFSYNC

  return Msync();
}

/**
 * Flush data as well as metadata to stable storage.
 */
IOStatus PosixMmapFile::Fsync(const IOOptions& /*opts*/,
                              IODebugContext* /*dbg*/) {
#ifdef HAVE_FULLFSYNC
  if (::fcntl(fd_, F_FULLFSYNC) < 0) {
    return IOError("While fcntl(F_FULLSYNC) on mmaped file", filename_, errno);
  }
#else   // HAVE_FULLFSYNC
  if (fsync(fd_) < 0) {
    return IOError("While fsync mmaped file", filename_, errno);
  }
#endif  // HAVE_FULLFSYNC

  return Msync();
}

// Lei Todo: this is for mmapFile
IOStatus PosixMmapFile::ASync(const IOOptions& opts, IODebugContext* dbg, struct uring_queue* uptr){
  return Sync(opts, dbg);
}

IOStatus PosixMmapFile::AFsync(const IOOptions& opts, IODebugContext* dbg, struct uring_queue* uptr){
  return Fsync(opts, dbg);
}


/**
 * Get the size of valid data in the file. This will not match the
 * size that is returned from the filesystem because we use mmap
 * to extend file by map_size every time.
 */
uint64_t PosixMmapFile::GetFileSize(const IOOptions& /*opts*/,
                                    IODebugContext* /*dbg*/) {
  size_t used = dst_ - base_;
  return file_offset_ + used;
}

IOStatus PosixMmapFile::InvalidateCache(size_t offset, size_t length) {
#ifndef OS_LINUX
  (void)offset;
  (void)length;
  return IOStatus::OK();
#else
  // free OS pages
  int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
  if (ret == 0) {
    return IOStatus::OK();
  }
  return IOError("While fadvise NotNeeded mmapped file", filename_, errno);
#endif
}

#ifdef ROCKSDB_FALLOCATE_PRESENT
IOStatus PosixMmapFile::Allocate(uint64_t offset, uint64_t len,
                                 const IOOptions& /*opts*/,
                                 IODebugContext* /*dbg*/) {
  assert(offset <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  assert(len <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  TEST_KILL_RANDOM("PosixMmapFile::Allocate:0");
  int alloc_status = 0;
  if (allow_fallocate_) {
    alloc_status =
        fallocate(fd_, fallocate_with_keep_size_ ? FALLOC_FL_KEEP_SIZE : 0,
                  static_cast<off_t>(offset), static_cast<off_t>(len));
  }
  if (alloc_status == 0) {
    return IOStatus::OK();
  } else {
    return IOError("While fallocate offset " + std::to_string(offset) +
                       " len " + std::to_string(len),
                   filename_, errno);
  }
}
#endif

/*
 * PosixWritableFile
 *
 * Use posix write to write data to a file.
 */
PosixWritableFile::PosixWritableFile(const std::string& fname, int fd,
                                     size_t logical_block_size,
                                     const EnvOptions& options)
    : FSWritableFile(options),
      filename_(fname),
      use_direct_io_(options.use_direct_writes),
      fd_(fd),
      filesize_(0),
      logical_sector_size_(logical_block_size) {
#ifdef ROCKSDB_FALLOCATE_PRESENT
  allow_fallocate_ = options.allow_fallocate;
  fallocate_with_keep_size_ = options.fallocate_with_keep_size;
#endif
#ifdef ROCKSDB_RANGESYNC_PRESENT
  sync_file_range_supported_ = IsSyncFileRangeSupported(fd_);
#endif  // ROCKSDB_RANGESYNC_PRESENT
  assert(!options.use_mmap_writes);
}

PosixWritableFile::~PosixWritableFile() {
  if (fd_ >= 0) {
    IOStatus s = PosixWritableFile::Close(IOOptions(), nullptr);
    s.PermitUncheckedError();
  }
}

IOStatus PosixWritableFile::Append(const Slice& data, const IOOptions& /*opts*/,
                                   IODebugContext* /*dbg*/) {
  if (use_direct_io()) {
    assert(IsSectorAligned(data.size(), GetRequiredBufferAlignment()));
    assert(IsSectorAligned(data.data(), GetRequiredBufferAlignment()));
  }
  const char* src = data.data();
  size_t nbytes = data.size();
  if (!PosixWrite(fd_, src, nbytes)) {
    return IOError("While appending to file", filename_, errno);
  }

  filesize_ += nbytes;
  return IOStatus::OK();
}

IOStatus PosixWritableFile::PositionedAppend(const Slice& data, uint64_t offset,
                                             const IOOptions& /*opts*/,
                                             IODebugContext* /*dbg*/) {
  if (use_direct_io()) {
    assert(IsSectorAligned(offset, GetRequiredBufferAlignment()));
    assert(IsSectorAligned(data.size(), GetRequiredBufferAlignment()));
    assert(IsSectorAligned(data.data(), GetRequiredBufferAlignment()));
  }
  assert(offset <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  const char* src = data.data();
  size_t nbytes = data.size();
  if (!PosixPositionedWrite(fd_, src, nbytes, static_cast<off_t>(offset))) {
    return IOError("While pwrite to file at offset " + std::to_string(offset),
                   filename_, errno);
  }
  filesize_ = offset + nbytes;
  return IOStatus::OK();
}

IOStatus PosixWritableFile::Truncate(uint64_t size, const IOOptions& /*opts*/,
                                     IODebugContext* /*dbg*/) {
  IOStatus s;
  int r = ftruncate(fd_, size);
  if (r < 0) {
    s = IOError("While ftruncate file to size " + std::to_string(size),
                filename_, errno);
  } else {
    filesize_ = size;
  }
  return s;
}

IOStatus PosixWritableFile::AClose(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  IOStatus s;
  size_t block_size;
  size_t last_allocated_block;
  GetPreallocationStatus(&block_size, &last_allocated_block);
  TEST_SYNC_POINT_CALLBACK("PosixWritableFile::Close", &last_allocated_block);
  if (last_allocated_block > 0) {
    // trim the extra space preallocated at the end of the file
    // NOTE(ljin): we probably don't want to surface failure as an IOError,
    // but it will be nice to log these errors.
    int dummy __attribute__((__unused__));
    dummy = ftruncate(fd_, filesize_);
#if defined(ROCKSDB_FALLOCATE_PRESENT) && defined(FALLOC_FL_PUNCH_HOLE)
    // in some file systems, ftruncate only trims trailing space if the
    // new file size is smaller than the current size. Calling fallocate
    // with FALLOC_FL_PUNCH_HOLE flag to explicitly release these unused
    // blocks. FALLOC_FL_PUNCH_HOLE is supported on at least the following
    // filesystems:
    //   XFS (since Linux 2.6.38)
    //   ext4 (since Linux 3.0)
    //   Btrfs (since Linux 3.7)
    //   tmpfs (since Linux 3.5)
    // We ignore error since failure of this operation does not affect
    // correctness.
    struct stat file_stats;
    int result = fstat(fd_, &file_stats);
    // After ftruncate, we check whether ftruncate has the correct behavior.
    // If not, we should hack it with FALLOC_FL_PUNCH_HOLE
    if (result == 0 &&
        (file_stats.st_size + file_stats.st_blksize - 1) /
                file_stats.st_blksize !=
            file_stats.st_blocks / (file_stats.st_blksize / 512)) {
      IOSTATS_TIMER_GUARD(allocate_nanos);
      if (allow_fallocate_) {
        fallocate(fd_, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, filesize_,
                  block_size * last_allocated_block - filesize_);
      }
    }
#endif
  }
  fd_ = -1;
  return s;
}



IOStatus PosixWritableFile::Close(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {

  IOStatus s;
  //
  size_t block_size;
  size_t last_allocated_block;
  GetPreallocationStatus(&block_size, &last_allocated_block);
  TEST_SYNC_POINT_CALLBACK("PosixWritableFile::Close", &last_allocated_block);
  if (last_allocated_block > 0) {
    // trim the extra space preallocated at the end of the file
    // NOTE(ljin): we probably don't want to surface failure as an IOError,
    // but it will be nice to log these errors.
    int dummy __attribute__((__unused__));
    dummy = ftruncate(fd_, filesize_);
#if defined(ROCKSDB_FALLOCATE_PRESENT) && defined(FALLOC_FL_PUNCH_HOLE)
    // in some file systems, ftruncate only trims trailing space if the
    // new file size is smaller than the current size. Calling fallocate
    // with FALLOC_FL_PUNCH_HOLE flag to explicitly release these unused
    // blocks. FALLOC_FL_PUNCH_HOLE is supported on at least the following
    // filesystems:
    //   XFS (since Linux 2.6.38)
    //   ext4 (since Linux 3.0)
    //   Btrfs (since Linux 3.7)
    //   tmpfs (since Linux 3.5)
    // We ignore error since failure of this operation does not affect
    // correctness.
    struct stat file_stats;
    int result = fstat(fd_, &file_stats);
    // After ftruncate, we check whether ftruncate has the correct behavior.
    // If not, we should hack it with FALLOC_FL_PUNCH_HOLE
    if (result == 0 &&
        (file_stats.st_size + file_stats.st_blksize - 1) /
                file_stats.st_blksize !=
            file_stats.st_blocks / (file_stats.st_blksize / 512)) {
      IOSTATS_TIMER_GUARD(allocate_nanos);
      if (allow_fallocate_) {
        fallocate(fd_, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, filesize_,
                  block_size * last_allocated_block - filesize_);
      }
    }
#endif
  }

  if (close(fd_) < 0) {
    s = IOError("While closing file after writing", filename_, errno);
  }
  fd_ = -1;
  return s;
}

// write out the cached data to the OS cache
IOStatus PosixWritableFile::Flush(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus PosixWritableFile::Sync(const IOOptions& /*opts*/,
                                 IODebugContext* /*dbg*/) {
#ifdef HAVE_FULLFSYNC
  if (::fcntl(fd_, F_FULLFSYNC) < 0) {
    return IOError("while fcntl(F_FULLFSYNC)", filename_, errno);
  }
#else   // HAVE_FULLFSYNC
  if (fdatasync(fd_) < 0) {
    return IOError("While fdatasync", filename_, errno);
  }
#endif  // HAVE_FULLFSYNC
  return IOStatus::OK();
}

IOStatus PosixWritableFile::Fsync(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
#ifdef HAVE_FULLFSYNC
  if (::fcntl(fd_, F_FULLFSYNC) < 0) {
    return IOError("while fcntl(F_FULLFSYNC)", filename_, errno);
  }
#else   // HAVE_FULLFSYNC
  if (fsync(fd_) < 0) {
    return IOError("While fsync", filename_, errno);
  }
#endif  // HAVE_FULLFSYNC
  return IOStatus::OK();
}

// Lei modified: this is for normal writable file
IOStatus PosixWritableFile::ASync(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/, struct uring_queue* uptr){
  
  
  // FIX(bug#5/#8): do not deref a null queue and do not return OK on failure;
  // callers check this IOStatus (compaction_outputs::WriterSyncClose, builder)
  // and will abort instead of installing an unsynced file.
  if(uptr == nullptr)
  {
    return IOStatus::IOError("Pome ASync: no io_uring queue available for " + filename_);
  }
  // FIX(bug#2): with max_subcompactions>1 several subcompaction threads share
  // this queue; liburing's submission queue is single-producer and the
  // fds/sync_count bookkeeping is not atomic. qmtx makes get_sqe+prep+submit+
  // accounting one atomic step. Uncontended by default (one thread per job).
  std::lock_guard<std::mutex> qlk(uptr->qmtx);
  struct io_uring *uq = &uptr->uring;
  struct io_uring_sqe* sqe = io_uring_get_sqe(uq);
  if(sqe == nullptr)
  {
    // FIX(bug#8): never prep on a null sqe (segfault). Report queue-full error.
    return IOStatus::IOError("Pome ASync: io_uring submission queue full for " + filename_);
  }

  // Lei Todo: this place should act like fdatasync, which means having flag IORING_FSYNC_DATASYNC.
  io_uring_prep_fsync(sqe, fd_, IORING_FSYNC_DATASYNC);
  // Set data can transmit datas
  //io_uring_sqe_set_data(sqe, (void*) uq);

  int ret = io_uring_submit(uq);
  if(ret <= 0)
  {
    // FIX(bug#5): submit failed -> nothing was enqueued; report error and do
    // NOT account it (otherwise wait_for_queue would block on a phantom CQE).
    return IOStatus::IOError("Pome ASync: io_uring_submit failed for " + filename_);
  }
  // Account the in-flight fsync only after a successful submit, so sync_count
  // and fds exactly match what wait_for_queue must reap/close (FIX(bug#8): was
  // incremented before the null/submit checks).
  uptr->fds.push_back(fd_);
  uptr->sync_count += 1;
  //printf("ASync: fd:%d, uq: %p\n", fd_, uptr);
  return IOStatus::OK();
}

// zl: AFsync: Each time add a new function should change many classes... 
IOStatus PosixWritableFile::AFsync(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/, struct uring_queue* uptr)
{
  // FIX(bug#5/#8): mirror ASync — no null deref, no OK-on-failure.
  if(uptr == nullptr)
  {
    return IOStatus::IOError("Pome AFsync: no io_uring queue available for " + filename_);
  }
  // FIX(bug#2): see ASync — serialize shared-queue submission and accounting.
  std::lock_guard<std::mutex> qlk(uptr->qmtx);
  struct io_uring *uq = &uptr->uring;
  struct io_uring_sqe* sqe = io_uring_get_sqe(uq);
  if(sqe == nullptr)
  {
    return IOStatus::IOError("Pome AFsync: io_uring submission queue full for " + filename_);
  }

  // Lei Todo: this place should act like fsync, which means no flag IORING_FSYNC_DATASYNC.
  io_uring_prep_fsync(sqe, fd_, IORING_FSYNC_DATASYNC);
  // Set data can transmit datas
  //io_uring_sqe_set_data(sqe, (void*) uq);

  int ret = io_uring_submit(uq);
  if(ret <= 0)
  {
    return IOStatus::IOError("Pome AFsync: io_uring_submit failed for " + filename_);
  }
  uptr->fds.push_back(fd_);
  uptr->sync_count += 1;
  //printf("AFSync: fd:%d, uq: %p\n", fd_, uptr);
  return IOStatus::OK();
}


bool PosixWritableFile::IsSyncThreadSafe() const { return true; }

uint64_t PosixWritableFile::GetFileSize(const IOOptions& /*opts*/,
                                        IODebugContext* /*dbg*/) {
  return filesize_;
}

void PosixWritableFile::SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) {
#ifdef OS_LINUX
// Suppress Valgrind "Unimplemented functionality" error.
#ifndef ROCKSDB_VALGRIND_RUN
  if (hint == write_hint_) {
    return;
  }
  if (fcntl(fd_, F_SET_RW_HINT, &hint) == 0) {
    write_hint_ = hint;
  }
#else
  (void)hint;
#endif  // ROCKSDB_VALGRIND_RUN
#else
  (void)hint;
#endif  // OS_LINUX
}

IOStatus PosixWritableFile::InvalidateCache(size_t offset, size_t length) {
  if (use_direct_io()) {
    return IOStatus::OK();
  }
#ifndef OS_LINUX
  (void)offset;
  (void)length;
  return IOStatus::OK();
#else
  // free OS pages
  int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
  if (ret == 0) {
    return IOStatus::OK();
  }
  return IOError("While fadvise NotNeeded", filename_, errno);
#endif
}

#ifdef ROCKSDB_FALLOCATE_PRESENT
IOStatus PosixWritableFile::Allocate(uint64_t offset, uint64_t len,
                                     const IOOptions& /*opts*/,
                                     IODebugContext* /*dbg*/) {
  assert(offset <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  assert(len <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  TEST_KILL_RANDOM("PosixWritableFile::Allocate:0");
  IOSTATS_TIMER_GUARD(allocate_nanos);
  int alloc_status = 0;
  if (allow_fallocate_) {
    alloc_status =
        fallocate(fd_, fallocate_with_keep_size_ ? FALLOC_FL_KEEP_SIZE : 0,
                  static_cast<off_t>(offset), static_cast<off_t>(len));
  }
  if (alloc_status == 0) {
    return IOStatus::OK();
  } else {
    return IOError("While fallocate offset " + std::to_string(offset) +
                       " len " + std::to_string(len),
                   filename_, errno);
  }
}
#endif

IOStatus PosixWritableFile::RangeSync(uint64_t offset, uint64_t nbytes,
                                      const IOOptions& opts,
                                      IODebugContext* dbg) {
#ifdef ROCKSDB_RANGESYNC_PRESENT
  assert(offset <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  assert(nbytes <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  if (sync_file_range_supported_) {
    int ret;
    if (strict_bytes_per_sync_) {
      // Specifying `SYNC_FILE_RANGE_WAIT_BEFORE` together with an offset/length
      // that spans all bytes written so far tells `sync_file_range` to wait for
      // any outstanding writeback requests to finish before issuing a new one.
      ret =
          sync_file_range(fd_, 0, static_cast<off_t>(offset + nbytes),
                          SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE);
    } else {
      ret = sync_file_range(fd_, static_cast<off_t>(offset),
                            static_cast<off_t>(nbytes), SYNC_FILE_RANGE_WRITE);
    }
    if (ret != 0) {
      return IOError("While sync_file_range returned " + std::to_string(ret),
                     filename_, errno);
    }
    return IOStatus::OK();
  }
#endif  // ROCKSDB_RANGESYNC_PRESENT
  return FSWritableFile::RangeSync(offset, nbytes, opts, dbg);
}

#ifdef OS_LINUX
size_t PosixWritableFile::GetUniqueId(char* id, size_t max_size) const {
  return PosixHelper::GetUniqueIdFromFile(fd_, id, max_size);
}
#endif

/*
 * PosixRandomRWFile
 */

PosixRandomRWFile::PosixRandomRWFile(const std::string& fname, int fd,
                                     const EnvOptions& /*options*/)
    : filename_(fname), fd_(fd) {}

PosixRandomRWFile::~PosixRandomRWFile() {
  if (fd_ >= 0) {
    IOStatus s = Close(IOOptions(), nullptr);
    s.PermitUncheckedError();
  }
}

IOStatus PosixRandomRWFile::Write(uint64_t offset, const Slice& data,
                                  const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  const char* src = data.data();
  size_t nbytes = data.size();
  if (!PosixPositionedWrite(fd_, src, nbytes, static_cast<off_t>(offset))) {
    return IOError("While write random read/write file at offset " +
                       std::to_string(offset),
                   filename_, errno);
  }

  return IOStatus::OK();
}

IOStatus PosixRandomRWFile::Read(uint64_t offset, size_t n,
                                 const IOOptions& /*opts*/, Slice* result,
                                 char* scratch, IODebugContext* /*dbg*/) const {
  size_t left = n;
  char* ptr = scratch;
  while (left > 0) {
    ssize_t done = pread(fd_, ptr, left, offset);
    if (done < 0) {
      // error while reading from file
      if (errno == EINTR) {
        // read was interrupted, try again.
        continue;
      }
      return IOError("While reading random read/write file offset " +
                         std::to_string(offset) + " len " + std::to_string(n),
                     filename_, errno);
    } else if (done == 0) {
      // Nothing more to read
      break;
    }

    // Read `done` bytes
    ptr += done;
    offset += done;
    left -= done;
  }

  *result = Slice(scratch, n - left);
  return IOStatus::OK();
}

IOStatus PosixRandomRWFile::Flush(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus PosixRandomRWFile::Sync(const IOOptions& /*opts*/,
                                 IODebugContext* /*dbg*/) {
#ifdef HAVE_FULLFSYNC
  if (::fcntl(fd_, F_FULLFSYNC) < 0) {
    return IOError("while fcntl(F_FULLFSYNC) random rw file", filename_, errno);
  }
#else   // HAVE_FULLFSYNC
  if (fdatasync(fd_) < 0) {
    return IOError("While fdatasync random read/write file", filename_, errno);
  }
#endif  // HAVE_FULLFSYNC
  return IOStatus::OK();
}

IOStatus PosixRandomRWFile::Fsync(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
#ifdef HAVE_FULLFSYNC
  if (::fcntl(fd_, F_FULLFSYNC) < 0) {
    return IOError("While fcntl(F_FULLSYNC) random rw file", filename_, errno);
  }
#else   // HAVE_FULLFSYNC
  if (fsync(fd_) < 0) {
    return IOError("While fsync random read/write file", filename_, errno);
  }
#endif  // HAVE_FULLFSYNC
  return IOStatus::OK();
}

IOStatus PosixRandomRWFile::Close(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  if (close(fd_) < 0) {
    return IOError("While close random read/write file", filename_, errno);
  }
  fd_ = -1;
  return IOStatus::OK();
}

PosixMemoryMappedFileBuffer::~PosixMemoryMappedFileBuffer() {
  // TODO should have error handling though not much we can do...
  munmap(this->base_, length_);
}

/*
 * PosixDirectory
 */
#if !defined(BTRFS_SUPER_MAGIC)
// The magic number for BTRFS is fixed, if it's not defined, define it here
#define BTRFS_SUPER_MAGIC 0x9123683E
#endif
PosixDirectory::PosixDirectory(int fd, const std::string& directory_name)
    : fd_(fd), directory_name_(directory_name) {
  is_btrfs_ = false;
#ifdef OS_LINUX
  struct statfs buf;
  int ret = fstatfs(fd, &buf);
  is_btrfs_ = (ret == 0 && buf.f_type == static_cast<decltype(buf.f_type)>(
                                             BTRFS_SUPER_MAGIC));
#endif
}

PosixDirectory::~PosixDirectory() {
  if (fd_ >= 0) {
    IOStatus s = PosixDirectory::Close(IOOptions(), nullptr);
    s.PermitUncheckedError();
  }
}

IOStatus PosixDirectory::Fsync(const IOOptions& opts, IODebugContext* dbg) {
  return FsyncWithDirOptions(opts, dbg, DirFsyncOptions());
}

// Users who want the file entries synced in Directory project must call a
// Fsync or FsyncWithDirOptions function before Close
IOStatus PosixDirectory::Close(const IOOptions& /*opts*/,
                               IODebugContext* /*dbg*/) {
  IOStatus s = IOStatus::OK();
  if (close(fd_) < 0) {
    s = IOError("While closing directory ", directory_name_, errno);
  } else {
    fd_ = -1;
  }
  return s;
}

IOStatus PosixDirectory::FsyncWithDirOptions(
    const IOOptions& /*opts*/, IODebugContext* /*dbg*/,
    const DirFsyncOptions& dir_fsync_options) {
  assert(fd_ >= 0);  // Check use after close
  IOStatus s = IOStatus::OK();
#ifndef OS_AIX
  if (is_btrfs_) {
    // skip dir fsync for new file creation, which is not needed for btrfs
    if (dir_fsync_options.reason == DirFsyncOptions::kNewFileSynced) {
      return s;
    }
    // skip dir fsync for renaming file, only need to sync new file
    if (dir_fsync_options.reason == DirFsyncOptions::kFileRenamed) {
      std::string new_name = dir_fsync_options.renamed_new_name;
      assert(!new_name.empty());
      int fd;
      do {
        IOSTATS_TIMER_GUARD(open_nanos);
        fd = open(new_name.c_str(), O_RDONLY);
      } while (fd < 0 && errno == EINTR);
      if (fd < 0) {
        s = IOError("While open renaming file", new_name, errno);
      } else if (fsync(fd) < 0) {
        s = IOError("While fsync renaming file", new_name, errno);
      }
      if (close(fd) < 0) {
        s = IOError("While closing file after fsync", new_name, errno);
      }
      return s;
    }
    // fallback to dir-fsync for kDefault, kDirRenamed and kFileDeleted
  }

  // skip fsync/fcntl when fd_ == -1 since this file descriptor has been closed
  // in either the de-construction or the close function, data must have been
  // fsync-ed before de-construction and close is called
#ifdef HAVE_FULLFSYNC
  // btrfs is a Linux file system, while currently F_FULLFSYNC is available on
  // Mac OS.
  assert(!is_btrfs_);
  if (fd_ != -1 && ::fcntl(fd_, F_FULLFSYNC) < 0) {
    return IOError("while fcntl(F_FULLFSYNC)", "a directory", errno);
  }
#else   // HAVE_FULLFSYNC
  if (fd_ != -1 && fsync(fd_) == -1) {
    s = IOError("While fsync", "a directory", errno);
  }
#endif  // HAVE_FULLFSYNC
#endif  // OS_AIX
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
#endif
