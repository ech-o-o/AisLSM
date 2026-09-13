// Integration test: drive the REAL Pome (RocksDB fork) async-compaction path
// under concurrency via the public API. Tiny buffers force many flushes and
// concurrent compactions, so get_empty_element() (the CAS-fixed allocator) and
// the async-fsync path run hot across 8 background threads. Also runs a
// disable_wal pass to exercise the MemTable::uq=nullptr fix (bug #4).
//
// Pass = all puts succeed, all keys readable after compaction, and survive a
// reopen (durability), with no crash/corruption.
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

using namespace rocksdb;

static int run(bool disable_wal, const std::string& dir, int subcompactions,
               int max_wbn, const char* tag) {
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 256 * 1024;       // tiny -> many flushes
  options.max_write_buffer_number = max_wbn;    // high => stress WAL log rings
  options.target_file_size_base = 256 * 1024;   // tiny SSTs -> many compactions
  options.level0_file_num_compaction_trigger = 2;
  options.max_background_jobs = 8;              // concurrent compactions/flushes
  options.max_subcompactions = subcompactions;  // >1 exercises shared-uptr path (bug#2 fix)

  DestroyDB(dir, options);
  DB* db = nullptr;
  Status s = DB::Open(options, dir, &db);
  if (!s.ok()) { printf("Open failed: %s\n", s.ToString().c_str()); return 2; }

#ifndef KPT
#define KPT 40000
#endif
  const int kThreads = 4;
  const int kPerThread = KPT;  // 160k keys total by default; smaller under TSan
  WriteOptions wo; wo.disableWAL = disable_wal;
  std::atomic<long> put_errors{0};

  auto writer = [&](int t) {
    char key[32], val[128];
    for (int i = 0; i < kPerThread; ++i) {
      snprintf(key, sizeof key, "k%02d_%08d", t, i);
      snprintf(val, sizeof val, "val_%d_%d_padpadpadpadpadpadpadpadpadpad", t, i);
      if (!db->Put(wo, key, val).ok()) put_errors.fetch_add(1);
    }
  };
  std::vector<std::thread> ts;
  for (int t = 0; t < kThreads; ++t) ts.emplace_back(writer, t);
  for (auto& th : ts) th.join();

  db->Flush(FlushOptions());
  db->CompactRange(CompactRangeOptions(), nullptr, nullptr);

  long missing = 0;
  std::string got;
  char key[32];
  for (int t = 0; t < kThreads; ++t)
    for (int i = 0; i < kPerThread; ++i) {
      snprintf(key, sizeof key, "k%02d_%08d", t, i);
      if (!db->Get(ReadOptions(), key, &got).ok()) missing++;
    }
  delete db;

  // reopen and re-verify (durability across close/open)
  long missing_after_reopen = 0;
  db = nullptr;
  s = DB::Open(options, dir, &db);
  if (!s.ok()) { printf("Reopen failed: %s\n", s.ToString().c_str()); return 2; }
  for (int t = 0; t < kThreads; ++t)
    for (int i = 0; i < kPerThread; ++i) {
      snprintf(key, sizeof key, "k%02d_%08d", t, i);
      if (!db->Get(ReadOptions(), key, &got).ok()) missing_after_reopen++;
    }
  delete db;
  DestroyDB(dir, options);

  printf("[%s] put_errors=%ld missing_after_compact=%ld missing_after_reopen=%ld\n",
         tag, put_errors.load(), missing, missing_after_reopen);
  // NOTE: with disable_wal, data is not expected durable across reopen; only
  // require no crash + all keys present before close for that pass.
  bool ok = (put_errors.load() == 0) && (missing == 0) &&
            (disable_wal || missing_after_reopen == 0);
  return ok ? 0 : 1;
}

int main() {
  int rc = 0;
  rc |= run(false, "/tmp/pome_test_wal",   1, 4, "WAL");
  rc |= run(true,  "/tmp/pome_test_nowal", 1, 4, "NOWAL");
  // exercises the shared-uptr subcompaction path (bug#2 fix: per-queue qmtx)
  rc |= run(false, "/tmp/pome_test_sub",   4, 4, "SUBC=4");
  // high memtable churn stresses the WAL log-ring allocator (bug#3/#6 fixes)
  rc |= run(false, "/tmp/pome_test_churn", 1, 8, "CHURN");
  printf(rc == 0 ? "ALL PASS\n" : "TEST FAILED\n");
  return rc;
}
