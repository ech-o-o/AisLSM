// Heavy multi-thread LOCK-CORRECTNESS stress for the fixed Pome (RocksDB fork).
// Goal (per user): validate lock/race correctness under many threads, NOT
// persistence. Tiny files + high thread/subcompaction counts maximize the rate
// at which the Pome io_uring paths (get_empty_element, ASync/AFsync qmtx,
// wait_for_queue, log-ring allocator) are hit concurrently. Run under
// ThreadSanitizer for the definitive check.
//
// Env knobs (all optional):
//   THREADS   writer threads            (default 8)
//   BGJOBS    max_background_jobs       (default 16)
//   SUBC      max_subcompactions        (default 4)   >1 exercises shared-uptr qmtx (bug#2)
//   WBN       max_write_buffer_number   (default 8)   high churns WAL log rings (bug#3/#6)
//   WBUF      write_buffer_size bytes   (default 65536=64KB) tiny -> many flushes
//   FSZ       target_file_size_base     (default 65536=64KB) tiny -> many compactions
//   KEYS      keys per writer thread    (default 30000)
//   ROUNDS    open/stress/close rounds  (default 3)
//   NOWAL     1 => disable_wal          (default 0)
//   DIR       db directory              (default /mnt/pomeram/db)
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

using namespace rocksdb;

static int geti(const char* k, int d){ const char* v=getenv(k); return v?atoi(v):d; }

int main() {
  const int THREADS = geti("THREADS", 8);
  const int BGJOBS  = geti("BGJOBS", 16);
  const int SUBC    = geti("SUBC", 4);
  const int WBN     = geti("WBN", 8);
  const int WBUF    = geti("WBUF", 65536);
  const int FSZ     = geti("FSZ", 65536);
  const int KEYS    = geti("KEYS", 30000);
  const int ROUNDS  = geti("ROUNDS", 3);
  const bool NOWAL  = geti("NOWAL", 0) != 0;
  const char* DIR   = getenv("DIR") ? getenv("DIR") : "/mnt/pomeram/db";

  printf("STRESS cfg: THREADS=%d BGJOBS=%d SUBC=%d WBN=%d WBUF=%d FSZ=%d KEYS=%d ROUNDS=%d NOWAL=%d DIR=%s\n",
         THREADS, BGJOBS, SUBC, WBN, WBUF, FSZ, KEYS*THREADS, ROUNDS, (int)NOWAL, DIR);

  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = WBUF;
  options.max_write_buffer_number = WBN;
  options.min_write_buffer_number_to_merge = 1;
  options.target_file_size_base = FSZ;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 8;
  options.max_background_jobs = BGJOBS;
  options.max_subcompactions = SUBC;

  long total_errors = 0, total_missing = 0;
  for (int r = 0; r < ROUNDS; ++r) {
    DestroyDB(DIR, options);
    DB* db = nullptr;
    Status s = DB::Open(options, DIR, &db);
    if (!s.ok()) { printf("Open failed: %s\n", s.ToString().c_str()); return 2; }

    WriteOptions wo; wo.disableWAL = NOWAL;
    std::atomic<long> errs{0};
    static std::atomic<bool> printed{false};
    auto writer = [&](int t){
      char key[40], val[160];
      for (int i = 0; i < KEYS; ++i) {
        snprintf(key, sizeof key, "r%d_t%02d_%09d", r, t, i);
        snprintf(val, sizeof val, "val_%d_%d_%d_padpadpadpadpadpadpadpadpadpadpad", r, t, i);
        Status ps = db->Put(wo, key, val);
        if (!ps.ok()) {
          errs.fetch_add(1);
          bool ex=false;
          if (printed.compare_exchange_strong(ex,true))
            printf("  first Put error: %s\n", ps.ToString().c_str());
        }
        // occasional explicit flush to force more flush/compaction concurrency
        if ((i & 8191) == 8191) db->Flush(FlushOptions());
      }
    };
    std::vector<std::thread> ts;
    for (int t = 0; t < THREADS; ++t) ts.emplace_back(writer, t);
    for (auto& th : ts) th.join();

    db->Flush(FlushOptions());
    db->CompactRange(CompactRangeOptions(), nullptr, nullptr);

    long missing = 0; std::string got; char key[40];
    for (int t = 0; t < THREADS; ++t)
      for (int i = 0; i < KEYS; ++i) {
        snprintf(key, sizeof key, "r%d_t%02d_%09d", r, t, i);
        if (!db->Get(ReadOptions(), key, &got).ok()) missing++;
      }
    printf("  round %d: put_errors=%ld missing=%ld\n", r, errs.load(), missing);
    total_errors += errs.load(); total_missing += missing;
    delete db;
  }
  DestroyDB(DIR, options);
  printf("TOTAL: put_errors=%ld missing=%ld -> %s\n",
         total_errors, total_missing,
         (total_errors==0 && total_missing==0) ? "PASS" : "FAIL");
  return (total_errors==0 && total_missing==0) ? 0 : 1;
}
