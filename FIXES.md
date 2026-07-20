# Pome io_uring 并发/健壮性修复（简要清单）

本补丁加固 Pome（RocksDB 7.10 fork）在 io_uring 异步 compaction 路径上的一批
并发与健壮性问题。**第 1 类改动一律定性为"潜在问题的加固"，不断言原代码必然出错。**
改动 9 个文件（+356 / −101 行）。真机（172 核 / kernel 6.18 / DRAM 上的 ext4）
ThreadSanitizer 高并发压测 0 告警通过。

## 1. CAS 锁实现与潜在 data race 加固（potential）

- **队列领取原子化**（`env/io_posix.cc: get_empty_element`）：原"先查 `running` 再置 `true`"
  为非原子两步，*潜在*允许两个并发 compaction 领到同一 io_uring 队列；改为单条
  `compare_exchange` 原子领取，探测起点按 job id 分散以降低争用。
- **日志环领取原子化**（`get_empty_element_for_log`）：同上思路，只 CAS 领取真正空闲的
  环、忙则让出，*潜在*避免抢用他人尚未收割的完成事件。
- **共享队列提交串行化**（`ASync` / `AFsync`）：多 subcompaction 线程存在理论可能共享同一队列，
  *潜在*竞争 io_uring 提交队列与 `fds`/`sync_count` 记账；加每队列互斥锁 `qmtx` 覆盖
  get_sqe+prep+submit+记账（默认单线程无争用，非热路径）。锁序统一 `urings.mtx`(外)→`qmtx`(内)。
- **回收前置校验**（`wait_for_queue` + `producer_done` 标志）：仅当生产者已完成全部提交后
  才允许回收/复用其队列，潜在避免排空一个仍在提交中的队列。
- **计数器原子化**：`allowed_seeks`、`uring_queue::job_id` 改为 `std::atomic`（*潜在*无锁并发读写）。

## 2. 完备性 / 健壮性修复

- **无 io_uring 内核下的回退**（`init_queues` / `clear_all`）：队列指针数组值初始化 + 逐槽判空 +
  初始化失败时释放两侧数组，避免在不支持 io_uring 的内核/容器上开库时崩溃。
- **disable_wal 崩溃**（`db/memtable.h`）：`MemTable::uq` 补默认 `= nullptr`，避免关闭 WAL 时
  可能对未初始化裸指针解引用。
- **空指针防护**（`ASync` / `AFsync`）：`uptr`/`sqe` 为空或 `io_uring_submit()<=0` 时返回 `IOError`
  （此前对空 `sqe` 调 `prep_fsync` 会 SIGSEGV），并把记账移到提交成功之后。
- **异步 fsync 错误不再静默**（`wait_for_queue`、`db/builder.cc`）：检查 `cqe->res`，失败则置
  `sync_failed` 隔离该队列并保留其祖先文件；`builder` 在 WAL 同步等待失败时置错误状态。

- **文件回收集中化**（`wait_for_queue`）：整批 fsync 确认完成后即时回收输入文件，避免
  `reserve_input`/`no_ref` 无界增长。
- **内存正确性**（`clear_all`、日志环）：`new[]` 数组改用 `delete[]` 并逐槽 `delete`（修 UB 与
  对象泄漏）；日志环记账在 flush 后复位，避免 `fds` 无界增长。

## 改动文件
`db/builder.cc`, `db/compaction/compaction_job.cc`, `db/compaction/compaction_picker_level.cc`,
`db/db_impl/db_impl_open.cc`, `db/db_impl/db_impl_write.cc`, `db/memtable.h`,
`db/version_set.cc`, `env/io_posix.cc`, `env/io_posix.h`

## 验证
- 真机 ThreadSanitizer（172 核，DRAM ext4，`max_subcompactions=8`，8 写线程 / 12 后台 job）：0 告警，PASS。
- 真机 Release 重压（16 写线程 / 24 后台 job / SUBC=8 / 4.8M key）：无崩溃 / 卡死 / 死锁 / 数据丢失。
- 本地 ThreadSanitizer 4 场景（WAL / NOWAL / SUBC=4 / 高 churn）：0 告警。
