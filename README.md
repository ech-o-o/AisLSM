# Pome Open Source Project Introduction

Welcome to the Pome open source project! This project is based on the Linux `io_uring` technology and aims to achieve optimal I/O operation performance for RocksDB. 

## Environment Requirements

1. **Linux Version Requirement**: Since `io_uring` is still maturing and continuously updating, it's recommended to use Linux version 6.2 or higher. However, at a minimum, version 5.2 is required to expect the software to reach its optimized performance.

2. **Dependency**: This project relies on the `liburing` library. You can download and install it from GitHub. Here's the installation link:

   [liburing GitHub](https://github.com/axboe/liburing)

  Installing liburing is quite straightforward. You can find installation instructions from the aforementioned GitHub link. Before installing, it's imperative to remove any existing liburing library from your Linux system to avoid unpredictable bugs during program execution

## Compilation Steps

1. First, ensure you've installed all the necessary dependencies and git clone this repository.

2. Compile using the following commands in the directory that you cloned:

   ```bash
   mkdir -p build && cd build
   cmake -DCMAKE_BUILD_TYPE=Release ..  -DWITH_SNAPPY=1 &&  cmake --build .

## Reference 

1. Yanpeng Hu, Li Zhu, Lei Jia, Chundong Wang. **Pome: Parallelizing I/Os and Computations for Efficient LSM-tree-based Data Storage.** To be presented at the 35th ACM International Symposium on High-Performance Parallel and Distributed Computing (HPDC 2026) as a technical paper.
