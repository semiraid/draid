# dRAID Implementation

## Contents

- `dRAID/` contains core implementation of dRAID (Distributed RAID).
  - `shared/` contains global variables and parameters shared by all code.
  - `host/` contains the host-side implementation.
    - `bdev_raid.h` & `bdev_raid.cc` contain the common controller logic shared by both RAID-5 and RAID-6.
    - `raid5.cc` contains core implementation of RAID-5 host controller.
    - `raid6.cc` contains core implementation of RAID-6 host controller.
    - `raidx.cc` contains the in-progress N+M (`raidx`) host controller.
    - `bdev_raid_rpc.cc` is for SPDK RPC framework integration.
    - `rpc_raid_main.cc` is the basic test program for dRAID for debugging purpose.
  - `server/` contains the server-side implementation.
    - `raid5.cc` contains core implementation of RAID-5 server controller.
    - `raid6.cc` contains core implementation of RAID-6 server controller.
    - `raidx.cc` contains the in-progress N+M (`raidx`) server controller.
    - `nvme0.json` is the configuration for NVMe SSD.
    - `malloc0.json` is the configuration for RAM Disk.

This is the dRAID baseline used for comparison with SemiRAID. Lives under
`deps/draid/` in the SemiRAID repo; tested via `test/fio/draid/`,
`test/smoke/draid/`, and `test/ycsb/draid/`.

## Compile

```bash
cd ~/SemiRAID/deps/draid/host
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug -Wno-dev && make -j$(nproc) rpc_raid_main
```
