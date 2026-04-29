cmake_minimum_required(VERSION 3.14)
project(draid_host CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ── SPDK / DPDK paths ──────────────────────────────────────────────────────────
set(SPDK_ROOT    "$ENV{HOME}/SemiRAID/deps/semiraid-spdk")
set(SPDK_LIB_DIR "${SPDK_ROOT}/build/lib")
set(DPDK_INCLUDE "${SPDK_ROOT}/dpdk/build/include")
set(ISAL_INCLUDE "${SPDK_ROOT}/isa-l/include")

# ── pkg-config: query SPDK flags ───────────────────────────────────────────────
set(ENV{PKG_CONFIG_PATH} "${SPDK_LIB_DIR}/pkgconfig")
find_package(PkgConfig REQUIRED)

execute_process(
    COMMAND pkg-config --cflags spdk_event spdk_event_bdev spdk_env_dpdk
    OUTPUT_VARIABLE SPDK_CFLAGS
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE _ret
)
if(NOT _ret EQUAL 0)
    message(FATAL_ERROR "pkg-config failed to find spdk_event/spdk_event_bdev/spdk_env_dpdk")
endif()

execute_process(
    COMMAND pkg-config --libs spdk_event spdk_event_bdev spdk_env_dpdk
    OUTPUT_VARIABLE SPDK_LDFLAGS
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE _ret
)
if(NOT _ret EQUAL 0)
    message(FATAL_ERROR "pkg-config failed to query SPDK link flags")
endif()

execute_process(
    COMMAND pkg-config --libs --static spdk_syslibs
    OUTPUT_VARIABLE SPDK_SYSLIBS
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE _ret
)
if(NOT _ret EQUAL 0)
    message(FATAL_ERROR "pkg-config failed to find spdk_syslibs")
endif()

separate_arguments(SPDK_CFLAGS_LIST  UNIX_COMMAND "${SPDK_CFLAGS}")
separate_arguments(SPDK_LDFLAGS_LIST UNIX_COMMAND "${SPDK_LDFLAGS}")
separate_arguments(SPDK_SYSLIBS_LIST UNIX_COMMAND "${SPDK_SYSLIBS}")

# ── Common compile options ─────────────────────────────────────────────────────
set(COMMON_CXXFLAGS -g -O0)

# ── Static library: bdev_raid_rpc ──────────────────────────────────────────────
# Sources: bdev_raid_rpc.cc  bdev_raid.cc  raid5.cc  raid6.cc
add_library(bdev_raid_rpc STATIC
    bdev_raid_rpc.cc
    bdev_raid.cc
    raid5.cc
    raid6.cc
    raidx.cc
)

target_compile_options(bdev_raid_rpc PRIVATE
    ${COMMON_CXXFLAGS}
    -fPIC
    ${SPDK_CFLAGS_LIST}
)

target_include_directories(bdev_raid_rpc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../shared
    ${DPDK_INCLUDE}
    ${ISAL_INCLUDE}
)

# ── Executable: rpc_raid_main ──────────────────────────────────────────────────
add_executable(rpc_raid_main rpc_raid_main.cc)

target_compile_options(rpc_raid_main PRIVATE
    ${COMMON_CXXFLAGS}
    ${SPDK_CFLAGS_LIST}
)

target_include_directories(rpc_raid_main PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../shared
    ${DPDK_INCLUDE}
    ${ISAL_INCLUDE}
)

# Linking order mirrors the original Makefile:
#   -pthread
#   -Wl,--whole-archive,-Bstatic  <SPDK libs>  <bdev_raid_rpc.a>
#   -Wl,--no-whole-archive,-Bdynamic  <sys libs>  <extra libs>
target_link_libraries(rpc_raid_main PRIVATE
    -pthread
    -Wl,--whole-archive,-Bstatic
    ${SPDK_LDFLAGS_LIST}
    bdev_raid_rpc
    -Wl,--no-whole-archive,-Bdynamic
    ${SPDK_SYSLIBS_LIST}
    pthread
    numa
    dl
    ibverbs
)
