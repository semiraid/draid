cmake_minimum_required(VERSION 3.14)
project(draid_server CXX)

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

# ── Helper macro to configure a RAID server executable ────────────────────────
macro(add_raid_server target source)
    add_executable(${target} ${source})

    target_compile_options(${target} PRIVATE
        ${COMMON_CXXFLAGS}
        ${SPDK_CFLAGS_LIST}
    )

    target_include_directories(${target} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../shared
        ${DPDK_INCLUDE}
        ${ISAL_INCLUDE}
    )

    target_link_libraries(${target} PRIVATE
        -pthread
        -Wl,--whole-archive,-Bstatic
        ${SPDK_LDFLAGS_LIST}
        -Wl,--no-whole-archive,-Bdynamic
        ${SPDK_SYSLIBS_LIST}
        pthread
        numa
        dl
        ibverbs
    )
endmacro()

# ── Executables: raid5 / raid6 ────────────────────────────────────────────────
add_raid_server(raid5 raid5.cc)
add_raid_server(raid6 raid6.cc)
add_raid_server(raidx raidx.cc)
