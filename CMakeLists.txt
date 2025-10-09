cmake_minimum_required(VERSION 3.31)
project(CompressionBenchmark)

set(ENABLE_SANITIZER OFF CACHE BOOL "" FORCE)
set(ENABLE_UBSAN OFF CACHE BOOL "" FORCE)
add_subdirectory(external/duckdb)
include_directories(external/duckdb/src/include)

include_directories(external/onpair)
add_subdirectory(external/onpair)

include_directories(src/algorithms)

add_executable(CompressionBenchmark main.cpp
    src/algorithms/fsst/libfsst.cpp
    src/algorithms/fsst/fsst_avx512.cpp
    src/algorithms/fsst12/libfsst12.cpp
)
target_link_libraries(CompressionBenchmark duckdb onpair)
set_target_properties(CompressionBenchmark PROPERTIES CXX_STANDARD 20)

add_executable(CompressionBenchmarkCLI cli_main.cpp
    src/algorithms/fsst/libfsst.cpp
    src/algorithms/fsst/fsst_avx512.cpp
    src/algorithms/fsst12/libfsst12.cpp
)
target_link_libraries(CompressionBenchmarkCLI duckdb onpair)
set_target_properties(CompressionBenchmarkCLI PROPERTIES CXX_STANDARD 20)

include_directories(external/lz4/lib)
add_subdirectory(external/lz4/build/cmake)
target_link_libraries(CompressionBenchmark lz4)
target_link_libraries(CompressionBenchmarkCLI lz4)

