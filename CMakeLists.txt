cmake_minimum_required(VERSION 3.31)
project(CompressionBenchmark)

set(CMAKE_CXX_STANDARD 20)

include_directories(/Users/paul/workspace/duckdb/src/include)
link_directories(/Users/paul/workspace/duckdb/build/release/src)

include_directories(external/onpair)
add_subdirectory(external/onpair)

include_directories(src/algorithms)


add_executable(CompressionBenchmark main.cpp
    src/algorithms/fsst/libfsst.cpp
    src/algorithms/fsst/fsst_avx512.cpp
    src/algorithms/fsst12/libfsst12.cpp
)
target_link_libraries(CompressionBenchmark duckdb onpair)

add_executable(CompressionBenchmarkCLI cli_main.cpp
    src/algorithms/fsst/libfsst.cpp
    src/algorithms/fsst/fsst_avx512.cpp
    src/algorithms/fsst12/libfsst12.cpp
)
target_link_libraries(CompressionBenchmarkCLI duckdb onpair)

