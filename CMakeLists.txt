cmake_minimum_required(VERSION 3.31)
project(CompressionBenchmark)

set(CMAKE_CXX_STANDARD 20)

include_directories(/Users/paul/workspace/duckdb/src/include)
link_directories(/Users/paul/workspace/duckdb/build/release/src)

include_directories(external/fsst)
add_subdirectory(external/fsst)

include_directories(external/onpair)
add_subdirectory(external/onpair)


add_executable(CompressionBenchmark main.cpp)
target_link_libraries(CompressionBenchmark duckdb fsst onpair)

add_executable(CompressionBenchmarkCLI cli_main.cpp)
target_link_libraries(CompressionBenchmarkCLI duckdb fsst onpair)

