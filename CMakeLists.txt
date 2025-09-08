cmake_minimum_required(VERSION 3.31)
project(CompressionBenchmark)

set(CMAKE_CXX_STANDARD 20)

add_library(CompressionBenchmark STATIC library.cpp)
