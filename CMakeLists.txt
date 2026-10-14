cmake_minimum_required(VERSION 3.20)
project(low_latency_exchange LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

add_compile_options(-Wall -Wextra -Wpedantic)

# Sanitizer build: none, asan, ubsan or tsan (see CMakePresets.json).
set(SANITIZER "none" CACHE STRING "Sanitizer to build with: none, asan, ubsan, tsan")
if(SANITIZER STREQUAL "asan")
    set(SANITIZER_FLAGS -fsanitize=address -fno-omit-frame-pointer)
elseif(SANITIZER STREQUAL "ubsan")
    set(SANITIZER_FLAGS -fsanitize=undefined -fno-sanitize-recover=all)
elseif(SANITIZER STREQUAL "tsan")
    set(SANITIZER_FLAGS -fsanitize=thread)
elseif(NOT SANITIZER STREQUAL "none")
    message(FATAL_ERROR "Unknown SANITIZER '${SANITIZER}'")
endif()
add_compile_options(${SANITIZER_FLAGS})
add_link_options(${SANITIZER_FLAGS})

find_package(Threads REQUIRED)

# Dependencies, pinned. Only json.hpp is used from nlohmann/json, so its own
# CMake project is skipped (SOURCE_SUBDIR points at a directory with no CMakeLists).
include(FetchContent)
FetchContent_Declare(nlohmann_json
    URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    SOURCE_SUBDIR single_include
)
FetchContent_MakeAvailable(nlohmann_json)
add_library(nlohmann_json_header INTERFACE)
target_include_directories(nlohmann_json_header SYSTEM INTERFACE ${nlohmann_json_SOURCE_DIR}/single_include)

# Core library: everything except main programs.
add_library(exchange_core
    src/wire/Messages.cpp
    src/codec/BinaryCodec.cpp
    src/codec/SymbolTable.cpp
    src/codec/TextCodec.cpp
    src/core/MarketConfig.cpp
    src/core/OrderBook.cpp
    src/core/Worker.cpp
    src/core/ServerTrace.cpp
    src/net/Socket.cpp
    src/net/FrameReader.cpp
    src/net/StreamCodec.cpp
    src/net/Poller.cpp
    src/net/Mailbox.cpp
    src/net/ClientRegistry.cpp
    src/net/UdpPublisher.cpp
    src/net/Dispatcher.cpp
    src/server/Exchange.cpp
    src/client/ClientSession.cpp
    src/client/UdpListener.cpp
    src/client/RawConnection.cpp
    src/client/OrderLedger.cpp
    src/client/SessionTracer.cpp
)
target_include_directories(exchange_core PUBLIC include)
target_link_libraries(exchange_core PUBLIC Threads::Threads PRIVATE nlohmann_json_header)

# Programs
add_executable(exchange_server src/apps/server_main.cpp)
target_link_libraries(exchange_server PRIVATE exchange_core)
add_executable(exchange_client src/apps/client_main.cpp)
target_link_libraries(exchange_client PRIVATE exchange_core)
add_executable(simulation src/apps/simulation_main.cpp)
target_link_libraries(simulation PRIVATE exchange_core)
add_executable(benchmark
    src/apps/benchmark_main.cpp
    src/bench/Report.cpp
    src/bench/LoadClient.cpp
)
target_link_libraries(benchmark PRIVATE exchange_core)

# Tests
FetchContent_Declare(doctest
    URL https://github.com/doctest/doctest/archive/refs/tags/v2.4.12.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
FetchContent_MakeAvailable(doctest)

enable_testing()
add_executable(unit_tests
    tests/test_main.cpp
    tests/test_byte_io.cpp
    tests/test_binary_codec.cpp
    tests/test_frame_reader.cpp
    tests/test_core.cpp
    tests/test_text_codec.cpp
    tests/test_integration.cpp
)
target_link_libraries(unit_tests PRIVATE exchange_core doctest::doctest)
add_test(NAME unit_tests COMMAND unit_tests)
add_test(NAME simulation COMMAND simulation)
