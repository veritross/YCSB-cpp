cmake_minimum_required(VERSION 3.10)
project(hashmap-kvssd)
set(CMAKE_CXX_STANDARD 17)
enable_testing()

set(CMAKE_BUILD_TYPE Release)
set(CompilerFlags
        CMAKE_CXX_FLAGS
        CMAKE_CXX_FLAGS_DEBUG
        CMAKE_CXX_FLAGS_RELEASE
        CMAKE_C_FLAGS
        CMAKE_C_FLAGS_DEBUG
        CMAKE_C_FLAGS_RELEASE
        )
        
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -g")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread -g")
        
set(TARGET_APP kvssd_test)
set(SrcLib SrcLib)
set(SrcFiles kvssd_hashmap_db_impl.cc kvssd_hashmap_db.cc)

add_library(${SrcLib} STATIC ${SrcFiles})
target_include_directories(${SrcLib} PUBLIC ${CMAKE_SOURCE_DIR})
target_include_directories(${SrcLib} PUBLIC ${CMAKE_SOURCE_DIR}/../)
add_subdirectory(googletest-1.15.2)
add_executable(${TARGET_APP} test/kvssd_test.cc)
target_include_directories(${TARGET_APP} PUBLIC ${CMAKE_SOURCE_DIR})
target_include_directories(${TARGET_APP} PUBLIC ${CMAKE_SOURCE_DIR}/../)
target_link_libraries(${TARGET_APP} gtest_main ${SrcLib})

include(GoogleTest)
gtest_discover_tests(${TARGET_APP})