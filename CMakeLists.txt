cmake_minimum_required(VERSION 3.5.1)

project(kv VERSION 0.0.0)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ADDITIONAL_CFLAGS} -O3")

add_subdirectory(src)

add_subdirectory(test)
