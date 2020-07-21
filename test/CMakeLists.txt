set(LIBRARIES pthread kv)

add_executable(input_gen input_gen.cc)

add_executable(testkv testkv.cc)
target_include_directories(testkv PRIVATE ../include)
target_link_libraries(testkv PUBLIC "${LIBRARIES}")

add_executable(testkv_reload testkv2.cc)
target_include_directories(testkv_reload PRIVATE ../include)
target_link_libraries(testkv_reload PUBLIC "${LIBRARIES}")