set(SOURCES system_catelog.cc 
            mapped_pages_ro.cc 
            log_storage.cc
            flexible_log.cc
            hash_index.cc
            kv.cc
)
add_library(kv STATIC ${SOURCES})
target_include_directories(kv PRIVATE ../include)