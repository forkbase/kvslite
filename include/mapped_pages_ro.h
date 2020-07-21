#ifndef KV_MAPPEDPAGESRO_H_
#define KV_MAPPEDPAGESRO_H_

#include <cstdint>
#include <list>
#include <unordered_map>
#include "file_linux.h"

class MappedPagesRO{
private:
    struct PgEntry{
        unsigned int num_page = 0;
        std::list<uint64_t>::iterator lru_idx;
        void* page = nullptr; //point to the buffer page allocated        
    };

    inline uint64_t NumPageRequired(const uint64_t address, const uint64_t size){
        if (!size) return 1;
        uint64_t address_align = address & ~(page_size_-1);
        return (size+address-address_align-1) / page_size_+1;
    }

public:
    MappedPagesRO()=default;
    MappedPagesRO(const MappedPagesRO&) = delete;
    MappedPagesRO& operator=(const MappedPagesRO&) = delete;
    MappedPagesRO(MappedPagesRO&&) = delete;
    MappedPagesRO& operator=(MappedPagesRO&&) = delete;

    inline void Initialize(File *file, const uint64_t k_num_pages){
        page_size_ = sysconf(_SC_PAGE_SIZE);
        k_num_pages_ = k_num_pages;
        num_page_mapped_ = 0;
        file_=file;
    }

    void * GetPage(const uint64_t address, const uint64_t size);

    void Close();

    ~MappedPagesRO()=default;
private: 
    uint64_t page_size_ = 4096;
    uint64_t k_num_pages_ = 16;
    uint64_t num_page_mapped_; //number of page mapped in
    std::list<uint64_t> lru_seq_;
    std::unordered_map<uint64_t, PgEntry> pg_entries_; //key is offset in file
    File * file_;
};

#endif