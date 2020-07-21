#include <cstdint>
#include <list>
#include <unordered_map>
#include <iostream>
#include "mapped_pages_ro.h"
#include "file_linux.h"

void * MappedPagesRO::GetPage(const uint64_t address, const uint64_t size){
    //determine how many pages needed
    uint64_t address_align = address & ~(page_size_-1);
    uint64_t num_page = NumPageRequired(address, size);
    bool not_in_cache = (pg_entries_.find(address_align)==pg_entries_.end());
    
    if (!not_in_cache){
        //if present in cache, check if size is sufficient
        if (num_page <= pg_entries_[address_align].num_page){
            lru_seq_.erase(pg_entries_[address_align].lru_idx);
            lru_seq_.push_front(address_align);
            pg_entries_[address_align].lru_idx = lru_seq_.begin();
            return pg_entries_[address_align].page;
        } else {
            not_in_cache = true;
            //flush then remap
            PgEntry * pe = &pg_entries_[address_align];
            file_->EvictPage(page_size_*pe->num_page, pe->page);
            num_page_mapped_ -= pe->num_page;
            lru_seq_.erase(pe->lru_idx);
            pg_entries_.erase(address_align);
        }
    } 
    // load pages
    while (num_page_mapped_+num_page > k_num_pages_){
        // the record is larger than the space allocated for random mapping cache
        if (!num_page_mapped_) break;
        uint64_t evict_page = lru_seq_.back();
        PgEntry * evict_page_entry = &pg_entries_[evict_page];
        file_->EvictPage(page_size_*evict_page_entry->num_page, evict_page_entry->page);
        num_page_mapped_ -= evict_page_entry-> num_page;
        pg_entries_.erase(evict_page);
        lru_seq_.pop_back();
    }
    //load the page
    num_page_mapped_+=num_page;
    pg_entries_.insert({address_align, PgEntry()});
    auto pe = &pg_entries_[address_align];
    pe->num_page = num_page;
    pe->page = nullptr;
    Status load_return_status;
    pe->page = file_->LoadPage(page_size_*num_page, address_align, nullptr, load_return_status); 
    if (load_return_status==Status::FAILED){
        std::cout << "file load page failed\n";
        return nullptr;
    }
    //update the lru sequence. update the lrubit in pg_entries_
    lru_seq_.push_front(address_align);
    pe->lru_idx = lru_seq_.begin();
    return pe->page;
}

void MappedPagesRO::Close(){
    for (auto it=pg_entries_.begin(); it!=pg_entries_.end(); it++){
        if (it->second.page!=nullptr){
            file_->EvictPage(it->second.num_page*page_size_, it->second.page);
            it->second.page=nullptr;
        } 
    }
}