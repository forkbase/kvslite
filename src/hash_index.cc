#include <cstdint>
#include <cstring>
#include <iostream>
#include <list>
#include <mutex>
#include "hash_index.h"
#include "status.h"
#include "file_linux.h"

// for debug 
#include <chrono>

HashIndex::MappedPage * HashIndex::LoadPage(uint64_t offset, uint32_t directory_idx){
    unsigned int idx;
    MappedPage * mp;
    if (in_mem_page_count_!=kNumInMemPage) {
        idx = in_mem_page_count_;
        mp = &im_pages_[idx];
        in_mem_page_count_++;
    } else {
        {
            std::lock_guard<std::mutex> latch(lru_seq_tail_mtx_);
            idx = lru_seq_.back();
            lru_seq_.pop_back();
        }
        mp = &im_pages_[idx];
        {
            std::lock_guard<std::recursive_mutex> wlatch(mp->page_mtx);
            entries_[mp->dir_idx].page_ptr = nullptr;
            if (mp->dirtybit){
                mp->dirtybit = 0;
                file_->FlushPage(page_size_, mp->page);
                // locked_cout(mp->content, mp->idx, "flush for evict");
            }
        }
        // evict logic here
        file_->EvictPage(page_size_, mp->page);
        // std::cout << " Evict then";
        // locked_cout(mp->content, mp->idx, "evict");
    }
    Status load_return_status;
    mp->page = file_->LoadPage(page_size_, offset, nullptr, load_return_status);
    if (load_return_status==Status::FAILED){
        std::cout << "file load page failed" << std::endl;
        return nullptr;
    }
    // std::cout << "load page (index)\n";
    mp->dir_idx = directory_idx;
    mp->idx = idx;
    lru_seq_.push_front(idx);
    mp->lru_idx = lru_seq_.begin();
    return mp; 
}

// background thread disabled on flushing index page
Status HashIndex::FlushLRUPage(){ 
    std::unique_lock<std::mutex> ltc(lru_seq_tail_mtx_);
    if (lru_seq_.empty()) return Status::FAILED;
    unsigned int idx = lru_seq_.back();
    MappedPage & mp = im_pages_[idx];
    if (!mp.dirtybit) return Status::FAILED;
    std::lock_guard<std::recursive_mutex> wlatch(mp.page_mtx);
    ltc.unlock();
    file_->FlushPage(page_size_, mp.page);
    mp.dirtybit=0;
    // locked_cout(mp.content, idx, "flush");
    // std::cout << "\n" << std::this_thread::get_id() << " flush index page " << mp.idx << "\n";
    return Status::SUCCESS;
}

HashIndex* HashIndex::Initialize(){
    void * buff = malloc((1<<global_depth_)*sizeof(DirEntry)); 
    //valgrind report error during write, if uninitialized. (maybe due to padding or some other things uninitalized)
    std::memset(buff, 0, (1<<global_depth_)*sizeof(DirEntry));
    if (!buff) {
        perror("alloc failed");
        return nullptr;
    }
    entries_ = reinterpret_cast<DirEntry*> (buff);
    for (int i=0; i<(1<<global_depth_); ++i){
        entries_[i].local_depth=global_depth_;
        entries_[i].overflown_bucket_cur = bucket_per_page_/2;
        entries_[i].offset=UINT64_MAX;
        entries_[i].page_ptr = nullptr;
    }
    return this;
}

HashIndex* HashIndex::LoadFromFile(File* file_in){
    uint64_t temp;
    uint64_t offset=0;
    file_in->Read(sizeof(page_size_), offset, &temp);
    //need at least match page size.
    if(temp!=page_size_) {
        printf("LOAD FAILED: page size does not match.");
        return nullptr; 
    }
    offset+=sizeof(page_size_);
    file_in->Read(sizeof(num_page_alloc_ed_), offset, &num_page_alloc_ed_);
    offset+=sizeof(num_page_alloc_ed_);
    file_in->Read(sizeof(global_depth_), offset, &global_depth_);
    offset+=sizeof(global_depth_);
    void * buff = malloc((1<<global_depth_)*sizeof(DirEntry));
    std::memset(buff, 0, (1<<global_depth_)*sizeof(DirEntry));
    if (!buff) {
        perror("alloc failed");
        return nullptr;
    }
    file_in->Read((1<<global_depth_)*sizeof(DirEntry), offset, buff);
    entries_ = reinterpret_cast<DirEntry* >(buff);
    for (int i=0; i<(1<<global_depth_); i++){
        entries_[i].page_ptr=nullptr;
    }
    file_=file_in;
    return this;
}

Status HashIndex::Get(const uint64_t record, uint64_t &log_addr){
    
    // auto index_start = std::chrono::high_resolution_clock::now();

    uint64_t idx = RefDirIdx(record);
    DirEntry& de = entries_[idx];
    {
        // std::cout << "\nhash value: " << record << " Directory index: " << idx << "\n";
        // auto bp_time = std::chrono::high_resolution_clock::now();
        // std::cout << "GET1: " << std::chrono::duration_cast<std::chrono::nanoseconds>(bp_time-index_start).count() << " ";

    }
    //page not even allocated
    //extended pages will have offset linked to the previous page @initialization
    if (de.offset==UINT64_MAX) return Status::FAILED;
    
    if (de.page_ptr == nullptr) de.page_ptr = LoadPage(de.offset*page_size_, idx);
    MappedPage & mp = *de.page_ptr;
    {
        // std::cout << "bucket idx: " << BucketIdx(record);
        // auto bp_time = std::chrono::high_resolution_clock::now();
        // std::cout << "GET2: " << std::chrono::duration_cast<std::chrono::nanoseconds>(bp_time-index_start).count() << " ";
    }
    HashBucket * bucket = GetBucket(BucketIdx(record), mp.page);
    HashEntry * itr = reinterpret_cast<HashEntry*>(bucket);
    uint64_t validbits = GetRecordValidate(record, de.local_depth);
    int count = kNumEntryPerBucket -1; //track if it encounters an overflown bucket
    
    ResetPageLru(mp);
    {
        // auto bp_time = std::chrono::high_resolution_clock::now();
        // std::cout << "GET3: " << std::chrono::duration_cast<std::chrono::nanoseconds>(bp_time-index_start).count() << " ";
    }
    std::unique_lock<std::recursive_mutex> wlatch(mp.page_mtx);
    {
        // auto bp_time = std::chrono::high_resolution_clock::now();
        // std::cout << "GET4: " << std::chrono::duration_cast<std::chrono::nanoseconds>(bp_time-index_start).count() << " ";
    }
    //last entry on a page will never be taken. 
    while(itr->CheckTaken()){
        //validation bit matched record inserted before
        if (itr->Validation(bucket_per_page_, de.local_depth) == validbits){
            // has been deleted
            if (itr->CheckDeleted()){
                return Status::FAILED;
            } else {
                //not deleted
                log_addr = itr->GetAddress();
                return Status::SUCCESS;    
            }
        }
        //if overflown bucket reached
        if(!count){
            uint64_t overflown_bucket_idx = itr->GetOverflownBucket(bucket_per_page_);
            //reached the hash bucket end. no more to check
            if (!overflown_bucket_idx) return Status::FAILED;
            {
                // std::cout << " overflow bucket: " << overflown_bucket_idx;
            }
            itr = reinterpret_cast<HashEntry*>(GetBucket(overflown_bucket_idx, mp.page));
            count = kNumEntryPerBucket-1;
        } else{
            itr++;
            count--;
        } 
    }    
    //all taken entries have been checked yet not found.
    return Status::FAILED;
}

Status HashIndex::Put(const uint64_t record, uint64_t &log_addr){
    
    uint64_t idx = RefDirIdx(record);
    DirEntry& de = entries_[idx];
    {
        // std::cout << "\nhash value: " << record << " Directory index: " << idx << "\n";
    }
    //assign the offset in file to a new directory entry
    if (de.offset==UINT64_MAX){
        de.offset = num_page_alloc_ed_;
        num_page_alloc_ed_++;
        de.page_ptr = LoadPage(de.offset*page_size_, idx);
        //initialize all values to zero.
        std::lock_guard<std::recursive_mutex> wlatch(de.page_ptr->page_mtx);
        std::memset(de.page_ptr->page, 0, page_size_);
        UpdatePageLocalDepth(de.page_ptr->page, de.local_depth);
        SetDirty(*(de.page_ptr));
    } else if (de.page_ptr == nullptr) de.page_ptr = LoadPage(de.offset*page_size_, idx);

    MappedPage & mp = *de.page_ptr;
    HashBucket * bucket = GetBucket(BucketIdx(record), mp.page);
    {
        // std::cout << "bucket idx: " << BucketIdx(record);
    }
    HashEntry * itr = reinterpret_cast<HashEntry*>(bucket);
    uint64_t validbits = GetRecordValidate(record, de.local_depth);
    int count = kNumEntryPerBucket -1; //track if it encounters an overflown bucket

    HashEntry * reserved = nullptr;
    ResetPageLru(mp);
    std::unique_lock<std::recursive_mutex> wlatch(mp.page_mtx);
    //last entry on a page will never be taken. 
    while(itr->CheckTaken()){
        //validation bit matched record inserted before
        if (itr->Validation(bucket_per_page_, de.local_depth) == validbits){
            // has been deleted
            if (itr->CheckDeleted()){
                itr->IncChainLength();
                itr->UnsetDeleted();
                itr->SetAddress(log_addr);
                SetDirty(mp);
                return Status::SUCCESS;
            } else {
                //not deleted
                log_addr = itr->GetAddress();
                return Status::FAILED;
            }
        }
        //taken by other record but deleted
        if (itr->CheckDeleted() && (!reserved)){
            reserved = itr;
        }
        //if overflown bucket reached
        if(!count){
            uint64_t overflown_bucket_idx = itr->GetOverflownBucket(bucket_per_page_);
            if (!overflown_bucket_idx){
                //if there is overflown bucket that can be allocated.
                if (de.overflown_bucket_cur == bucket_per_page_) {
                    Extend(record);
                    wlatch.unlock();
                    return Put(record, log_addr);
                }
                {
                    // std::cout << " (new)";
                }
                itr->SetOverflownBucket(de.overflown_bucket_cur);
                overflown_bucket_idx = de.overflown_bucket_cur;
                de.overflown_bucket_cur++;
            }
            {
                // std::cout << " overflow bucket: " << overflown_bucket_idx;
            }
            itr = reinterpret_cast<HashEntry*>(GetBucket(overflown_bucket_idx, mp.page));
            count = kNumEntryPerBucket-1;
        } else{
            itr++;
            count--;
        } 
    }    
    //itr points to the next empty Entry or the last Entry
    //if a deleted hole has been reserved for the entry
    if (reserved){
        reserved->IncChainLength();
        reserved->UnsetDeleted();
        reserved->SetAddress(log_addr);
        reserved->SetValidation(record, bucket_per_page_, de.local_depth);
        SetDirty(mp);
        return Status::SUCCESS;
    }

    //check if itr points to the last entry.
    if (itr->GetAddress()){
        Extend(record);
        wlatch.unlock();
        return Put(record, log_addr);
        // //log_address set to invalid
        // //signal that the page need to be extended
    } else {
        itr->IncChainLength();
        itr->SetTaken();
        itr->SetAddress(log_addr);
        itr->SetValidation(record, bucket_per_page_, de.local_depth);
        SetDirty(mp);
        return Status::SUCCESS; 
    }    
}

Status HashIndex::Upsert(const uint64_t record, uint64_t &log_addr){

    uint64_t idx = RefDirIdx(record);
    int overflow_debug_stopper = 0;
    {
        // std::cout << "\nhash value: " << record << " Directory index: " << idx << "\n";
    }
    DirEntry& de = entries_[idx];
    //assign the offset in file to a new directory entry
    if (de.offset==UINT64_MAX){
        de.offset = num_page_alloc_ed_;
        num_page_alloc_ed_++;
        de.page_ptr = LoadPage(de.offset*page_size_, idx);
        //initialize all values to zero.
        std::lock_guard<std::recursive_mutex> wlatch(de.page_ptr->page_mtx);
        std::memset(de.page_ptr->page, 0, page_size_);
        UpdatePageLocalDepth(de.page_ptr->page, de.local_depth);
        SetDirty(*(de.page_ptr));
    } else if (de.page_ptr == nullptr) de.page_ptr = LoadPage(de.offset*page_size_, idx);

    MappedPage & mp = *de.page_ptr;
    {
        // std::cout << "bucket idx: " << BucketIdx(record);
    }
    HashBucket * bucket = GetBucket(BucketIdx(record), mp.page);
    HashEntry * itr = reinterpret_cast<HashEntry*>(bucket);
    uint64_t validbits = GetRecordValidate(record, de.local_depth);
    int count = kNumEntryPerBucket -1; //track if it encounters an overflown bucket

    HashEntry * reserved = nullptr;
    ResetPageLru(mp);
    std::unique_lock<std::recursive_mutex> wlatch(mp.page_mtx);
    //last entry on a page will never be taken. 
    while(itr->CheckTaken()){
        //validation bit matched record inserted before
        if (itr->Validation(bucket_per_page_, de.local_depth) == validbits){
            // has been deleted
            if (itr->CheckDeleted()){
                itr->IncChainLength();
                itr->UnsetDeleted();
                itr->SetAddress(log_addr);
            } else {
                itr->IncChainLength(); //upsert normal
                uint64_t temp;
                temp = itr->GetAddress();    
                itr->SetAddress(log_addr);
                log_addr = temp;//return the previous value.
            }
            SetDirty(mp);
            return Status::SUCCESS;
        }
        //taken by other record but deleted
        if (itr->CheckDeleted() && (!reserved)){
            reserved = itr;
        }
        //if overflown bucket reached
        if(!count){
            uint64_t overflown_bucket_idx = itr->GetOverflownBucket(bucket_per_page_);
            if (!overflown_bucket_idx){
                //if there is overflown bucket that can be allocated.
                if (de.overflown_bucket_cur == bucket_per_page_) {
                    Extend(record);
                    wlatch.unlock();
                    return Upsert(record, log_addr);
                }
                {
                    // std::cout << " (new)";
                }
                itr->SetOverflownBucket(de.overflown_bucket_cur);
                overflown_bucket_idx = de.overflown_bucket_cur;
                de.overflown_bucket_cur++;
            }
            {
                // std::cout << " overflow bucket: " << overflown_bucket_idx;
                if(overflow_debug_stopper++>64) {
                    std::cerr << "kill too many overflown bucket\n"; 
                    exit(1);
                }
            }
            itr = reinterpret_cast<HashEntry*>(GetBucket(overflown_bucket_idx, mp.page));
            count = kNumEntryPerBucket-1;
        } else{
            itr++;
            count--;
        } 
    }

    //if (dec) return Status::FAILED; // no record found for this tombstone.    
    //itr points to the next empty Entry or the last Entry
    //if a deleted hole has been reserved for the entry
    if (reserved){
        reserved->IncChainLength();
        reserved->UnsetDeleted();
        reserved->SetAddress(log_addr);
        reserved->SetValidation(record, bucket_per_page_, de.local_depth);
        SetDirty(mp);
        return Status::SUCCESS;
    }

    //check if itr points to the last entry.
    if (itr->GetAddress()){
        Extend(record);
        wlatch.unlock();
        return Put(record, log_addr);// by this step we know the record hash_val has not inserted in this index, so safe to call put.
        // //log_address set to invalid
        // //signal that the page need to be extended
    } else {
        itr->IncChainLength();
        itr->SetTaken();
        itr->SetAddress(log_addr);
        itr->SetValidation(record, bucket_per_page_, de.local_depth);
        SetDirty(mp);
        return Status::SUCCESS; 
    }    
}

Status HashIndex::Delete(const uint64_t record, uint64_t &log_addr){
    uint64_t idx = RefDirIdx(record);
    DirEntry& de = entries_[idx];
    //page not even allocated
    //offset == UINT64_MAX only happen on the initial pages. 
    //extended pages will have offset linked to the previous page @initialization
    if (de.offset==UINT64_MAX) return Status::FAILED;
    
    if (de.page_ptr == nullptr) de.page_ptr = LoadPage(de.offset*page_size_, idx);
    MappedPage & mp = *de.page_ptr;
    HashBucket * bucket = GetBucket(BucketIdx(record), mp.page);
    HashEntry * itr = reinterpret_cast<HashEntry*>(bucket);
    uint64_t validbits = GetRecordValidate(record, de.local_depth);
    int count = kNumEntryPerBucket -1; //track if it encounters an overflown bucket
    
    ResetPageLru(mp);
    std::unique_lock<std::recursive_mutex> wlatch(mp.page_mtx);
    //last entry on a page will never be taken. 
    while(itr->CheckTaken()){
        //validation bit matched record inserted before
        if (itr->Validation(bucket_per_page_, de.local_depth) == validbits){
            // has been deleted
            if (itr->CheckDeleted()){
                return Status::FAILED;
            } else {
                //not deleted
                itr->DecChainLength();
                if (itr->EmptyChain()) itr->SetDeleted();
                log_addr = itr->GetAddress();
                SetDirty(mp);
                return Status::SUCCESS;    
            }
        }
        //if overflown bucket reached
        if(!count){
            uint64_t overflown_bucket_idx = itr->GetOverflownBucket(bucket_per_page_);
            //reached the hash bucket end. no more to check
            if (!overflown_bucket_idx) return Status::FAILED;
            itr = reinterpret_cast<HashEntry*>(GetBucket(overflown_bucket_idx, mp.page));
            count = kNumEntryPerBucket-1;
        } else {
            itr++;
            count--;
        }
    }    
    //all taken entries have been checked yet not found.
    return Status::FAILED;
}

Status HashIndex::Extend(const uint64_t record){
    
    // auto extend_start = std::chrono::high_resolution_clock::now();
    
    DirEntry& de = entries_[RefDirIdx(record)];
    {
        // std::cout << "\n extend page called! \n";
        // std::cout << "local depth: " << de.local_depth << " global depth: " << global_depth_ << '\n';
        // std::cout << "--- print table directory ---\n";
        // for (unsigned int i=0; i< (1ULL<<global_depth_); i++){
        //     std::cout << "directory idx: " << i << " local depth: " << entries_[i].local_depth << '\n';
        // }
    }
    //directory needs to be extended if necessary
    if(de.local_depth == global_depth_){
        {
            // std::cout << "directory extension!\n";
        }
        void * buff = malloc((1<<(global_depth_+1))*sizeof(DirEntry)); 
        std::memset(buff, 0, (1<<(global_depth_+1))*sizeof(DirEntry));
        if (!buff) {
            perror("alloc failed");
            return Status::FAILED;
        }
        DirEntry* entries_new = reinterpret_cast<DirEntry*>(buff); 
        //initialization: new entries only have their local_depth value updated to locate the DirEntry they refer to.
        //Only entries_[RefDirIdx(*)] are used in operations so avoided unecessary dependency updates.
        uint64_t prev_size = 1<<global_depth_;
        for (uint64_t i=0; i<prev_size; ++i){
            entries_new[i] = entries_[i];
            entries_new[i+prev_size].local_depth = entries_[i].local_depth;
        }
        //switch
        free(entries_);
        entries_=nullptr;
        entries_ = entries_new;
        global_depth_++;
        {
            // auto extend_dir_end = std::chrono::high_resolution_clock::now();
            // std::cout << "extend dir took" << std::chrono::duration_cast<std::chrono::nanoseconds>(extend_dir_end-extend_start).count() << "\n";
        }
    }
    //split page operation
    //dependent DirEntries update.
    uint64_t idx = RefDirIdx(record);
    DirEntry* source_de = &entries_[RefDirIdx(record)];
    //original local depth value
    uint64_t ld = source_de->local_depth;
    DirEntry* target_de = source_de + (1<<ld);
    //local depth update //all dir entries referencing this one will have their local_depth increment by 1.
    for (int i = 0; i <(1<<(global_depth_-ld)); i++){
        (source_de+(1<<ld)*i)->local_depth++;
    }
    //allocate new page to target_de
    target_de->offset = num_page_alloc_ed_;
    target_de->overflown_bucket_cur = bucket_per_page_/2;
    num_page_alloc_ed_++;
    SetDirty(*(source_de->page_ptr));
    target_de->page_ptr = LoadPage(target_de->offset*page_size_, idx+(1<<ld));
    void * target_page = target_de->page_ptr->page;
    std::lock_guard<std::recursive_mutex> wlatch_target(target_de->page_ptr->page_mtx);
    std::memset(target_page, 0, page_size_);
    UpdatePageLocalDepth(target_page, target_de->local_depth);
    SetDirty(*(target_de->page_ptr));

    // some preparation on source de and temp page
    source_de->overflown_bucket_cur = bucket_per_page_/2;
    void * source_page = source_de -> page_ptr -> page;
    void * source_page_new_copy = malloc(page_size_);
    if (source_page_new_copy==NULL){
        std::cerr << "malloc failed\n"; 
        exit(1);
    } 
    std::memset(source_page_new_copy, 0, page_size_);
    UpdatePageLocalDepth(source_page_new_copy, source_de->local_depth);
    for(int i=0; i<bucket_per_page_/2; i++){
        {
            // std::cout << "\nsplitting bucket: " << i;
        }
        //iterator pointing to the hash entry to process in source page;
        HashEntry * itr_r = reinterpret_cast<HashEntry*>(reinterpret_cast<HashBucket*>(source_page)+i);
        //iterator pointing to the next hash entry to write in source page temporary copy;
        HashEntry * itr_ws = reinterpret_cast<HashEntry*>(reinterpret_cast<HashBucket*>(source_page_new_copy)+i);
        //iterator pointing to the next hash entry to write in target page
        HashEntry * itr_wt = reinterpret_cast<HashEntry*>(reinterpret_cast<HashBucket*>(target_page)+i);
        int count_r = kNumEntryPerBucket-1;
        int count_ws = kNumEntryPerBucket-1;
        int count_wt = kNumEntryPerBucket-1;
        while (itr_r->CheckTaken()){
            if(!itr_r->CheckDeleted()){
                if (itr_r->GetHashVal()&(bucket_per_page_/2<<ld)){//check the bit that is now used to redistribute the HashEntry
                    *(itr_wt) = *(itr_r);
                    //unset the overflown bucket from the old entry
                    if(!count_r){
                        uint64_t overflown_bucket_idx = itr_r->GetOverflownBucket(bucket_per_page_);
                        if (!overflown_bucket_idx){ //this 
                            {
                                // std::cout << "\n source pointer: no more overflown bucket\n";
                            }
                            break; 
                        } else {
                            {
                                // std::cout << "\n source pointer to overflown bucket: "<< overflown_bucket_idx << "\n";
                            }
                            itr_r = reinterpret_cast<HashEntry*>(GetBucket(overflown_bucket_idx, source_page));
                            count_r = kNumEntryPerBucket-1;
                        } 
                        itr_wt->ResetOverflownBucket(bucket_per_page_);
                    } else{
                        itr_r++;
                        count_r--;
                    }
                    //need to take an overflown bucket.
                    if(!count_wt){
                        itr_wt->SetOverflownBucket(target_de->overflown_bucket_cur);
                        {
                            // std::cout << "\n new target pointer to: " << target_de -> overflown_bucket_cur;
                        }
                        itr_wt = reinterpret_cast<HashEntry*>(GetBucket(target_de->overflown_bucket_cur, target_page));
                        target_de->overflown_bucket_cur++;
                        count_wt = kNumEntryPerBucket-1;
                    } else{
                        itr_wt++;
                        count_wt--;
                    } 
                } else {
                    *(itr_ws) = *(itr_r);
                    //unset the overflown bucket from the old entry
                    if(!count_r){
                        uint64_t overflown_bucket_idx = itr_r->GetOverflownBucket(bucket_per_page_);
                        if (!overflown_bucket_idx){ //this 
                            {
                                // std::cout << "\n source pointer: no more overflown bucket\n";
                            }
                            break; 
                        } else {
                            {
                                // std::cout << "\n source pointer to overflown bucket: "<< overflown_bucket_idx << "\n";
                            }
                            itr_r = reinterpret_cast<HashEntry*>(GetBucket(overflown_bucket_idx, source_page));
                            count_r = kNumEntryPerBucket-1;
                        } 
                        itr_ws->ResetOverflownBucket(bucket_per_page_);
                    } else{
                        itr_r++;
                        count_r--;
                    }
                    //need to take an overflown bucket.
                    if(!count_ws){
                        itr_ws->SetOverflownBucket(source_de->overflown_bucket_cur);
                        {
                            // std::cout << "\n new source pointer to: " << source_de -> overflown_bucket_cur; 
                        }
                        itr_ws = reinterpret_cast<HashEntry*>(GetBucket(source_de->overflown_bucket_cur, source_page_new_copy));
                        source_de->overflown_bucket_cur++;
                        count_ws = kNumEntryPerBucket-1;
                    } else{
                        itr_ws++;
                        count_ws--;
                    } 
                }
            } else {
                if(!count_r){
                    uint64_t overflown_bucket_idx = itr_r->GetOverflownBucket(bucket_per_page_);
                    if (!overflown_bucket_idx){ //this 
                        {
                            // std::cout << "\n source pointer: no more overflown bucket\n";
                        }
                        break; 
                    } else {
                        {
                            // std::cout << "\n source pointer to overflown bucket: "<< overflown_bucket_idx << "\n";
                        }
                        itr_r = reinterpret_cast<HashEntry*>(GetBucket(overflown_bucket_idx, source_page));
                        count_r = kNumEntryPerBucket-1;
                    } 
                } else{
                    itr_r++;
                    count_r--;
                }
            }
        }

    }
    std::memcpy(source_page, source_page_new_copy, page_size_);
    free(source_page_new_copy);
    source_page_new_copy = nullptr;
    {
        // std::cout << "\n--- print new table directory ---\n";
        // for (unsigned int i=0; i< (1ULL<<global_depth_); i++){
        //     std::cout << "directory idx: " << i << " local depth: " << entries_[i].local_depth << '\n';
        // }
        // auto extend_end = std::chrono::high_resolution_clock::now();
        // std::cout << "extend took" << std::chrono::duration_cast<std::chrono::nanoseconds>(extend_end-extend_start).count() << "\n";
    }
    return Status::SUCCESS;
}

Status HashIndex::Checkpoint(){
    //flush all pages in Mapped pages
    for (unsigned int i = 0; i < kNumInMemPage; i++){
        if (im_pages_[i].dirtybit){
            std::lock_guard<std::recursive_mutex> wlatch(im_pages_[i].page_mtx);
            file_->FlushPage(page_size_, im_pages_[i].page);
            im_pages_[i].dirtybit = 0;
        }
    }

    void * first_page = nullptr; 
    Status load_page_status;
    const unsigned int meta_pages = kMaxIndexPage-kNumInMemPage; 
    first_page = file_->LoadPage(meta_pages*page_size_, 0, first_page, load_page_status);
    if(load_page_status==Status::FAILED) return Status::FAILED;
    std::memset(first_page, 0, meta_pages*page_size_);
    uint8_t * ptr = reinterpret_cast<uint8_t*>(first_page);
    //write metadata out
    memcpy(ptr, &page_size_, sizeof(page_size_));
    ptr += sizeof(page_size_);
    memcpy(ptr, &num_page_alloc_ed_, sizeof(num_page_alloc_ed_));
    ptr += sizeof(num_page_alloc_ed_);
    //write directory into file
    memcpy(ptr, &global_depth_, sizeof(global_depth_));
    ptr += sizeof(global_depth_);
    memcpy(ptr, entries_, (1<<global_depth_)*sizeof(DirEntry));
    file_->FlushPage(meta_pages*page_size_, first_page);
    file_->EvictPage(meta_pages*page_size_, first_page);
    first_page = nullptr;
    return Status::SUCCESS;
}

void HashIndex::Close(){
    {
        PrintDirectoryInfo();
    }
    if(entries_!=nullptr) free(entries_);
    entries_=nullptr;
    for (unsigned int i = 0; i < kNumInMemPage; i++){
        if (im_pages_[i].page != nullptr){
            if (im_pages_[i].dirtybit){
                file_->FlushPage(page_size_, im_pages_[i].page);
                im_pages_[i].dirtybit = 0;
            }
            file_->EvictPage(page_size_, im_pages_[i].page);
        }
    }
}

void HashIndex::PrintDirectoryInfo(){
    std::cout << "--- print index directory ---\n";
    for (unsigned int i=0; i< (1ULL<<global_depth_); i++){
        std::cout << "directory idx: " << i << " local depth: " << entries_[i].local_depth << '\n';
    }
}