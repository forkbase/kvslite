#include <cstdint>
#include <cstring>
#include <condition_variable>
#include <mutex>
#include "log_storage.h"
#include "file_linux.h"
#include "key_value.h"
#include "mapped_pages_ro.h"

// for debug logging
#include <chrono>
#include <iostream>
#include <thread>

LogStorage* LogStorage::Initialize(){
    log_head_addr_=page_size_; //head of in memory log.
    log_end_addr_=page_size_; //the front to insert (address of the byte after the last element)
    last_flush_request_until_addr_=page_size_;
    Status load_return_status;
    buffer_ = file_->LoadPage(page_size_*kNumBufPage, page_size_, buffer_, load_return_status);
    if (load_return_status == Status::FAILED) {return nullptr;}
    std::memset(buffer_, 0, page_size_*kNumBufPage);
    return this;        
}

LogStorage* LogStorage::LoadFromFile(File* file_in){
    uint64_t temp;
    uint64_t offset=0;
    file_in->Read(sizeof(page_size_), offset, &temp);
    if (temp!=page_size_){
        printf("Loaded log has different page_size");
    }
    offset+=sizeof(page_size_);
    file_in->Read(sizeof(log_end_addr_), offset, &log_end_addr_);    
    uint64_t load_offset = page_size_;        
    if (load_offset+page_size_*kNumBufPage <= log_end_addr_+page_size_){
        load_offset = NextBoundary(log_end_addr_) - page_size_*kNumBufPage + page_size_; 
    }
    log_head_addr_ = load_offset;
    log_persist_page_ = (log_end_addr_ - log_head_addr_)/page_size_;// need to explicit set otherwise the pending request does not align
    last_flush_request_until_addr_ = log_end_addr_- (log_end_addr_%page_size_);

    Status load_return_status; 
    buffer_ = file_->LoadPage(page_size_*kNumBufPage, load_offset, buffer_, load_return_status);
    // std::cout<< "log_head_address " << log_head_addr_ << " log_end_addr_" << log_end_addr_ << " log_persis_addr " << last_flush_request_until_addr_ 
    //     << " log_perisit_page " << log_persist_page_ << "\n";
    if (load_return_status == Status::FAILED) return nullptr;
    return this;        
}

Status LogStorage::FlushPages(std::unique_lock<std::mutex>&& latch){
    {
        // std::cout << std::this_thread::get_id() << " entered\n";

    }
    if (flush_pending_count==0){
        // the last page shall not be flushed by flushTheLastPage function but only checkpoint. otherwise too much contention to write.
        return Status::FAILED; 
        //no flush request. background thread can do other things 
    } 
    if (flush_in_progress) {
        // std::cout << "someone in!!!";
        return Status::SUCCESS;
    } else flush_in_progress = true; 
    { //for thread debug printing
        // uint64_t persist_addr = (log_persist_page_ + kNumBufPage - log_head_page_)%kNumBufPage*page_size_ + log_head_addr_;
        // std::cout << "\n" << std::this_thread::get_id() << "in, flushed " <<  persist_addr << " of pending no. " << flush_pending_count << "\n";
    }
    unsigned int batch_factor = (flush_pending_count>(kNumBufPage-log_persist_page_)) ? (kNumBufPage-log_persist_page_) : flush_pending_count;
    flush_pending_count-=batch_factor;
    latch.unlock();
    file_->FlushPage(page_size_*batch_factor, GetPage(log_persist_page_));
    // std::cout << "\n" << std::this_thread::get_id() << "flush done ("<<batch_factor<<")!\n";
    log_persist_page_ = (log_persist_page_ + batch_factor) % kNumBufPage;
    flush_in_progress = false;
    // std::cout << "\n" << std::this_thread::get_id() << "out!"<<flush_in_progress<<"\n";
    return Status::SUCCESS;
}

unsigned int LogStorage::EvictPage(unsigned int num_page){
    if (num_page<1) {
        printf("less than one page to evict\n");
        return 0;
    }
    //num_page is bounded by the total number of mapped pages
    if ((log_end_addr_-log_head_addr_)<num_page*page_size_){
        // at here the flush_pending count is definitely 0. so no contention with background thread.
        // so update log_persist_page without log.
        printf("buffer cleared\n");
        // manually flush the last page that was not flushed
        file_->FlushPage(page_size_, GetPage(BufPageIdx(log_end_addr_)));
        file_->EvictPage(kNumBufPage, buffer_); 
        log_end_addr_ = NextBoundary(log_end_addr_);
        log_head_addr_ = log_end_addr_;
        last_flush_request_until_addr_ = log_end_addr_;
        log_head_page_ = 0;
        log_persist_page_ = 0;
    } else {
        if (num_page + log_head_page_ > kNumBufPage){
            unsigned int num_page_addi = (num_page + log_head_page_)-kNumBufPage;
            file_->EvictPage((num_page-num_page_addi)*page_size_, GetPage(log_head_page_));
            file_->EvictPage(num_page_addi*page_size_, buffer_);
        } else file_->EvictPage(num_page*page_size_, GetPage(log_head_page_));    
        log_head_addr_ +=(num_page)*page_size_;
        log_head_page_ = (log_head_page_ + num_page)%kNumBufPage;
    } 
    return num_page;
}