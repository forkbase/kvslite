#ifndef KV_FILELINUX_H_
#define KV_FILELINUX_H_

#include <fcntl.h>
#include <unistd.h> 
#include <cstdio>
#include <cinttypes>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h> 
#include "file.h"

class File_linux:public File{
public:
    File_linux(){}

    inline Status Open(const char * file_path){
        if ((fd_ = open(file_path, O_RDWR|O_CREAT, 0644))==-1){
            perror("open error");
            return Status::FAILED;
        } else return Status::SUCCESS;
    }

    // buff needs to be allocated to a page aligned address, as MAP_FIXED is used for mmap.
    void * LoadPage(const uint64_t size, const uint64_t offset, void* buff, Status &status){
        struct stat sb;
        if (fstat(fd_, &sb)==-1) {
            perror("fstat error");
            status = Status::FAILED;
            return nullptr;
        } 
        
        //offset must be page aligned. moved to the nearest boundary in front. 
        uint64_t offset_align = offset & ~(sysconf(_SC_PAGE_SIZE)-1);
        //calibrate size to multiple of page size
        //first calculate the new size to load after left shift offset to a page boundary
        //then extend the size to multiple of page size
        uint64_t size_cali = ((size + offset - offset_align -1) | (sysconf(_SC_PAGE_SIZE) -1)) +1;
        //extend the file, if file size is too small
        if ((offset_align+size_cali) > (uintmax_t) sb.st_size) {
            // printf("file extend to %lu\n", (offset_align+size_cali));   
            if (ftruncate(fd_, (offset_align+size_cali))==-1){perror("extension failed");}
        }
        
        //Fix the map at buff with MAP_FIXED, MAP_SHARED needed for msync in FlushPage().
        if (buff == nullptr){
            buff = mmap(NULL, size_cali, PROT_READ|PROT_WRITE, MAP_SHARED, fd_, offset_align);
        } 
        else buff=mmap(buff, size_cali, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_FIXED, fd_, offset_align);
        if (buff==(void *)-1){
            perror("mmap error");
            status = Status::FAILED;
            return nullptr;
        } else {
            status = Status::SUCCESS;
            return buff;
        } 
    }

    Status FlushPage(const uint64_t size, void * buff){
        if (size==0) return Status::SUCCESS;
        //size is calibrated to the next multiple of page size.
        uint64_t size_cali = ((size-1) | (sysconf(_SC_PAGE_SIZE)-1))+1;
        if (msync(buff, size_cali, MS_SYNC)==-1){
            perror("msync error");
            return Status::FAILED;
        } else return Status::SUCCESS;
    }

    Status EvictPage(const uint64_t size, void * buff){
        if (buff==nullptr) {
            perror("null pointer in!");
            return Status::FAILED;
        }
        if (munmap(buff, size)==-1){
            perror("munmap error");
            return Status::FAILED;
        } else return Status::SUCCESS;
    }

    Status Read(const uint64_t size, const uint64_t offset, void * buff){
        ssize_t read_count = pread(fd_, buff, size, offset); 
        if (read_count==-1){
            perror("read failed");
            return Status::FAILED;
        } else if((uintmax_t) read_count != size){
            printf("%" PRIu64 " requested, %" PRIu64 " read.\n", size, read_count);
        }
        return Status::SUCCESS;        
    }

    Status Write(const uint64_t size, const uint64_t offset, const void * buff){
        ssize_t write_count = pwrite(fd_, buff, size, offset); 
        if (write_count==-1){
            perror("write failed");
            return Status::FAILED;
        } else if((uintmax_t) write_count != size){
            printf("%" PRIu64 "requested, %" PRIu64 " written.\n", size, write_count);
        }
        return Status::SUCCESS;        
    }

    Status Close(){
        if (close(fd_)==-1){
            perror("close error");
            return Status::FAILED;
        } else return Status::SUCCESS;
    }

    const uint64_t GetPageSize(){
        return page_size_;
    }

    int GetFd(){
        return fd_;
    }

private:
    int fd_;
    int fdtemp;
    const uint64_t page_size_ = sysconf(_SC_PAGE_SIZE);
};

#endif
