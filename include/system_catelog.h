#ifndef KV_SYSTEMCATELOG_H_
#define KV_SYSTEMCATELOG_H_

#include <fstream>
#include <string>
#include "status.h"

// the SystemCatelog class opens the system catelog file.
// it stores the path of index file and data file.
// and necessary system information for recovery or restart.
class SystemCatelog{
private:
    uint64_t kBlockSize = 64;

public:
    SystemCatelog(){}

    // set up the file reference for kv (load old one, if exists)
    SystemCatelog(const std::string& system_catelog_path, const int newbit);

    inline std::string GetdataFilePath(){
        return data_file_path_;
    }

    inline std::string GetIndexFilePath(){
        return index_file_path_;
    }

    
private:
    std::fstream file_;
    std::string index_file_path_;
    std::string data_file_path_;
    // std::string checkpoint_info_; //TODO 
};

#endif