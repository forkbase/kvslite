#include <fstream>
#include <string>
#include "system_catelog.h"
    
SystemCatelog::SystemCatelog(const std::string& system_catelog_path, const int newbit){
    if (!newbit){
        file_.open(system_catelog_path+"sc.txt", std::ios::in);
        std::getline(file_, index_file_path_);
        std::getline(file_, data_file_path_);
        file_.close();
    } else {
        file_.open(system_catelog_path+"sc.txt", std::ios::out | std::ios::trunc);
        std::fstream temp;
        index_file_path_ = system_catelog_path+"index.BIN";
        temp.open(index_file_path_, std::ios::out | std::ios::trunc);
        temp.close();
        file_<<index_file_path_<<"\n";

        data_file_path_ = system_catelog_path+"data.BIN";
        temp.open(data_file_path_, std::ios::out | std::ios::trunc);
        temp.close();
        file_<<data_file_path_<<"\n";

        file_<<"checkpoint000\n";
        file_.close();
    }
}