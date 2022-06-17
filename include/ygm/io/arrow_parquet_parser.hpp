// Copyright 2019-2022 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <filesystem>
#include <cassert>
#include <regex>
#include <string>
#include <vector>

#include <ygm/comm.hpp>
#include <ygm/detail/ygm_ptr.hpp>

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <arrow/util/logging.h>

#include <parquet/api/reader.h>
#include <parquet/exception.h>
#include <parquet/metadata.h>
#include <parquet/stream_reader.h>
#include <parquet/types.h>

namespace stdfs = std::filesystem;

namespace ygm::io {

//template <typename T>
//using parquet_optional = parquet::StreamReader::optional<T>;

using file_schema_container = std::vector<std::tuple<std::string, std::string>>;

class arrow_parquet_parser {

 public :
   using self_type = arrow_parquet_parser;

   arrow_parquet_parser(ygm::comm& _comm) :
     m_comm(_comm), pthis(this) { 
     pthis.check(m_comm);
   } 

   arrow_parquet_parser(ygm::comm& _comm, const std::string& _dir_name) :
     m_comm(_comm), pthis(this) {
     pthis.check(m_comm);
     build_file_list(_dir_name);
     read_file_schema();
     m_comm.barrier();      
   } 

   ~arrow_parquet_parser() {
     m_comm.barrier();
   }

   void build_file_list(const std::string& dir_name) {
     stdfs::path dir_path = dir_name;
     std::string wildcard = dir_path.filename().string();
     dir_path.remove_filename();

     const std::regex filename_filter(wildcard + ".*\\.parquet");

     if (!stdfs::exists(dir_path) ||
       !stdfs::is_directory(dir_path)) {
       std::cerr << "Error: Invalid directory path." << std::endl;       
     } else {
       std::smatch what;
       stdfs::directory_iterator end_itr;
  
       for (stdfs::directory_iterator itr(dir_path); itr != end_itr; ++itr) {
         auto filename = itr->path().filename().string();
         if (!stdfs::is_regular_file(itr->status())) {
           continue;
         } else if (!std::regex_match(filename, what, filename_filter)) {
           continue;
         } else {
           m_paths.emplace_back(itr->path()); 
         }
       } // for 
     }
   }

   void read_file_schema() {
     parquet_stream_reader(m_paths[0], 
       [](auto& stream_reader, const auto& field_count){}, true);   
   }

   const file_schema_container& schema() {
     return m_schema; 
   }

   const std::string& schame_to_string() {
     return m_schema_string;  
   }

   void check_file_schema(const parquet::SchemaDescriptor* file_schema) {
     // check the number of fields
     auto file_schema_group_node = file_schema->group_node();
     size_t field_count = file_schema_group_node->field_count();
     assert(field_count == m_schema.size());       
   }

   template <typename Function>
   void read_file(const stdfs::path& file_path, Function fn) {
     parquet_stream_reader(file_path.string(), fn);
   }

   template <typename Function>
   void read_files(Function fn) {
     for (size_t i = 0; i < m_paths.size(); ++i) {
       if (is_owner(i)) {
         read_file(m_paths[i], fn);    
       }
     } // for
   } 

   size_t file_count() {
     return m_paths.size();
   }

   bool is_owner(const size_t& item_ID) {
     return m_comm.rank() == item_ID % m_comm.size() ? true : false; 
   }

   template <typename Function>
   void for_all(Function fn) {
     read_files(fn);
   }

   template <typename Function>
   void parquet_stream_reader(std::string input_filename, Function fn, 
     bool read_schema_only = false) {
     std::shared_ptr<arrow::io::ReadableFile> input_file;
     PARQUET_ASSIGN_OR_THROW(
       input_file, arrow::io::ReadableFile::Open(input_filename));
     std::unique_ptr<parquet::ParquetFileReader> parquet_file_reader =
       parquet::ParquetFileReader::OpenFile(input_filename);
     parquet::StreamReader stream_reader{
       parquet::ParquetFileReader::Open(input_file)};

     std::shared_ptr<parquet::FileMetaData> file_metadata = 
       parquet_file_reader->metadata();
     auto file_schema = file_metadata->schema(); // SchemaDescriptor

     size_t field_count = 0;

     if (read_schema_only) {
       auto file_schema_group_node = file_schema->group_node();    
       size_t field_count = file_schema_group_node->field_count();
       for (int i = 0; i < file_schema_group_node->field_count(); ++i) {
         auto node_ptr = file_schema_group_node->field(i);
         m_schema.emplace_back(std::forward_as_tuple(
           node_ptr->logical_type()->ToString(), node_ptr->name()));         
       } // for
     } else {
       check_file_schema(file_schema);  
       field_count = m_schema.size();  
     } 

     if (read_schema_only) {
       m_schema_string = file_schema->ToString();
       return;
     } 

     //auto& file_key_value_metadata = file_metadata->key_value_metadata(); 

     for (size_t i = 0; !stream_reader.eof(); ++i) {
       fn(stream_reader, field_count);
     } // for 
   }
 
 private :
   ygm::comm m_comm;
   typename ygm::ygm_ptr<self_type> pthis;   
   std::vector<stdfs::path> m_paths;
   file_schema_container m_schema;
   std::string m_schema_string;
}; 

}  // namespace ygm::io
