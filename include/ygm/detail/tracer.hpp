#pragma once

#include <iostream>
#include <fstream>
#include <unordered_map>
#include <unistd.h>
#include <syscall.h>
#include <sys/time.h>
#include <atomic>

#include <any>

using ProcessID = unsigned long int;
using ThreadID = unsigned long int;
using TimeResolution = unsigned long long int;
using EventType = char*;
using ConstEventType = const char*;

namespace ygm::detail{


class tracer {
    public:
        
        tracer() {}

        ~tracer() {
            size = snprintf(data, MAX_LINE_SIZE, "]");
            output_file.write(data, size); 

            output_file.close();
            if (output_file.fail()) {
                std::cerr << "Error writing to tracing file!" << std::endl;
                // Handle failure to close the file
            }
        }

        inline TimeResolution get_time(){
            struct timeval tv {};
            gettimeofday(&tv, NULL);
            TimeResolution t = 1000000 * tv.tv_sec + tv.tv_usec;
            return t;
        }

        void trace_event(TimeResolution start_time, TimeResolution duration){    

            if (!output_file.is_open()) {
                open_file();
            }

            ProcessID pid = syscall(SYS_getpid);
            
            ThreadID tid = syscall(SYS_gettid);
            
            ConstEventType event_name = "async";
            ConstEventType category = "ygm";
            std::unordered_map<std::string, std::any> metadata; 

            // convert to json formating
            convert_json(event_name, category, start_time,
                duration, &metadata, pid, tid, &size, data);

            output_file.write(data, size);
            
        }

    private: 
        std::ofstream output_file;

        static const int MAX_LINE_SIZE = 4096;
        static const int MAX_META_LINE_SIZE = 3000;

        char data [MAX_LINE_SIZE];
        int size = 0;

        void convert_json(ConstEventType event_name, ConstEventType category,
        TimeResolution start_time, TimeResolution duration,
        std::unordered_map<std::string, std::any> *metadata, ProcessID process_id,
        ThreadID thread_id, int *size, char* data){

        std::atomic_int index;
        std::string is_first_char = ""; // Initialize based on index value

        *size = snprintf(
            data, MAX_LINE_SIZE,
            "%s{\"id\":\"%d\",\"name\":\"%s\",\"cat\":\"%s\",\"pid\":\"%lu\","
            "\"tid\":\"%lu\",\"ts\":\"%llu\",\"dur\":\"%llu\",\"ph\":\"X\","
            "\"args\":{}}\n",
            is_first_char.c_str(), index.load(), event_name, category, process_id,
            thread_id, start_time, duration);

        }

        void open_file() {
            output_file.open("output.txt");

            if (!output_file.is_open()) {
                std::cerr << "Error opening tracing file for writing!" << std::endl;
                // Handle failure to open the file
            }

            size = snprintf(data, MAX_LINE_SIZE, "[\n");
            output_file.write(data, size);
        }

};
}