#pragma once

using ProcessID = unsigned long int;
using ThreadID = unsigned long int;
using TimeResolution = unsigned long long int;
using EventType = char*;
using ConstEventType = const char*;

namespace ygm::detail::tracing {

    inline TimeResolution get_time(){
        struct timeval tv {};
        gettimeofday(&tv, NULL);
        TimeResolution t = 1000000 * tv.tv_sec + tv.tv_usec;
        return t;
    }

    void convert_json(ConstEventType event_name, ConstEventType category,
        TimeResolution start_time, TimeResolution duration,
        std::unordered_map<std::string, boost::any> *metadata, ProcessID process_id,
        ThreadID thread_id, int *size, char* data){


        // TODO: understand how to utilized these
        std::atomic_int index;
        std::string is_first_char = ""; 

        *size = snprintf(
            data, MAX_LINE_SIZE,
            "%s{\"id\":\"%d\",\"name\":\"%s\",\"cat\":\"%s\",\"pid\":\"%lu\","
            "\"tid\":\"%lu\",\"ts\":\"%llu\",\"dur\":\"%llu\",\"ph\":\"X\","
            "\"args\":{}}\n",
            is_first_char.c_str(), index.load(), event_name, category, process_id,
            thread_id, start_time, duration);

    }

    

}