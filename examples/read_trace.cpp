#include <iostream>
#include <variant>
#include <string>
#include <sstream>
#include <fstream>
#include <cereal/archives/binary.hpp>
#include <cereal/types/variant.hpp>
#include <cereal/types/string.hpp>

struct YGMEvent {
    uint64_t event_id;
    int from;
    int to;
    uint32_t message_size;
    char type;
    char action;

    template <class Archive>
    void serialize(Archive & ar) {
        ar(event_id, from, to, message_size, type, action);
    }
};

struct YGMBarrierEvent {
    uint64_t event_id;
    int rank;
    uint64_t send_count;
    uint64_t recv_count;
    size_t pending_isend_bytes;
    size_t send_buffer_bytes;

    template <class Archive>
    void serialize(Archive & ar) {
        ar(event_id, rank, send_count, recv_count, pending_isend_bytes, send_buffer_bytes);
    }
};

struct VariantEvent {
    std::variant<YGMEvent, YGMBarrierEvent> data {};
    template< class Archive >
    void serialize( Archive & archive ) {
        archive( data );
    }
};

// Function to deserialize the object from a file and print it
void deserializeFromFile(const std::string& filename) {
    std::ifstream is(filename, std::ios::binary);
    if (!is) {
        std::cerr << "Failed to open file for reading: " << filename << std::endl;
        return;
    }

    cereal::BinaryInputArchive iarchive(is);
    VariantEvent variant_event {};
    iarchive(variant_event);

    // Use std::visit to determine the type and print it
    std::visit([](auto&& arg) {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, YGMEvent>) {
            std::cout << "YGMEvent - Event ID: " << arg.event_id << ", From: " << arg.from 
                      << ", To: " << arg.to << ", Message Size: " << arg.message_size 
                      << ", Type: " << arg.type << ", Action: " << arg.action << std::endl;
        } else if constexpr (std::is_same_v<T, YGMBarrierEvent>) {
            std::cout << "YGMBarrierEvent - Event ID: " << arg.event_id << ", Rank: " << arg.rank 
                      << ", Send Count: " << arg.send_count << ", Recv Count: " << arg.recv_count 
                      << ", Pending ISend Bytes: " << arg.pending_isend_bytes 
                      << ", Send Buffer Bytes: " << arg.send_buffer_bytes << std::endl;
        }
    }, variant_event.data);
}

int main() {
    std::string filename = "trace/trace_0.bin";

    // Deserialize and print the object from the file
    deserializeFromFile(filename);

    return 0;
}
