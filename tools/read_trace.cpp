#include <cereal/archives/binary.hpp>
#include <cereal/types/string.hpp>
#include <cereal/types/variant.hpp>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <variant>
#include <ygm/detail/tracer.hpp>

using namespace ygm::detail;

void deserializeFromFile(const std::string& filename) {
  std::ifstream is(filename, std::ios::binary);
  if (!is) {
    std::cerr << "Failed to open file for reading: " << filename << std::endl;
    return;
  }

  cereal::BinaryInputArchive iarchive(is);
  variant_event              variant_event{};

  // Loop to deserialize and print events until the end of the file
  while (is.peek() != EOF) {
    iarchive(variant_event);

    std::visit(
        [](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, ygm_async_event>) {
            std::cout << "YGM Async Event - Event ID: " << arg.event_id
                      << ", To: " << arg.to
                      << ", Message Size: " << arg.message_size << std::endl;
          } else if constexpr (std::is_same_v<T, mpi_send_event>) {
            std::cout << "MPI Send Event - Event ID: " << arg.event_id
                      << ", To: " << arg.to
                      << ", Buffer Size: " << arg.buffer_size << std::endl;
          } else if constexpr (std::is_same_v<T, mpi_recv_event>) {
            std::cout << "MPI Receive Event - Event ID: " << arg.event_id
                      << ", From: " << arg.from
                      << ", Buffer Size: " << arg.buffer_size << std::endl;
          } else if constexpr (std::is_same_v<T, barrier_begin_event>) {
            std::cout << "Barrier Begin Event - Event ID: " << arg.event_id
                      << ", Send Count: " << arg.send_count
                      << ", Recv Count: " << arg.recv_count
                      << ", Pending ISend Bytes: " << arg.pending_isend_bytes
                      << ", Send Local Buffer Bytes: "
                      << arg.send_local_buffer_bytes
                      << ", Send Remote Buffer Bytes: "
                      << arg.send_remote_buffer_bytes << std::endl;
          } else if constexpr (std::is_same_v<T, barrier_end_event>) {
            std::cout << "Barrier End Event - Event ID: " << arg.event_id
                      << ", Send Count: " << arg.send_count
                      << ", Recv Count: " << arg.recv_count
                      << ", Pending ISend Bytes: " << arg.pending_isend_bytes
                      << ", Send Local Buffer Bytes: "
                      << arg.send_local_buffer_bytes
                      << ", Send Remote Buffer Bytes: "
                      << arg.send_remote_buffer_bytes << std::endl;
          }
        },
        variant_event.data);
  }
}

int main() {
  std::string filename = "trace/trace_0.bin";

  // Deserialize and print the object from the file
  deserializeFromFile(filename);

  return 0;
}
