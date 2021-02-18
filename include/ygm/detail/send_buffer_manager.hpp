#pragma once

#include <vector>
#include <ygm/utility.hpp>

namespace ygm {

struct CommLockBankTag {};

class locking_send_buffer_manager {
 public:
  using lock_bank = ygm::lock_bank<1024, CommLockBankTag>;

  locking_send_buffer_manager(){};

  locking_send_buffer_manager &operator=(locking_send_buffer_manager &&man) {
    m_num_buffers = man.m_num_buffers;
    m_buffer_capacity = man.m_buffer_capacity;
    m_comm_ptr = man.m_comm_ptr;
    m_vec_send_buffers = std::move(man.m_vec_send_buffers);

    return *this;
  }

  locking_send_buffer_manager(
      const int num_buffers, const size_t buffer_capacity,
      ygm::comm<locking_send_buffer_manager>::impl *comm_ptr)
      : m_num_buffers(num_buffers),
        m_buffer_capacity(buffer_capacity),
        m_comm_ptr(comm_ptr) {
    for (int i = 0; i < m_num_buffers; ++i) {
      m_vec_send_buffers.push_back(allocate_buffer());
    }
  }

  void insert(const int dest, const std::vector<char> &packed_msg) {
    auto l = lock_bank::mutex_lock(dest);
    // check if buffer doesn't have enough space
    if (packed_msg.size() + m_vec_send_buffers[dest]->size() >
        m_buffer_capacity) {
      flush_buffer(dest);
    }

    // add data to the to dest buffer
    m_vec_send_buffers[dest]->insert(m_vec_send_buffers[dest]->end(),
                                     packed_msg.begin(), packed_msg.end());
  }

  void all_flush() {
    for (int i = 0; i < m_num_buffers; ++i) { flush_buffer(i); }
    return;
  }

 private:
  /**
   * @brief Allocates buffer; checks free pool first.
   *
   * @return std::shared_ptr<std::vector<char>>
   */
  std::shared_ptr<std::vector<char>> allocate_buffer() {
    std::scoped_lock lock(m_vec_free_buffers_mutex);
    if (m_vec_free_buffers.empty()) {
      auto to_return = std::make_shared<std::vector<char>>();
      to_return->reserve(m_buffer_capacity);
      return to_return;
    } else {
      auto to_return = m_vec_free_buffers.back();
      m_vec_free_buffers.pop_back();
      return to_return;
    }
  }

  /**
   * @brief Frees a previously allocated buffer.  Adds buffer to free pool.
   *
   * @param b buffer to free
   */
  void free_buffer(std::shared_ptr<std::vector<char>> b) {
    b->clear();
    std::scoped_lock lock(m_vec_free_buffers_mutex);
    m_vec_free_buffers.push_back(b);
  }

  /**
   * @brief Safely flush a single buffer
   *
   * @param dest Index of buffer to send
   */
  void flush_buffer(const int dest) {
    // Currently locking before calling async_flush, so no locking here
    if (m_vec_send_buffers[dest]->size() == 0) return;
    auto buffer = allocate_buffer();
    std::swap(buffer, m_vec_send_buffers[dest]);
    m_comm_ptr->async_send(dest, buffer->size(), buffer->data());
    free_buffer(buffer);
  }
  int m_num_buffers;
  int m_buffer_capacity;

  std::vector<std::shared_ptr<std::vector<char>>> m_vec_send_buffers;

  std::mutex m_vec_free_buffers_mutex;
  std::vector<std::shared_ptr<std::vector<char>>> m_vec_free_buffers;

  comm<locking_send_buffer_manager>::impl *m_comm_ptr;
};
}  // namespace ygm
