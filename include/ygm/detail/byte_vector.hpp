#pragma once

#include <cstring>
#include <string>
#include <cstddef>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

namespace byte {
class byte_vector {  
  using value_type        = std::byte;
  using pointer           = std::byte*;
  using reference         = std::byte&;
  using difference_type   = std::ptrdiff_t;
public:
  class Byte_Iterator {
  public:

    Byte_Iterator()                      : m_bv(nullptr), i(0) {}
    Byte_Iterator(byte_vector* v, int i) : m_bv(v), i(i) {}
    
    reference       operator*()             { return (*m_bv)[i]; }
    const reference operator*()       const { return (*m_bv)[i]; }
    pointer         operator->()            { return &(*m_bv)[i]; }
    const pointer   operator->()      const { return &(*m_bv)[i]; }
    reference       operator[](int n)       { return (*m_bv)[i + n]; }
    const reference operator[](int n) const { return (*m_bv)[i + n]; }

    Byte_Iterator&  operator++() { ++i; return *this; }
    Byte_Iterator&  operator--() { --i; return *this; }
    Byte_Iterator   operator++(int) { Byte_Iterator r(*this); ++i; return r; }
    Byte_Iterator   operator--(int) { Byte_Iterator r(*this); --i; return r; }

    Byte_Iterator&  operator+=(int n) { i += n; return *this; }
    Byte_Iterator&  operator-=(int n) { i -= n; return *this; }

    difference_type operator-(Byte_Iterator const& r) const { return i - r.i; }

    bool operator<(Byte_Iterator const& r) const  { return i <  r.i; }
    bool operator<=(Byte_Iterator const& r) const { return i <= r.i; }
    bool operator>(Byte_Iterator const& r) const  { return i >  r.i; }
    bool operator>=(Byte_Iterator const& r) const { return i >= r.i; }
    bool operator!=(Byte_Iterator const& r) const { return i != r.i; }
    bool operator==(Byte_Iterator const& r) const { return i == r.i; }

  private:
    const byte_vector* m_bv;
    int i;
  };

  byte_vector() : m_data(nullptr), m_size(0), m_capacity(0) {}

  byte_vector(size_t set_capacity) : m_size(0) {
    m_capacity = get_page_aligned_size(set_capacity);
    // now that the shm_file is the correct size we can memory map to it.
    std::byte* m_data = (std::byte*) mmap(NULL, m_capacity, PROT_READ | PROT_WRITE, MAP_PRIVATE, MAP_ANONYMOUS, 0);
    if (m_data == MAP_FAILED) {
      throw std::runtime_error("mmap failed");
    }
  }

  byte_vector(std::vector<std::byte> &stream) {
    m_capacity = get_page_aligned_size(stream.capacity());
    // now that the shm_file is the correct size we can memory map to it.
    std::byte* m_data = (std::byte*) mmap(NULL, m_capacity, PROT_READ | PROT_WRITE, MAP_PRIVATE, MAP_ANONYMOUS, 0);
    if (m_data == MAP_FAILED) {
      throw std::runtime_error("mmap failed");
    }
    memcpy(m_data, stream.data(), stream.size());
    m_size = stream.size();
  }

  ~byte_vector() {
    munmap(m_data, m_capacity);
  }

  byte_vector(byte_vector&)        = default;
  byte_vector(const byte_vector&)  = default;
  byte_vector(byte_vector&&)       = default;

  const reference operator[](int i) const { return m_data[i]; }
  reference operator[](int i) { return m_data[i]; }
  pointer data() const { return m_data; }
  bool empty() const { return m_size == 0; }
  void clear() { m_size = 0; } 
  size_t size() const { return m_size; }
  size_t capacity() const { return m_capacity; }

  Byte_Iterator begin() { return Byte_Iterator(this, 0); }
  Byte_Iterator end()   { return Byte_Iterator(this, m_size); }
  
  void swap(byte_vector& other) {
    using std::swap;
    swap(*this, other);
  }

  void reserve(size_t cap) {
    size_t new_capacity = get_page_aligned_size(cap);
    m_data = (std::byte*) mremap(m_data, m_capacity, new_capacity, MREMAP_MAYMOVE);
    if(m_data == MAP_FAILED) {
      throw std::runtime_error("mremap failed");
    }
    m_capacity = new_capacity;
  } 

  void resize(size_t s) {
    if(s < m_capacity) this->reserve(s);
    m_size = s;
  }

private:
  size_t get_page_aligned_size(size_t s) const {
    auto pagesize = getpagesize();
    auto num_pages = s / pagesize;
    if (s % pagesize != 0) num_pages++;
    return num_pages * pagesize;
  }

  friend void swap(byte_vector& first, byte_vector& second);

  std::byte* m_data;
  uint64_t m_size;
  uint64_t m_capacity;
};

void swap(byte_vector& first, byte_vector& second) {
  using std::swap;
  swap(first.m_data, second.m_data);
  swap(first.m_size, second.m_size);
  swap(first.m_capacity, second.m_capacity);
}

} // end ygm namespace
