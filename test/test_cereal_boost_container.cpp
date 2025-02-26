// Copyright 2019-2024 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <vector>

#include <boost/container/vector.hpp>

#include <ygm/detail/assert.hpp>
#include <ygm/detail/byte_vector.hpp>
#include <ygm/detail/cereal_boost_container.hpp>
#include <ygm/detail/ygm_cereal_archive.hpp>

int main() {
  // Currently, we only support boost::container::vector

  boost::container::vector<int> original_value = {1, 2, 3, 4, 5};
  ygm::detail::byte_vector      cereal_buffer;
  {
    cereal::YGMOutputArchive archive(cereal_buffer);
    archive(original_value);
  }

  {
    cereal::YGMInputArchive archive(cereal_buffer.data(), cereal_buffer.size());
    boost::container::vector<int> load_value;
    archive(load_value);

    YGM_ASSERT_RELEASE(original_value == load_value);
  }

  return 0;
}