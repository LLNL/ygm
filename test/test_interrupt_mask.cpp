// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG
#include <ygm/comm.hpp>
#include <ygm/detail/interrupt_mask.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv, 8);

  int  count{0};
  auto count_ptr = world.make_ygm_ptr(count);
  int  num_sends{100};
  {
    ygm::detail::interrupt_mask mask(world);

    for (int i = 0; i < num_sends; ++i) {
      world.async(
          0, [](auto count_ptr) { ++(*count_ptr); }, count_ptr);
    }

    world.cf_barrier();

    ASSERT_RELEASE(count == 0);
  }

  world.barrier();

  if (world.rank0()) {
    ASSERT_RELEASE(count == num_sends * world.size());
  }

  return 0;
}
