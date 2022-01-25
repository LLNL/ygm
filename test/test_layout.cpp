// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG
#include <ygm/comm.hpp>
#include <ygm/detail/layout.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  //
  // node sizes agree
  {
    int node_size(world.layout().node_size());
    int min_node_size = world.all_reduce_min(node_size);
    world.barrier();
    ASSERT_RELEASE(min_node_size == node_size);
  }

  //
  // local sizes agree
  {
    int local_size(world.layout().local_size());
    int min_local_size = world.all_reduce_min(local_size);
    world.barrier();
    ASSERT_RELEASE(min_local_size == local_size);
  }

  //
  // local and node id computations agree locally
  {
    for (int dst(0); dst < world.size(); ++dst) {
      auto p = world.layout().rank_to_nl(dst);
      ASSERT_RELEASE(p.first == world.layout().node_id(dst));
      ASSERT_RELEASE(p.second == world.layout().local_id(dst));
    }
    world.barrier();
  }

  //
  // local and node id computations agree globally
  {
    if (world.rank0()) {
      for (int dst(0); dst < world.size(); ++dst) {
        auto check_fn = [](auto pcomm, int node_guess, int local_guess) {
          ASSERT_RELEASE(pcomm->layout().node_id() == node_guess);
          ASSERT_RELEASE(pcomm->layout().local_id() == local_guess);
        };
        auto p = world.layout().rank_to_nl(dst);
        world.async(dst, check_fn, p.first, p.second);
      }
    }
    world.barrier();
  }

  //
  // is_local() is correct
  {
    auto check_fn = [](auto pcomm, int rank, bool tru) {
      ASSERT_RELEASE(pcomm->layout().is_local(rank) == tru);
    };

    bool target = (world.layout().node_id() == 0) ? true : false;
    world.async(0, check_fn, world.layout().rank(), target);
    world.barrier();
  }

  return 0;
}
