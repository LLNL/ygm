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
  // node counts agree
  {
    int node_count(world.layout().node_count());
    int min_node_count = world.all_reduce_min(node_count);
    world.barrier();
    // world.cout0("node count: ", min_node_count);
    ASSERT_RELEASE(min_node_count == node_count);
  }

  //
  // local counts agree
  {
    int local_count(world.layout().local_count());
    int min_local_count = world.all_reduce_min(local_count);
    world.barrier();
    // world.cout0("local count: ", min_local_count);
    ASSERT_RELEASE(min_local_count == local_count);
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
          // std::cout << "I am: " << pcomm->rank()
          //           << ", local: " << pcomm->layout().local_id()
          //           << ", local guess is: " << guess << std::endl;
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
      // std::cout << "I am: " << pcomm->layout().rank() << ", my node id is "
      //           << pcomm->layout().node_id() << " and I am "
      //           << ((pcomm->layout().is_local(rank)) ? "" : "not ")
      //           << "local to rank " << rank << std::endl;
      ASSERT_RELEASE(pcomm->layout().is_local(rank) == tru);
    };

    bool target = (world.layout().node_id() == 0) ? true : false;
    // world.cout("node id:", world.layout().node_id(), "target: ", target);
    // world.cout(world.layout().node_id(), ", ", world.layout().local_id());
    world.async(0, check_fn, world.layout().rank(), target);
    world.barrier();
  }

  return 0;
}
