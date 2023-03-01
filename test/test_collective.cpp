// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <ygm/collective.hpp>
#include <ygm/comm.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  ASSERT_RELEASE(ygm::sum(size_t(1), world) == world.size());
  ASSERT_RELEASE(ygm::sum(double(1), world) == double(world.size()));
  ASSERT_RELEASE(ygm::sum(float(1), world) == float(world.size()));

  ASSERT_RELEASE(ygm::min(world.rank(), world) == 0);
  ASSERT_RELEASE(ygm::min(double(world.rank()), world) == double(0));
  ASSERT_RELEASE(ygm::min(float(world.rank()), world) == float(0));

  ASSERT_RELEASE(ygm::max(world.rank(), world) == world.size() - 1);

  ASSERT_RELEASE(ygm::prefix_sum(1, world) == world.rank());

  ASSERT_RELEASE(ygm::logical_and(true, world) == true);
  ASSERT_RELEASE(ygm::logical_and(false, world) == false);
  ASSERT_RELEASE(ygm::logical_or(true, world) == true);
  ASSERT_RELEASE(ygm::logical_or(false, world) == false);

  if (world.size() > 1) {
    ASSERT_RELEASE(ygm::logical_and(world.rank() % 2 == 0, world) == 0);
    ASSERT_RELEASE(ygm::logical_or(world.rank() % 2 == 0, world) == 1);
  }

  return 0;
}