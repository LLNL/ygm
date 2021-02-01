// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG
#include <ygm/comm.hpp>
#include <ygm/container/set.hpp>
#include <ygm/container/bag.hpp>
#include <ygm/container/map.hpp>
#include <ygm/container/counting_set.hpp>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  //
  // Test bag serialization
  {// Create and serialize to file
   {ygm::container::bag<int> my_bag(world);
  if (world.rank0()) {
    my_bag.async_insert(2);
    my_bag.async_insert(5);
    my_bag.async_insert(5);
    my_bag.async_insert(8);
  }
  ASSERT_RELEASE(my_bag.size() == 4);

  my_bag.serialize("serialization_test.bag");
}

// Reload bag and check contents
{
  ygm::container::bag<int> reloaded_bag(world);
  reloaded_bag.deserialize("serialization_test.bag");

  ASSERT_RELEASE(reloaded_bag.size() == 4);
}
}

//
// Test set serialization
{// Create set and serialize to file
 {ygm::container::set<int> my_set(world);
if (world.rank0()) {
  my_set.async_insert(0);
  my_set.async_insert(2);
  my_set.async_insert(3);
  my_set.async_insert(3);
}
ASSERT_RELEASE(my_set.count(0) == 1);
ASSERT_RELEASE(my_set.count(2) == 1);
ASSERT_RELEASE(my_set.count(3) == 1);
ASSERT_RELEASE(my_set.size() == 3);

my_set.serialize("serialization_test.set");
}

// Reload set and check contents
{
  ygm::container::set<int> reloaded_set(world);
  reloaded_set.deserialize("serialization_test.set");

  ASSERT_RELEASE(reloaded_set.count(0) == 1);
  ASSERT_RELEASE(reloaded_set.count(2) == 1);
  ASSERT_RELEASE(reloaded_set.count(3) == 1);
  ASSERT_RELEASE(reloaded_set.size() == 3);

  reloaded_set.async_insert(4);
  ASSERT_RELEASE(reloaded_set.size() == 4);
}
}

//
// Test multiset serialization
{// Create set and serialize to file
 {ygm::container::multiset<int> my_mset(world);
if (world.rank0()) {
  my_mset.async_insert(0);
  my_mset.async_insert(2);
  my_mset.async_insert(3);
  my_mset.async_insert(3);
}
ASSERT_RELEASE(my_mset.count(0) == 1);
ASSERT_RELEASE(my_mset.count(2) == 1);
ASSERT_RELEASE(my_mset.count(3) == 2);
ASSERT_RELEASE(my_mset.size() == 4);

my_mset.serialize("serialization_test.set");
}

// Reload set and check contents
{
  ygm::container::set<int> reloaded_mset(world);
  reloaded_mset.deserialize("serialization_test.set");

  ASSERT_RELEASE(reloaded_mset.count(0) == 1);
  ASSERT_RELEASE(reloaded_mset.count(2) == 1);
  ASSERT_RELEASE(reloaded_mset.count(3) == 2);
  ASSERT_RELEASE(reloaded_mset.size() == 4);

  reloaded_mset.async_insert(4);
  ASSERT_RELEASE(reloaded_mset.size() == 5);
}
}

//
// Test map serialization
{// Create map and serialize to file
 {ygm::container::map<std::string, std::string> smap(world);

smap.async_insert("dog", "cat");
smap.async_insert("apple", "orange");
smap.async_insert("red", "green");

ASSERT_RELEASE(smap.count("dog") == 1);
ASSERT_RELEASE(smap.count("apple") == 1);
ASSERT_RELEASE(smap.count("red") == 1);

smap.serialize("serialization_test.map");
}

// Reload map and check contents
{
  ygm::container::map<std::string, std::string> reloaded_map(world);
  reloaded_map.deserialize("serialization_test.map");

  ASSERT_RELEASE(reloaded_map.count("dog") == 1);
  ASSERT_RELEASE(reloaded_map.count("apple") == 1);
  ASSERT_RELEASE(reloaded_map.count("red") == 1);
}
}

//
// Test multimap serialization
{// Create multimap and serialize to file
 {ygm::container::multimap<std::string, std::string> smap(world);

smap.async_insert("dog", "cat");
smap.async_insert("apple", "orange");
smap.async_insert("red", "green");

ASSERT_RELEASE(smap.count("dog") == world.size());
ASSERT_RELEASE(smap.count("apple") == world.size());
ASSERT_RELEASE(smap.count("red") == world.size());

smap.serialize("serialization_test.mmap");
}

// Reload multimap and check contents
{
  ygm::container::multimap<std::string, std::string> reloaded_mmap(world);
  reloaded_mmap.deserialize("serialization_test.mmap");

  ASSERT_RELEASE(reloaded_mmap.count("dog") == world.size());
  ASSERT_RELEASE(reloaded_mmap.count("apple") == world.size());
  ASSERT_RELEASE(reloaded_mmap.count("red") == world.size());
}
}

//
// Test counting set serialization
{
  // Create counting set and serialize to file
  {
    ygm::container::counting_set<std::string> cset(world);

    cset.async_insert("dog");
    cset.async_insert("apple");
    cset.async_insert("red");

    ASSERT_RELEASE(cset.count("dog") == world.size());
    ASSERT_RELEASE(cset.count("apple") == world.size());
    ASSERT_RELEASE(cset.count("red") == world.size());
    ASSERT_RELEASE(cset.size() == 3);

    auto count_map = cset.all_gather({"dog", "cat", "apple"});
    ASSERT_RELEASE(count_map["dog"] == world.size());
    ASSERT_RELEASE(count_map["apple"] == world.size());
    ASSERT_RELEASE(cset.count("cat") == 0);

    ASSERT_RELEASE(cset.count_all() == 3 * world.size());

    cset.serialize("serialization_test.cset");
  }

  // Reload counting set and check contents
  {
    ygm::container::counting_set<std::string> reloaded_cset(world);
    reloaded_cset.deserialize("serialization_test.cset");

    ASSERT_RELEASE(reloaded_cset.count("dog") == world.size());
    ASSERT_RELEASE(reloaded_cset.count("apple") == world.size());
    ASSERT_RELEASE(reloaded_cset.count("red") == world.size());
    ASSERT_RELEASE(reloaded_cset.size() == 3);

    auto count_map = reloaded_cset.all_gather({"dog", "cat", "apple"});
    ASSERT_RELEASE(count_map["dog"] == world.size());
    ASSERT_RELEASE(count_map["apple"] == world.size());
    ASSERT_RELEASE(reloaded_cset.count("cat") == 0);

    ASSERT_RELEASE(reloaded_cset.count_all() == 3 * world.size());
  }
}
return 0;
}
