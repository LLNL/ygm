// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <vector>
#include <ygm/comm.hpp>
#include <ygm/container/detail/hash_partitioner.hpp>
#include <ygm/detail/ygm_ptr.hpp>

namespace ygm::container::detail {
template <typename Item, typename Partitioner>
class disjoint_set_impl {
 public:
  using self_type         = disjoint_set_impl<Item, Partitioner>;
  using self_ygm_ptr_type = typename ygm::ygm_ptr<self_type>;
  using value_type        = Item;

  Partitioner partitioner;

  disjoint_set_impl(ygm::comm &comm)
      : m_comm(comm), pthis(this), m_num_local_sets(0) {
    m_comm.barrier();
  }

  ~disjoint_set_impl() { m_comm.barrier(); }

  void async_make_set(const value_type &item) {
    auto inserter = [](auto dset, const value_type &item) {
      auto itr = dset->m_local_item_parent_map.find(item);

      // Only insert if no entry exists
      if (itr == dset->m_local_item_parent_map.end()) {
        dset->m_local_item_parent_map.insert(std::make_pair(item, item));
        dset->m_num_local_sets++;
      }
    };

    int dest = owner(item);
    m_comm.async(dest, inserter, pthis, item);
  }

  void async_union(const value_type &a, const value_type &b) {
    // Walking up parent trees can be expressed as a recursive operation
    struct simul_parent_walk_functor {
      void operator()(self_ygm_ptr_type pdset, const value_type &my_item,
                      const value_type &other_item) {
        const auto my_parent = pdset->local_get_parent(my_item);

        // Found root
        if (my_parent == my_item) {
          pdset->local_set_parent(my_item, other_item);
          pdset->m_num_local_sets--;
          return;
        }

        // Switch branches
        if (my_parent < other_item) {
          int dest = pdset->owner(other_item);
          pdset->comm().async(dest, simul_parent_walk_functor(), pdset,
                              other_item, my_parent);
        }
        // Keep walking up current branch
        else if (my_parent > other_item) {
          pdset->local_set_parent(my_item, other_item);  // Splicing
          int dest = pdset->owner(my_parent);
          pdset->comm().async(dest, simul_parent_walk_functor(), pdset,
                              my_parent, other_item);
        }
        // Paths converged. Sets were already merged.
        else {
          return;
        }
      }
    };

    // Visit a first
    if (a > b) {
      int main_dest = owner(a);
      int sub_dest  = owner(b);
      m_comm.async(main_dest, simul_parent_walk_functor(), pthis, a, b);
      // Side-effect of looking up parent of b is setting b's parent to be
      // itself if b has no parent
      m_comm.async(sub_dest,
                   [](self_ygm_ptr_type pdset, const value_type &item) {
                     pdset->local_get_parent(item);
                   },
                   pthis, b);
    }
    // Visit b first
    else if (a < b) {
      int main_dest = owner(b);
      int sub_dest  = owner(a);
      m_comm.async(main_dest, simul_parent_walk_functor(), pthis, b, a);
      m_comm.async(sub_dest,
                   [](self_ygm_ptr_type pdset, const value_type &item) {
                     pdset->local_get_parent(item);
                   },
                   pthis, a);
    } else {
      return;
    }
  }

  // Very inefficient as written. Starts walk from every single item.
  void all_compress() {
    m_comm.barrier();

    struct find_root_functor {
      void operator()(self_ygm_ptr_type pdset, const value_type &source_item,
                      const value_type &local_item) {
        const auto parent = pdset->local_get_parent(local_item);

        // Found root
        if (parent == local_item) {
          int dest = pdset->owner(source_item);
          pdset->comm().async(
              dest,
              [](self_ygm_ptr_type pdset, const value_type &source_item,
                 const value_type &root) {
                pdset->local_set_parent(source_item, root);
              },
              pdset, source_item, parent);
        } else {
          int dest = pdset->owner(parent);
          pdset->comm().async(dest, find_root_functor(), pdset, source_item,
                              parent);
        }
      }
    };

    // Initiate walks up trees from all local items
    for (const auto &item_parent : m_local_item_parent_map) {
      int dest = owner(item_parent.second);
      m_comm.async(dest, find_root_functor(), pthis, item_parent.first,
                   item_parent.second);
    }

    m_comm.barrier();
  }

  std::map<value_type, value_type> all_find(
      const std::vector<value_type> &items) {
    m_comm.barrier();

    using return_type = std::map<value_type, value_type>;
    return_type          to_return;
    ygm_ptr<return_type> p_to_return(&to_return);

    struct find_rep_functor {
      void operator()(self_ygm_ptr_type pdset, ygm_ptr<return_type> p_to_return,
                      const value_type &source_item, const int source_rank,
                      const value_type &local_item) {
        const auto parent = pdset->local_get_parent(local_item);

        // Found root
        if (parent == local_item) {
          // Send message to update parent of original item
          int dest = pdset->owner(source_item);
          pdset->comm().async(
              dest,
              [](self_ygm_ptr_type pdset, const value_type &source_item,
                 const value_type &root) {
                pdset->local_set_parent(source_item, root);
              },
              pdset, source_item, parent);
          // Send message to store return value
          pdset->comm().async(
              source_rank,
              [](ygm_ptr<return_type> p_to_return,
                 const value_type &   source_item,
                 const value_type &rep) { (*p_to_return)[source_item] = rep; },
              p_to_return, source_item, parent);
        } else {
          int dest = pdset->owner(parent);
          pdset->comm().async(dest, find_rep_functor(), pdset, p_to_return,
                              source_item, source_rank, parent);
        }
      }
    };

    for (int i = 0; i < items.size(); ++i) {
      int dest = owner(items[i]);
      m_comm.async(dest, find_rep_functor(), pthis, p_to_return, items[i],
                   m_comm.rank(), items[i]);
    }

    m_comm.barrier();
    return to_return;
  }

  size_t size() {
    m_comm.barrier();
    return m_comm.all_reduce_sum(m_local_item_parent_map.size());
  }

  size_t num_sets() {
    m_comm.barrier();
    return m_comm.all_reduce_sum(m_num_local_sets);
  }

  int owner(const value_type &item) const {
    auto [owner, rank] = partitioner(item, m_comm.size(), 1024);
    return owner;
  }

  bool is_mine(const value_type &item) const {
    return owner(item) == m_comm.rank();
  }

  const value_type &local_get_parent(const value_type &item) {
    ASSERT_DEBUG(is_mine(item) == true);

    auto itr = m_local_item_parent_map.find(item);

    // Create new set if item is not found
    if (itr == m_local_item_parent_map.end()) {
      m_local_item_parent_map.insert(std::make_pair(item, item));
      m_num_local_sets++;
      return item;
    } else {
      return itr->second;
    }
  }

  void local_set_parent(const value_type &item, const value_type &parent) {
    m_local_item_parent_map[item] = parent;
  }

  ygm::comm &comm() { return m_comm; }

 protected:
  disjoint_set_impl() = delete;

  ygm::comm                        m_comm;
  self_ygm_ptr_type                pthis;
  size_t                           m_num_local_sets;
  std::map<value_type, value_type> m_local_item_parent_map;
};
}  // namespace ygm::container::detail
