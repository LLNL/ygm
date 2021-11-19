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

  disjoint_set_impl(ygm::comm &comm) : m_comm(comm), pthis(this) {
    m_comm.barrier();
  }

  ~disjoint_set_impl() { m_comm.barrier(); }

  void async_union(const value_type &a, const value_type &b) {
    // Walking up parent trees can be expressed as a recursive operation
    struct simul_parent_walk_functor {
      void operator()(self_ygm_ptr_type pdset, const value_type &my_item,
                      const value_type &other_item) {
        const auto my_parent = pdset->local_get_parent(my_item);

        // Found root
        if (my_parent == my_item) {
          pdset->local_set_parent(my_item, other_item);
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
      // Set item as own parent
      m_comm.async(owner(a),
                   [](self_ygm_ptr_type pdset, const value_type &item) {
                     pdset->local_get_parent(item);
                   },
                   pthis, a);
    }
  }

  template <typename Function, typename... FunctionArgs>
  void async_union_and_execute(const value_type &a, const value_type &b,
                               Function fn, const FunctionArgs &... args) {
    // Walking up parent trees can be expressed as a recursive operation
    struct simul_parent_walk_functor {
      void operator()(self_ygm_ptr_type pdset, const value_type &my_item,
                      const value_type &other_item, const value_type &orig_a,
                      const value_type &orig_b, const FunctionArgs &... args) {
        const auto my_parent = pdset->local_get_parent(my_item);

        // Found root
        if (my_parent == my_item) {
          pdset->local_set_parent(my_item, other_item);

          // Perform user function after merge
          Function *f = nullptr;
          ygm::meta::apply_optional(
              *f, std::make_tuple(pdset),
              std::forward_as_tuple(orig_a, orig_b, args...));

          return;
        }

        // Switch branches
        if (my_parent < other_item) {
          int dest = pdset->owner(other_item);
          pdset->comm().async(dest, simul_parent_walk_functor(), pdset,
                              other_item, my_parent, orig_a, orig_b, args...);
        }
        // Keep walking up current branch
        else if (my_parent > other_item) {
          pdset->local_set_parent(my_item, other_item);  // Splicing
          int dest = pdset->owner(my_parent);
          pdset->comm().async(dest, simul_parent_walk_functor(), pdset,
                              my_parent, other_item, orig_a, orig_b, args...);
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
      m_comm.async(main_dest, simul_parent_walk_functor(), pthis, a, b, a, b,
                   args...);
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
      m_comm.async(main_dest, simul_parent_walk_functor(), pthis, b, a, a, b,
                   args...);
      m_comm.async(sub_dest,
                   [](self_ygm_ptr_type pdset, const value_type &item) {
                     pdset->local_get_parent(item);
                   },
                   pthis, a);
    } else {
      // Set item as own parent
      m_comm.async(owner(a),
                   [](self_ygm_ptr_type pdset, const value_type &item) {
                     pdset->local_get_parent(item);
                   },
                   pthis, a);
    }
  }

  void all_compress() {
    m_comm.barrier();

    static std::set<value_type>    active_set;
    static std::vector<value_type> active_set_to_remove;
    // parents being looked up -> vector<local keys looking up parent>,
    // grandparent (if returned), active parent (if returned), lookup returned
    // flag
    static std::map<value_type,
                    std::tuple<std::vector<value_type>, value_type, bool, bool>>
        parent_lookup_map;

    active_set.clear();
    active_set_to_remove.clear();
    parent_lookup_map.clear();

    auto find_grandparent_lambda = [](auto p_dset, const value_type &parent,
                                      const int inquiring_rank) {
      const value_type &grandparent = p_dset->local_get_parent(parent);

      if (active_set.count(parent)) {
        p_dset->comm().async(inquiring_rank,
                             [](auto p_dset, const value_type &parent,
                                const value_type &grandparent) {
                               auto &inquiry_tuple = parent_lookup_map[parent];
                               std::get<1>(inquiry_tuple) = grandparent;
                               std::get<2>(inquiry_tuple) = true;
                               std::get<3>(inquiry_tuple) = true;

                               // Process all waiting lookups
                               auto &child_vec = std::get<0>(inquiry_tuple);
                               for (const auto &child : child_vec) {
                                 p_dset->local_set_parent(child, grandparent);
                               }

                               child_vec.clear();
                             },
                             p_dset, parent, grandparent);
      } else {
        p_dset->comm().async(inquiring_rank,
                             [](auto p_dset, const value_type &parent,
                                const value_type &grandparent) {
                               auto &inquiry_tuple = parent_lookup_map[parent];
                               std::get<1>(inquiry_tuple) = grandparent;
                               std::get<2>(inquiry_tuple) = true;
                               std::get<3>(inquiry_tuple) = false;

                               // Process all waiting lookups
                               auto &child_vec = std::get<0>(inquiry_tuple);
                               for (const auto &child : child_vec) {
                                 p_dset->local_set_parent(child, grandparent);
                                 active_set_to_remove.push_back(child);
                               }

                               child_vec.clear();
                             },
                             p_dset, parent, grandparent);
      }
    };

    // Initialize active set to contain all non-roots
    for (const auto &item_parent_pair : m_local_item_parent_map) {
      if (item_parent_pair.first != item_parent_pair.second) {
        active_set.emplace(item_parent_pair.first);
      }
    }

    while (m_comm.all_reduce_sum(active_set.size())) {
      for (const auto &item : active_set) {
        const value_type &parent = local_get_parent(item);

        auto parent_lookup_iter = parent_lookup_map.find(parent);
        // Already seen this parent
        if (parent_lookup_iter != parent_lookup_map.end()) {
          // Already found grandparent
          if (std::get<2>(parent_lookup_iter->second)) {
            local_set_parent(item, std::get<1>(parent_lookup_iter->second));
            if (!std::get<3>(parent_lookup_iter->second)) {
              active_set_to_remove.push_back(item);
            }
          } else {  // Grandparent hasn't returned yet
            std::get<0>(parent_lookup_iter->second).push_back(item);
          }
        } else {  // Need to look up grandparent
          parent_lookup_map.emplace(std::make_pair(
              parent, std::make_tuple(std::vector<value_type>({item}), parent,
                                      false, true)));

          const int dest = owner(parent);
          m_comm.async(dest, find_grandparent_lambda, pthis, parent,
                       m_comm.rank());
        }
      }
      m_comm.barrier();

      for (const auto &item : active_set_to_remove) {
        active_set.erase(item);
      }
      active_set_to_remove.clear();
      parent_lookup_map.clear();
    }
  }

  template <typename Function>
  void for_all(Function fn) {
    all_compress();

    std::for_each(m_local_item_parent_map.begin(),
                  m_local_item_parent_map.end(), fn);
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

    for (size_t i = 0; i < items.size(); ++i) {
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
    size_t num_local_sets{0};
    for (const auto &item_parent_pair : m_local_item_parent_map) {
      if (item_parent_pair.first == item_parent_pair.second) {
        ++num_local_sets;
      }
    }
    return m_comm.all_reduce_sum(num_local_sets);
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
  std::map<value_type, value_type> m_local_item_parent_map;
};
}  // namespace ygm::container::detail
