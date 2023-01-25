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
  class rank_parent_t;
  using self_type         = disjoint_set_impl<Item, Partitioner>;
  using self_ygm_ptr_type = typename ygm::ygm_ptr<self_type>;
  using value_type        = Item;
  using rank_type         = int16_t;
  using parent_map_type   = std::map<value_type, rank_parent_t>;

  Partitioner partitioner;

  class rank_parent_t {
   public:
    rank_parent_t() : m_rank{-1} {}

    rank_parent_t(const rank_type rank, const value_type &parent)
        : m_rank(rank), m_parent(parent) {}

    bool increase_rank(rank_type new_rank) {
      if (new_rank > m_rank) {
        m_rank = new_rank;
        return true;
      } else {
        return false;
      }
    }

    void set_parent(const value_type &new_parent) { m_parent = new_parent; }

    const rank_type   get_rank() const { return m_rank; }
    const value_type &get_parent() const { return m_parent; }

    template <typename Archive>
    void serialize(Archive &ar) {
      ar(m_parent, m_rank);
    }

   private:
    rank_type  m_rank;
    value_type m_parent;
  };

  disjoint_set_impl(ygm::comm &comm) : m_comm(comm), pthis(this) {
    pthis.check(m_comm);
  }

  ~disjoint_set_impl() { m_comm.barrier(); }

  typename ygm::ygm_ptr<self_type> get_ygm_ptr() const { return pthis; }

  template <typename Visitor, typename... VisitorArgs>
  void async_visit(const value_type &item, Visitor visitor,
                   const VisitorArgs &...args) {
    int  dest          = owner(item);
    auto visit_wrapper = [](auto p_dset, const value_type &item,
                            const VisitorArgs &...args) {
      auto rank_parent_pair_iter = p_dset->m_local_item_parent_map.find(item);
      if (rank_parent_pair_iter == p_dset->m_local_item_parent_map.end()) {
        rank_parent_t new_ranked_item = rank_parent_t(0, item);
        rank_parent_pair_iter =
            p_dset->m_local_item_parent_map
                .insert(std::make_pair(item, new_ranked_item))
                .first;
      }
      Visitor *vis = nullptr;

      ygm::meta::apply_optional(
          *vis, std::make_tuple(p_dset),
          std::forward_as_tuple(*rank_parent_pair_iter, args...));
    };

    m_comm.async(dest, visit_wrapper, pthis, item,
                 std::forward<const VisitorArgs>(args)...);
  }

  void async_union(const value_type &a, const value_type &b) {
    static auto update_parent_lambda = [](auto             &item_info,
                                          const value_type &new_parent) {
      item_info.second.set_parent(new_parent);
    };

    static auto resolve_merge_lambda = [](auto p_dset, auto &item_info,
                                          const value_type &merging_item,
                                          const rank_type   merging_rank) {
      const auto &my_item   = item_info.first;
      const auto  my_rank   = item_info.second.get_rank();
      const auto &my_parent = item_info.second.get_parent();
      ASSERT_RELEASE(my_rank >= merging_rank);

      if (my_rank > merging_rank) {
        return;
      } else {
        ASSERT_RELEASE(my_rank == merging_rank);
        if (my_parent == my_item) {  // Has not found new parent
          item_info.second.increase_rank(merging_rank + 1);
        } else {  // Tell merging item about new parent
          p_dset->async_visit(
              merging_item,
              [](auto &item_info, const value_type &new_parent) {
                item_info.second.set_parent(new_parent);
              },
              my_parent);
        }
      }
    };

    // Walking up parent trees can be expressed as a recursive operation
    struct simul_parent_walk_functor {
      void operator()(self_ygm_ptr_type                           p_dset,
                      std::pair<const value_type, rank_parent_t> &my_item_info,
                      const value_type                           &my_child,
                      const value_type                           &other_parent,
                      const value_type                           &other_item,
                      const rank_type                             other_rank) {
        // Note: other_item needs rank info for comparison with my_item's
        // parent. All others need rank and item to determine if other_item
        // has been visited/initialized.

        const value_type &my_item   = my_item_info.first;
        const rank_type  &my_rank   = my_item_info.second.get_rank();
        const value_type &my_parent = my_item_info.second.get_parent();

        // Path splitting
        if (my_child != my_item) {
          p_dset->async_visit(my_child, update_parent_lambda, my_parent);
        }

        if (my_parent == other_parent) {
          return;
        }

        if (my_rank > other_rank) {  // Other path has lower rank
          p_dset->async_visit(other_parent, simul_parent_walk_functor(),
                              other_item, my_parent, my_item, my_rank);
        } else if (my_rank == other_rank) {
          if (my_parent == my_item) {  // At a root

            if (my_item < other_parent) {  // Need to break ties in rank before
                                           // merging to avoid cycles of merges
                                           // creating cycles in disjoint set
              // Perform merge
              my_item_info.second.set_parent(
                  other_parent);  // Guaranteed any path through current
                                  // item will find an item with rank >=
                                  // my_rank+1 by going to other_parent
              p_dset->async_visit(other_parent, resolve_merge_lambda, my_item,
                                  my_rank);
            } else {
              // Switch to other path to attempt merge
              p_dset->async_visit(other_parent, simul_parent_walk_functor(),
                                  other_item, my_parent, my_item, my_rank);
            }
          } else {  // Not at a root
            // Continue walking current path
            p_dset->async_visit(my_parent, simul_parent_walk_functor(), my_item,
                                other_parent, other_item, other_rank);
          }
        } else {                       // Current path has lower rank
          if (my_parent == my_item) {  // At a root
            my_item_info.second.set_parent(
                other_parent);  // Safe to attach to other path
          } else {              // Not at a root
            // Continue walking current path
            p_dset->async_visit(my_parent, simul_parent_walk_functor(), my_item,
                                other_parent, other_item, other_rank);
          }
        }
      }
    };

    async_visit(a, simul_parent_walk_functor(), a, b, b, -1);

    /*
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
    m_comm.async(
    sub_dest,
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
    m_comm.async(
    sub_dest,
    [](self_ygm_ptr_type pdset, const value_type &item) {
    pdset->local_get_parent(item);
    },
    pthis, a);
    } else {
    // Set item as own parent
    m_comm.async(
    owner(a),
    [](self_ygm_ptr_type pdset, const value_type &item) {
    pdset->local_get_parent(item);
    },
    pthis, a);
    }
    */
  }

  template <typename Function, typename... FunctionArgs>
  void async_union_and_execute(const value_type &a, const value_type &b,
                               Function fn, const FunctionArgs &...args) {
    static auto update_parent_lambda = [](auto             &item_info,
                                          const value_type &new_parent) {
      item_info.second.set_parent(new_parent);
    };

    static auto resolve_merge_lambda = [](auto p_dset, auto &item_info,
                                          const value_type &merging_item,
                                          const rank_type   merging_rank) {
      const auto &my_item   = item_info.first;
      const auto  my_rank   = item_info.second.get_rank();
      const auto &my_parent = item_info.second.get_parent();
      ASSERT_RELEASE(my_rank >= merging_rank);

      if (my_rank > merging_rank) {
        return;
      } else {
        ASSERT_RELEASE(my_rank == merging_rank);
        if (my_parent == my_item) {  // Has not found new parent
          item_info.second.increase_rank(merging_rank + 1);
        } else {  // Tell merging item about new parent
          p_dset->async_visit(
              merging_item,
              [](auto &item_info, const value_type &new_parent) {
                item_info.second.set_parent(new_parent);
              },
              my_parent);
        }
      }
    };

    // Walking up parent trees can be expressed as a recursive operation
    struct simul_parent_walk_functor {
      void operator()(self_ygm_ptr_type                           p_dset,
                      std::pair<const value_type, rank_parent_t> &my_item_info,
                      const value_type                           &my_child,
                      const value_type                           &other_parent,
                      const value_type &other_item, const rank_type other_rank,
                      const value_type &orig_a, const value_type &orig_b,
                      const FunctionArgs &...args) {
        // Note: other_item needs rank info for comparison with my_item's
        // parent. All others need rank and item to determine if other_item
        // has been visited/initialized.

        const value_type &my_item   = my_item_info.first;
        const rank_type  &my_rank   = my_item_info.second.get_rank();
        const value_type &my_parent = my_item_info.second.get_parent();

        // Path splitting
        if (my_child != my_item) {
          p_dset->async_visit(my_child, update_parent_lambda, my_parent);
        }

        if (my_parent == other_parent) {
          return;
        }

        if (my_rank > other_rank) {  // Other path has lower rank
          p_dset->async_visit(other_parent, simul_parent_walk_functor(),
                              other_item, my_parent, my_item, my_rank, orig_a,
                              orig_b, args...);
        } else if (my_rank == other_rank) {
          if (my_parent == my_item) {  // At a root

            if (my_item < other_parent) {  // Need to break ties in rank before
                                           // merging to avoid cycles of merges
                                           // creating cycles in disjoint set
              // Perform merge
              my_item_info.second.set_parent(
                  other_parent);  // Guaranteed any path through current
                                  // item will find an item with rank >=
                                  // my_rank+1 by going to other_parent

              // Perform user function after merge
              Function *f = nullptr;
              ygm::meta::apply_optional(
                  *f, std::make_tuple(p_dset),
                  std::forward_as_tuple(orig_a, orig_b, args...));

              return;

              p_dset->async_visit(other_parent, resolve_merge_lambda, my_item,
                                  my_rank);
            } else {
              // Switch to other path to attempt merge
              p_dset->async_visit(other_parent, simul_parent_walk_functor(),
                                  other_item, my_parent, my_item, my_rank,
                                  orig_a, orig_b, args...);
            }
          } else {  // Not at a root
            // Continue walking current path
            p_dset->async_visit(my_parent, simul_parent_walk_functor(), my_item,
                                other_parent, other_item, other_rank, orig_a,
                                orig_b, args...);
          }
        } else {                       // Current path has lower rank
          if (my_parent == my_item) {  // At a root
            my_item_info.second.set_parent(
                other_parent);  // Safe to attach to other path

            // Perform user function after merge
            Function *f = nullptr;
            ygm::meta::apply_optional(
                *f, std::make_tuple(p_dset),
                std::forward_as_tuple(orig_a, orig_b, args...));

            return;

          } else {  // Not at a root
            // Continue walking current path
            p_dset->async_visit(my_parent, simul_parent_walk_functor(), my_item,
                                other_parent, other_item, other_rank, orig_a,
                                orig_b, args...);
          }
        }
      }
    };

    async_visit(a, simul_parent_walk_functor(), a, b, b, -1, a, b, args...);

    /*
  // Walking up parent trees can be expressed as a recursive operation
  struct simul_parent_walk_functor {
  void operator()(self_ygm_ptr_type pdset, const value_type &my_item,
          const value_type &other_item, const value_type &orig_a,
          const value_type &orig_b, const FunctionArgs &...args) {
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
  m_comm.async(
  sub_dest,
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
  m_comm.async(
  sub_dest,
  [](self_ygm_ptr_type pdset, const value_type &item) {
  pdset->local_get_parent(item);
  },
  pthis, a);
  } else {
  // Set item as own parent
  m_comm.async(
  owner(a),
  [](self_ygm_ptr_type pdset, const value_type &item) {
  pdset->local_get_parent(item);
  },
  pthis, a);
  }
  */
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
        p_dset->comm().async(
            inquiring_rank,
            [](auto p_dset, const value_type &parent,
               const value_type &grandparent) {
              auto &inquiry_tuple        = parent_lookup_map[parent];
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
        p_dset->comm().async(
            inquiring_rank,
            [](auto p_dset, const value_type &parent,
               const value_type &grandparent) {
              auto &inquiry_tuple        = parent_lookup_map[parent];
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
      if (item_parent_pair.first != item_parent_pair.second.get_parent()) {
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
    m_comm.cout0(m_comm.all_reduce_sum(m_local_item_parent_map.size()));
    all_compress();

    const auto end = m_local_item_parent_map.end();
    for (auto iter = m_local_item_parent_map.begin(); iter != end; ++iter) {
      const auto &[item, rank_parent_pair] = *iter;
      fn(std::make_pair(item, rank_parent_pair.get_parent()));
    }
  }

  std::map<value_type, value_type> all_find(
      const std::vector<value_type> &items) {
    m_comm.barrier();

    using return_type = std::map<value_type, value_type>;
    return_type          to_return;
    ygm_ptr<return_type> p_to_return(&to_return);
    /*

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
     const value_type    &source_item,
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

    */
    m_comm.barrier();
    return to_return;
  }

  size_t size() {
    m_comm.barrier();
    return m_comm.all_reduce_sum(m_local_item_parent_map.size());
  }

  size_t num_sets() {
    /*
  m_comm.barrier();
  size_t num_local_sets{0};
  for (const auto &item_parent_pair : m_local_item_parent_map) {
  if (item_parent_pair.first == item_parent_pair.second) {
  ++num_local_sets;
  }
  }
  return m_comm.all_reduce_sum(num_local_sets);
    */
    return 0;
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
      m_local_item_parent_map.insert(
          std::make_pair(item, rank_parent_t(0, item)));
      return m_local_item_parent_map[item].get_parent();
    } else {
      return itr->second.get_parent();
    }
    return m_local_item_parent_map[item].get_parent();
  }

  const rank_type local_get_rank(const value_type &item) {
    ASSERT_DEBUG(is_mine(item) == true);

    auto itr = m_local_item_parent_map.find(item);

    if (itr != m_local_item_parent_map.end()) {
      return itr->second.first;
    }
    return 0;
  }

  void local_set_parent(const value_type &item, const value_type &parent) {
    m_local_item_parent_map[item].set_parent(parent);
  }

  ygm::comm &comm() { return m_comm; }

 protected:
  disjoint_set_impl() = delete;

  ygm::comm         m_comm;
  self_ygm_ptr_type pthis;
  // std::map<value_type, value_type> m_local_item_parent_map;
  parent_map_type m_local_item_parent_map;
};
}  // namespace ygm::container::detail
