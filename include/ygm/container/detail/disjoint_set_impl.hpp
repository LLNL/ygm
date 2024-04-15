// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <unordered_map>
#include <vector>
#include <ygm/comm.hpp>
#include <ygm/container/container_traits.hpp>
#include <ygm/container/detail/hash_partitioner.hpp>
#include <ygm/detail/ygm_ptr.hpp>
#include <ygm/detail/ygm_traits.hpp>

namespace ygm::container::detail {
template <typename Item, typename Partitioner>
class disjoint_set_impl {
 public:
  class rank_parent_t;
  using self_type          = disjoint_set_impl<Item, Partitioner>;
  using self_ygm_ptr_type  = typename ygm::ygm_ptr<self_type>;
  using value_type         = Item;
  using size_type          = size_t;
  using ygm_for_all_types  = std::tuple<Item, Item>;
  using ygm_container_type = ygm::container::disjoint_set_tag;
  using rank_type          = int16_t;
  using parent_map_type    = std::map<value_type, rank_parent_t>;

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
        if (my_parent ==
            my_item) {  // Merging new item onto root. Need to increase rank.
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

        if (my_parent == other_parent || my_parent == other_item) {
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
                  other_parent);  // other_parent may be of same rank as my_item
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

        if (my_parent == other_parent || my_parent == other_item) {
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
              if constexpr (std::is_invocable<decltype(fn), const value_type &,
                                              const value_type &,
                                              FunctionArgs &...>() ||
                            std::is_invocable<decltype(fn), self_ygm_ptr_type,
                                              const value_type &,
                                              const value_type &,
                                              FunctionArgs &...>()) {
                ygm::meta::apply_optional(
                    *f, std::make_tuple(p_dset),
                    std::forward_as_tuple(orig_a, orig_b, args...));
              } else {
                static_assert(
                    ygm::detail::always_false<>,
                    "remote disjoint_set lambda signature must be invocable "
                    "with (const value_type &, const value_type &) signature");
              }

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
  }

  void all_compress() {
    struct rep_query {
      value_type              rep;
      std::vector<value_type> local_inquiring_items;
    };

    struct item_status {
      bool             found_root;
      std::vector<int> held_responses;
    };

    static rank_type                                 level;
    static std::unordered_map<value_type, rep_query> queries;
    static std::unordered_map<value_type, item_status>
        local_item_status;  // For holding incoming queries while my items are
                            // waiting for their representatives (only needed
                            // for when parent rank is same as mine)

    struct update_rep_functor {
     public:
      void operator()(self_ygm_ptr_type p_dset, const value_type &parent,
                      const value_type &rep) {
        auto &local_rep_query = queries.at(parent);
        local_rep_query.rep   = rep;

        for (const auto &local_item : local_rep_query.local_inquiring_items) {
          p_dset->local_set_parent(local_item, rep);

          // Forward rep for any held responses
          auto local_item_statuses_iter = local_item_status.find(local_item);
          if (local_item_statuses_iter != local_item_status.end()) {
            for (int dest : local_item_statuses_iter->second.held_responses) {
              p_dset->comm().async(dest, update_rep_functor(), p_dset,
                                   local_item, rep);
            }
            local_item_statuses_iter->second.found_root = true;
            local_item_statuses_iter->second.held_responses.clear();
          }
        }
        local_rep_query.local_inquiring_items.clear();
      }
    };

    auto query_rep_lambda = [](self_ygm_ptr_type p_dset, const value_type &item,
                               int inquiring_rank) {
      const auto &item_info = p_dset->m_local_item_parent_map[item];

      if (item_info.get_rank() > level) {
        const value_type &rep = item_info.get_parent();

        p_dset->comm().async(inquiring_rank, update_rep_functor(), p_dset, item,
                             rep);
      } else {  // May need to hold because this item is in the current level
        auto local_item_status_iter = local_item_status.find(item);
        // If query is ongoing for my parent, hold response
        if ((local_item_status_iter != local_item_status.end()) &&
            (local_item_status_iter->second.found_root == false)) {
          local_item_status[item].held_responses.push_back(inquiring_rank);
        } else {
          p_dset->comm().async(inquiring_rank, update_rep_functor(), p_dset,
                               item, item_info.get_parent());
        }
      }
    };

    m_comm.barrier();

    level = max_rank();
    while (level >= 0) {
      queries.clear();
      local_item_status.clear();

      // Prepare all queries for this round
      for (const auto &[local_item, item_info] : m_local_item_parent_map) {
        if (item_info.get_rank() == level &&
            item_info.get_parent() != local_item) {
          local_item_status[local_item].found_root = false;

          auto query_iter = queries.find(item_info.get_parent());
          if (query_iter == queries.end()) {  // Have not queried for parent's
                                              // rep. Begin new query.
            auto &new_query = queries[item_info.get_parent()];
            new_query.rep   = item_info.get_parent();
            new_query.local_inquiring_items.push_back(local_item);

          } else {
            query_iter->second.local_inquiring_items.push_back(local_item);
          }
        }
      }

      m_comm.cf_barrier();

      // Start all queries for this round
      for (const auto &[item, query] : queries) {
        int dest = owner(item);
        m_comm.async(dest, query_rep_lambda, pthis, item, m_comm.rank());
      }

      m_comm.barrier();

      --level;
    }
  }

  template <typename Function>
  void for_all(Function fn) {
    all_compress();

    if constexpr (std::is_invocable<decltype(fn), const value_type &,
                                    const value_type &>()) {
      const auto end = m_local_item_parent_map.end();
      for (auto iter = m_local_item_parent_map.begin(); iter != end; ++iter) {
        const auto &[item, rank_parent_pair] = *iter;
        fn(item, rank_parent_pair.get_parent());
      }
    } else {
      static_assert(ygm::detail::always_false<>,
                    "local disjoint_set lambda signature must be invocable "
                    "with (const value_type &, const value_type &) signature");
    }
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

    for (size_type i = 0; i < items.size(); ++i) {
      int dest = owner(items[i]);
      m_comm.async(dest, find_rep_functor(), pthis, p_to_return, items[i],
                   m_comm.rank(), items[i]);
    }

    m_comm.barrier();
    return to_return;
  }

  void clear() {
    m_comm.barrier();
    m_local_item_parent_map.clear();
  }

  size_type size() {
    m_comm.barrier();
    return m_comm.all_reduce_sum(m_local_item_parent_map.size());
  }

  size_type num_sets() {
    m_comm.barrier();
    size_t num_local_sets{0};
    for (const auto &item_parent_pair : m_local_item_parent_map) {
      if (item_parent_pair.first == item_parent_pair.second.get_parent()) {
        ++num_local_sets;
      }
    }
    return m_comm.all_reduce_sum(num_local_sets);
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
      return itr->second.get_rank();
    }
    return 0;
  }

  void local_set_parent(const value_type &item, const value_type &parent) {
    m_local_item_parent_map[item].set_parent(parent);
  }

  rank_type max_rank() {
    rank_type local_max{0};

    for (const auto &local_item : m_local_item_parent_map) {
      local_max = std::max<rank_type>(local_max, local_item.second.get_rank());
    }

    return m_comm.all_reduce_max(local_max);
  }

  ygm::comm &comm() { return m_comm; }

 protected:
  disjoint_set_impl() = delete;

  ygm::comm        &m_comm;
  self_ygm_ptr_type pthis;
  parent_map_type   m_local_item_parent_map;
};
}  // namespace ygm::container::detail
