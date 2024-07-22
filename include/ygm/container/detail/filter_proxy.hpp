// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once


namespace ygm::container::detail {

template <typename Container, typename FilterFunction>
class filter_proxy
    : public base_iteration<filter_proxy<Container, FilterFunction>,
                            typename Container::for_all_args> {
 public:
  using for_all_args = typename Container::for_all_args;

  filter_proxy(Container& rc, FilterFunction filter)
      : m_rcontainer(rc), m_filter_fn(filter) {}

  template <typename Function>
  void for_all(Function fn) {
    auto flambda = [fn, this](auto&&... xs) {
      bool b = m_filter_fn(std::forward<decltype(xs)>(xs)...);
      if (b) {
        fn(std::forward<decltype(xs)>(xs)...);
      }
    };

    m_rcontainer.for_all(flambda);
  }

 private:
  Container&     m_rcontainer;
  FilterFunction m_filter_fn;
};

}