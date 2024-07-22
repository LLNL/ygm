// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

namespace ygm::container::detail {

template <typename Container>
class flatten_proxy
    : public base_iteration<flatten_proxy<Container>,
                            std::tuple<std::tuple_element_t<
                                0, typename Container::for_all_args>>> {
  // static_assert(
  //     type_traits::is_vector<
  //         std::tuple_element<0, typename Container::for_all_args>>::value);

 public:
  using for_all_args =
      std::tuple<std::tuple_element_t<0, typename Container::for_all_args>>;

  flatten_proxy(Container& rc) : m_rcontainer(rc) {}

  template <typename Function>
  void for_all(Function fn) {
    auto flambda =
        [fn](std::tuple_element_t<0, typename Container::for_all_args>&
                 stlcont) {
          for (auto& v : stlcont) {
            fn(v);
          }
        };

    m_rcontainer.for_all(flambda);
  }

 private:
  Container& m_rcontainer;
};

}