// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <functional>
#include <tuple>
#include <utility>

namespace ygm::container::detail {

namespace type_traits {
template <template <typename...> class T, typename U>
struct is_specialization_of : std::false_type {};

template <template <typename...> class T, typename... Us>
struct is_specialization_of<T, T<Us...>> : std::true_type {};

template <typename T>
struct is_vector
    : is_specialization_of<std::vector, typename std::decay<T>::type> {};

template <typename T>
struct is_tuple
    : is_specialization_of<std::tuple, typename std::decay<T>::type> {};

template <class T, bool isTuple>
struct tuple_wrapper_helper  // T is not a tuple
{
  using type = std::tuple<T>;
};

template <class T>
struct tuple_wrapper_helper<T, true>  // T is a tuple
{
  using type = T;
};

template <class T>
struct tuple_wrapper  // T is a tuple
{
  using type = tuple_wrapper_helper<T, is_tuple<T>::value>::type;
};
}  // namespace type_traits

template <typename Container, typename MapFunction>
class map_proxy
    : public base_iteration<
          map_proxy<Container, MapFunction>,
          typename type_traits::tuple_wrapper<decltype(std::apply(
              std::declval<MapFunction>(),
              std::declval<typename Container::for_all_args>()))>::type>{
 private:
  using map_function_ret =
      decltype(std::apply(std::declval<MapFunction>(),
                          std::declval<typename Container::for_all_args>()));

 public:
  using for_all_args = type_traits::tuple_wrapper<map_function_ret>::type;

  map_proxy(Container& rc, MapFunction filter)
      : m_rcontainer(rc), m_map_fn(filter) {}

  template <typename Function>
  void for_all(Function fn) {
    auto mlambda = [fn, this](auto&&... xs) {
      auto map_result = m_map_fn(std::forward<decltype(xs)>(xs)...);
      if constexpr (type_traits::is_tuple<decltype(map_result)>::value) {
        std::apply(fn, map_result);
      } else {
        fn(map_result);
      }
    };

    m_rcontainer.for_all(mlambda);
  }

 private:
  Container&  m_rcontainer;
  MapFunction m_map_fn;
};



}  // namespace ygm::container::detail