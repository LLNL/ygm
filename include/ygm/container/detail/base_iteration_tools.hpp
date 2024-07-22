// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <functional>
#include <tuple>
#include <utility>

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

  template <typename Function>
  void for_all(Function fn) const {
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
              std::declval<typename Container::for_all_args>()))>::type> {
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

template <typename derived_type, typename for_all_args>
struct base_iteration_tools {
  template <typename FilterFunction>
  filter_proxy<derived_type, FilterFunction> filter(FilterFunction ffn) {
    derived_type* derived_this = static_cast<derived_type*>(this);
    return filter_proxy<derived_type, FilterFunction>(*derived_this, ffn);
  }

  template <typename MapFunction>
  map_proxy<derived_type, MapFunction> map(MapFunction ffn) {
    derived_type* derived_this = static_cast<derived_type*>(this);
    return map_proxy<derived_type, MapFunction>(*derived_this, ffn);
  }

  flatten_proxy<derived_type> flatten() {
    // static_assert(
    //     type_traits::is_vector<std::tuple_element<0, for_all_args>>::value);
    derived_type* derived_this = static_cast<derived_type*>(this);
    return flatten_proxy<derived_type>(*derived_this);
  }
};

}  // namespace ygm::container::detail