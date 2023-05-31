// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once
#include <type_traits>


namespace ygm::container::detail {
struct array_tag;
struct bag_tag;
struct counting_set_tag;
struct disjoint_set_tag;
struct map_tag;
struct set_tag;


template <class Container, typename = void>
struct has_ygm_container_type : std::false_type {};

template <class Container>
struct has_ygm_container_type<
    Container,
    std::void_t< typename Container::ygm_container_type > 
> : std::true_type {};

template <class Container, typename Tag>
constexpr bool check_ygm_container_type() {
    if constexpr(has_ygm_container_type< Container >::value) {
        return std::is_same< 
            typename Container::ygm_container_type, 
            Tag 
        >::value;
    } else {
        return false;
    }
} 


template <class Container>
constexpr bool is_array(Container &c) {
    return check_ygm_container_type<Container, array_tag>();
}

template <class Container>
constexpr bool is_bag(Container &c) {
    return check_ygm_container_type<Container, bag_tag>();
}

template <class Container>
constexpr bool is_counting_set(Container &c) {
    return check_ygm_container_type<Container, counting_set_tag>();
}

template <class Container>
constexpr bool is_disjoint_set(Container &c) {
    return check_ygm_container_type<Container, disjoint_set_tag>();
}

template <class Container>
constexpr bool is_map(Container &c) {
    return check_ygm_container_type<Container, map_tag>();
}

template <class Container>
constexpr bool is_set(Container &c) {
    return check_ygm_container_type<Container, set_tag>();
}

}   // ygm::container::detail
