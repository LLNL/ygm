// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <vector>
#include <ygm/comm.hpp>
#include <ygm/container/disjoint_set.hpp>
#include <ygm/container/counting_set.hpp>
#include <ygm/container/bag.hpp>
#include <ygm/container/map.hpp>
#include <ygm/io/line_parser.hpp>
#include <fstream>
#include <random>
#include <algorithm>

int main(int argc, char **argv) {
    ygm::comm world(&argc, &argv);

    // ygm::io::line_parser file_reader(world, {"facebook_combined.txt"});
    ygm::container::bag<std::pair<int,int>> graph_edges(world);
    std::vector<std::pair<int,int>> edges;
    file_reader.for_all([&graph_edges](const std::string& line) {
        // Line Parsing
        int start = 0;
        std::string delim = " ";
        int end = line.find(delim);
        std::vector<std::string> split_vec;
        while (end != std::string::npos) {
            split_vec.push_back(line.substr(start, end - start));
            start = end + delim.size();
            end = line.find(delim, start);
        }
        split_vec.push_back(line.substr(start, end - start));
        // graph_edges.async_insert(std::make_pair(std::stoi(split_vec[0]), std::stoi(split_vec[1])));
        edges.push_back(std::make_pair(std::stoi(split_vec[0]), std::stoi(split_vec[1])));
    }); 

    // std::vector<std::pair<int,int>> edges;
    // if (world.rank0()) {
    //     for (int i = 0; i < 100; i++) {
    //         for (int j = i+1; j < 100; j++) {
    //             // graph_edges.async_insert(std::make_pair(i,j));
    //             edges.push_back(std::make_pair(i,j));
    //         }
    //     }
    // }
    std::vector<int> label_vec;
    for (int i = 0; i < edges.size(); i++) {
        label_vec.push_back(i);
    }
    std::default_random_engine rand_eng = std::default_random_engine(42);

    world.barrier();

    // world.cout0() << "Graph has " << graph_edges.size() << " edges" << std::endl;
    
    static std::vector<std::pair<int, int>> local_spanning_tree_edges;
    ygm::container::counting_set<std::string> edge_frequency(world);
    ygm::container::disjoint_set<int> dset(world);

    int trees = 10;
    // Start generating random spanning trees
    for (int i = 0; i < trees; i++) { 

        local_spanning_tree_edges.clear();
        graph_edges.clear();

        // Shuffle ranks on rank 0 then redistribute
        std::shuffle(edges.begin(), edges.end(), std::default_random_engine(std::random_device()()));
        // Shuffle label vec with same seed
        std::shuffle(label_vec.begin(), label_vec.end(), rand_eng);

        if (world.rank0()) {
            for (auto edge : edges) {
                graph_edges.async_insert(edge);
            }
        }

        graph_edges.local_shuffle();
        world.barrier();

        auto add_spanning_tree_edges_lambda = [](const int u, const int v) {
            local_spanning_tree_edges.push_back(std::make_pair(u, v));
        };

        auto process_edge_lambda = [&dset, &add_spanning_tree_edges_lambda](const std::pair<int,int> edge) {
            // world.barrier();
            dset.async_union_and_execute(edge.first, edge.second, add_spanning_tree_edges_lambda);
        };

        // Generate tree
        world.barrier();
        graph_edges.for_all(process_edge_lambda);

        // world.cout0() << "Spanning Trees Generated: " << i << std::endl;

        // Now use counting set to count edge occurrences
        world.barrier();
        for (const auto edge : local_spanning_tree_edges) {
            int real_label_a =label_vec[edge.first];
            int real_label_b =label_vec[edge.second];
            std::string edge_str;
            if (real_label_a < real_label_b) {
                edge_str = std::to_string(real_label_a) + "," + std::to_string(real_label_b);
            }
            else {
                edge_str = std::to_string(real_label_b) + "," + std::to_string(real_label_a);
            }
            // std::string edge_str = std::to_string(edge.first) + "," + std::to_string(edge.second);
            edge_frequency.async_insert(edge_str);
        }

        dset.clear();
    }
    world.barrier();

    auto count_lambda = [&world](const std::pair<std::string,int> edge_count){
        world.cout() << "(" << edge_count.first << ")" << ": " <<  edge_count.second << std::endl;
    };

    edge_frequency.for_all(count_lambda);

    world.barrier();
    return 0;
}