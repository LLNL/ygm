#include <ygm/comm.hpp>
#include <ygm/random.hpp>
#include <ygm/utility.hpp>
#include <ygm/collective.hpp> 
#include <unordered_map>
#include <map>
#include <mpi.h>
#include <cmath>
#include <algorithm>


namespace ygm::random {

template <typename table_id_type, typename item_id_type, typename weight_type, typename RNG>
class multi_alias_table {

    using self_type = multi_alias_table<table_id_type, item_id_type, weight_type, RNG>;

    public: 

        struct item {
            table_id_type t_id;
            item_id_type i_id;
            weight_type weight;

            template <typename Archive>
            void serialize(Archive& ar) { // Needed for cereal serialization
                ar(t_id, i_id, weight);
            }
        };


        struct table_item { 
            double p; // prob item a is selected = p, prob item b is selected = (1 - p)
            item_id_type a;
            item_id_type b;

            template <typename Archive>
            void serialize(Archive& ar) { // Needed for cereal serialization
                ar(p, a, b);
            }
        };


        multi_alias_table(ygm::comm &comm, RNG rng) :
                m_comm(comm),
                pthis(this),
                m_rng(rng),
                m_tables_built(false),
                m_balanced(false),
                // m_local_weight(0),
                m_rank_dist(0, comm.size()-1) { }


        template <typename container>
        void load_items_from_container(container &cont) {
            cont.for_all([&](item itm){
                m_local_items[itm.t_id].push_back(itm);
                m_each_tables_local_weight[itm.t_id] += itm.weight; 
            });
        }        

        void local_add_item (std::tuple<table_id_type, item_id_type, weight_type> i) {
            item itm({std::get<0>(i), std::get<1>(i), std::get<2>(i)});
            m_local_items[itm.t_id].push_back(itm);
            m_each_tables_local_weight[itm.t_id] += itm.weight; 
        }

        void add_item(std::tuple<table_id_type, item_id_type, weight_type> i) {
            item itm({std::get<0>(i), std::get<1>(i), std::get<2>(i)});

            auto add_item_to_rand_rank = [](item i, auto ptr_MAT){
                ptr_MAT->m_local_items[i.t_id].push_back(i);
                ptr_MAT->m_each_tables_local_weight[i.t_id] += i.weight; 
            };

            m_comm.async(m_rank_dist(m_rng), add_item_to_rand_rank, itm, pthis);
        }

        // For debugging
        std::string get_weight_str(std::vector<item> items) {
            std::string weights_str = "< ";
            for (auto& itm : items) {
                weights_str += std::to_string(itm.weight) + " ";
            }
            return weights_str + ">"; 
        }

        std::vector<table_id_type> get_table_ids() {
            std::vector<table_id_type> table_ids;
            for (const table_id_type t : m_each_tables_local_weight) {
                table_ids.push_back(t);
            }
            return table_ids;
        }

        void clear() {
            m_comm.barrier();
            m_tables_built = false;
            m_local_items.clear();
            m_local_alias_tables.clear();
            m_each_tables_local_weight.clear();
        }

        void clear_items() {
            m_local_items.clear(); 
        }
        
        void balance_weight() { 
            using t_id_vec = std::vector<table_id_type>;
            t_id_vec table_ids;
            for (const auto& [t_id, items] : m_local_items) {
                table_ids.push_back(t_id);
            }
            std::sort(table_ids.begin(), table_ids.end());
            table_ids.erase(std::unique(table_ids.begin(), table_ids.end()), table_ids.end());

            auto merge_func = [](t_id_vec a, t_id_vec b){
                t_id_vec res;
                std::set_union(a.cbegin(), a.cend(), b.cbegin(), b.cend(), std::back_inserter(res));
                return res;
            };

            table_ids = m_comm.all_reduce(table_ids, merge_func);
            m_comm.barrier();

            std::vector<double> local_weights_vec(table_ids.size());
            for (int i = 0; i < table_ids.size(); i++) {
                local_weights_vec[i] = m_each_tables_local_weight[table_ids[i]]; 
            }
            MPI_Comm comm = m_comm.get_mpi_comm();
            std::vector<double> w8s_prfx_sum(table_ids.size());
            MPI_Exscan(local_weights_vec.data(), w8s_prfx_sum.data(), local_weights_vec.size(), MPI_DOUBLE, MPI_SUM, comm);
            std::vector<double> global_table_w8s(table_ids.size()); // Global total weight for each table
            MPI_Allreduce(local_weights_vec.data(), global_table_w8s.data(), local_weights_vec.size(), MPI_DOUBLE, MPI_SUM, comm);
            m_comm.barrier();
            // m_comm.cout0("Completed collective communications in balance()");

            ASSERT_RELEASE(ygm::is_same(table_ids.size(), m_comm));
            ASSERT_RELEASE(ygm::is_same(w8s_prfx_sum.size(), m_comm));
            ASSERT_RELEASE(ygm::is_same(global_table_w8s.size(), m_comm));

            std::unordered_map<table_id_type, std::vector<item>> local_table_items;
            // Need to initialize all vectors or accessing map inside lambdas fails
            for (auto& t_id : table_ids) {
                local_table_items.insert({t_id, std::vector<item>()});
            }
            using ygm_items_map_ptr = ygm::ygm_ptr<std::unordered_map<table_id_type, std::vector<item>>>;
            ygm_items_map_ptr ptr_tbl_items = m_comm.make_ygm_ptr(local_table_items); 
            m_comm.barrier(); // ensure ygm_ptr is created on every rank

            for (uint32_t t = 0; t < table_ids.size(); t++) { 
                uint32_t t_id = table_ids[t];
                double p = w8s_prfx_sum[t];
                double global_w8 = global_table_w8s[t];
                double target_w8 = global_w8 / m_comm.size(); // target weight per rank
                uint32_t dest_rank = p / target_w8; 
                double curr_weight = std::fmod(p, target_w8); // Spillage weight
                std::vector<item> items_to_send;

                std::vector<item>& local_items = m_local_items[t_id];
                for (uint64_t i = 0; i < local_items.size(); i++) { // WARNING: SIZE OF m_local_items CAN GROW DURING LOOP
                    item local_item = local_items[i]; 
                    if (curr_weight + local_item.weight >= target_w8) {
                    
                        double remaining_item_w8 = curr_weight + local_item.weight - target_w8;
                        double weight_to_send = local_item.weight - remaining_item_w8;
                        curr_weight += weight_to_send;
                        item item_to_send = {local_item.t_id, local_item.i_id, weight_to_send};
                        items_to_send.push_back(item_to_send);

                        // if (dest_rank == m_comm.rank()) {
                        //     local_table_items[t_id].insert(local_table_items[t_id].end(), items_to_send.begin(), items_to_send.end());  
                        if ((curr_weight > 0.0001) && (dest_rank < m_comm.size())) { // Accounts for rounding errors
                            // Lambda that moves weights to dest_rank's local_table_items
                            m_comm.async(dest_rank, [](table_id_type table_id, std::vector<item> items, ygm_items_map_ptr my_items_ptr) {
                                my_items_ptr->at(table_id).insert(my_items_ptr->at(table_id).end(), items.begin(), items.end()); 
                            }, t_id, items_to_send, ptr_tbl_items);
                        }
                        // Handle case where item weight is large enough to span multiple partitions
                        if (remaining_item_w8 >= target_w8) { 
                            local_items.push_back({local_item.t_id, local_item.i_id, remaining_item_w8});
                            curr_weight = 0;
                        }  else {
                            curr_weight = remaining_item_w8;
                        }
                        items_to_send.clear();
                        if (curr_weight != 0) {
                            items_to_send.push_back({local_item.t_id, local_item.i_id, curr_weight});
                        }
                        dest_rank++;

                    } else {
                        items_to_send.push_back(local_item);
                        curr_weight += local_item.weight;
                    }
                }
                
                // Need to handle left over items and send them to neccessary destination
                if (curr_weight > 0.0001 && dest_rank < m_comm.size()) { // Account for rounding errors
                    // if (dest_rank == m_comm.rank()) {
                    //     local_table_items[t_id].insert(local_table_items[t_id].end(), items_to_send.begin(), items_to_send.end());  
                    // } else {
                        // Lambda that moves the weights to curr_ranks local_table_items
                        m_comm.async(dest_rank, [](table_id_type table_id, std::vector<item> items, ygm_items_map_ptr my_items_ptr) {
                            my_items_ptr->at(table_id).insert(my_items_ptr->at(table_id).end(), items.begin(), items.end()); 
                        }, t_id, items_to_send, ptr_tbl_items);
                    // }
                }
            }
            m_comm.barrier();
            std::swap(local_table_items, m_local_items);

            // Update the total weight of each table that the rank owns
            for (uint32_t t = 0; t < table_ids.size(); t++) {
                uint32_t t_id = table_ids[t];
                double new_total_w8 = 0;
                for (auto& itm : m_local_items[t_id]) {
                    new_total_w8 += itm.weight;
                }
                m_each_tables_local_weight[t_id] = new_total_w8; 
            }

            ASSERT_RELEASE(m_each_tables_local_weight.size() == m_local_items.size());
            m_balanced = true;
        } 

        bool check_balancing() { 
            // Not a perfect check. Will check if every rank has the same weight for each table
            // but does not confirm that the actual target weight is actually on each rank
            std::vector<double> w8_vec;
            for (const auto& w : m_each_tables_local_weight) {
                w8_vec.push_back(w.second); 
                // m_comm.cout(w.first, ", ", w.second);
            } 

            m_comm.barrier();

            ASSERT_RELEASE(ygm::is_same(w8_vec.size(), m_comm));

            auto equal_vec = [](std::vector<double> a, std::vector<double> b){
                if (a.size() != b.size()) {
                    return false;
                } else {
                    for (size_t i = 0; i < a.size(); i++) {
                        if (std::abs(a[i] - b[i]) > 0.0001)
                            return false;
                    }
                }
                return true;
            };

            bool balanced = ygm::is_same(w8_vec, m_comm, equal_vec);

            return balanced;
        }

        void build_alias_tables() {
            if (!m_balanced) {
                balance_weight();
            }
            m_comm.barrier(); 
            for (auto& [t_id, items] : m_local_items) {
                // Make average weight of items 1, so coin flip is simple across all tables 
                double avg_w8 = m_each_tables_local_weight[t_id] / items.size(); 
                double new_total_weight = 0;
                for (auto& i : items) {
                    i.weight /= avg_w8;
                    new_total_weight += i.weight;
                }
                double new_avg_w8 = new_total_weight / items.size(); // Should be 1
                if (std::abs((new_avg_w8 - 1.0)) < 0.0001) {
                    ASSERT_RELEASE(std::abs((new_avg_w8 - 1.0)) < 0.0001);
                }                

                // Implementation of Vose's algorithm, utilized Keith Schwarz numerically stable version
                // https://www.keithschwarz.com/darts-dice-coins/
                std::vector<item> heavy_items;
                std::vector<item> light_items;
                for (auto& item : items) {
                    if (item.weight < 1.0) {
                        light_items.push_back(item);
                    } else {
                        heavy_items.push_back(item);
                    }
                }

                while (!light_items.empty() && !heavy_items.empty()) {
                    item l = light_items.back();
                    item h = heavy_items.back(); 
                    table_item tbl_i = {l.weight, l.i_id, h.i_id};
                    m_local_alias_tables[t_id].push_back(tbl_i);
                    h.weight = (h.weight + l.weight) - 1;
                    light_items.pop_back(); 
                    if (h.weight < 1) {
                        light_items.push_back(h);
                        heavy_items.pop_back();
                    }   
                }

                // Either heavy items or light_items is empty, need to flush the non empty 
                // vector and add them to the alias table with a p value of 1
                while (!heavy_items.empty()) {
                    item h = heavy_items.back();
                    table_item tbl_i = {1.0, h.i_id, 0};
                    m_local_alias_tables[t_id].push_back(tbl_i);
                    heavy_items.pop_back();
                }
                while (!light_items.empty()) {
                    item l = light_items.back();
                    table_item tbl_i = {1.0, l.i_id, 0};
                    m_local_alias_tables[t_id].push_back(tbl_i);
                    light_items.pop_back();
                }
            }
            m_comm.barrier();
            m_tables_built = true;
        }

        uint32_t get_rank() {
            return m_comm.rank();
        }

        template <typename Visitor, typename... VisitorArgs>
        void async_sample(table_id_type table_id, Visitor visitor, const VisitorArgs &...args) { // Sample should be provided a function with takes as an argument the item type 

            ASSERT_RELEASE(m_tables_built);

            auto visit_wrapper = [](auto ptr_MAT, table_id_type t_id, const VisitorArgs &...args) {
                std::uniform_int_distribution<uint64_t> table_item_dist(0, ptr_MAT->m_local_alias_tables[t_id].size()-1);
                table_item tbl_itm = ptr_MAT->m_local_alias_tables[t_id][table_item_dist(ptr_MAT->m_rng)];
                item_id_type s;
                if (tbl_itm.p == 1) {
                    s = tbl_itm.a;
                } else {
                    std::uniform_real_distribution<float> zero_one_dist(0.0, 1.0);
                    float f = zero_one_dist(ptr_MAT->m_rng);
                    if (f < tbl_itm.p) {
                        s = tbl_itm.a;
                    } else {
                        s = tbl_itm.b;
                    }
                }
                Visitor *vis = nullptr;
                ygm::meta::apply_optional(*vis, std::make_tuple(ptr_MAT), std::forward_as_tuple(s, t_id, args...));
            };

            uint32_t dest_rank = m_rank_dist(m_rng);
            m_comm.async(dest_rank, visit_wrapper, pthis, table_id, std::forward<const VisitorArgs>(args)...);
        }        

    private:
        bool                                                m_balanced;
        bool                                                m_tables_built;
        ygm::comm&                                          m_comm;
        ygm::ygm_ptr<self_type>                             pthis;
        std::uniform_int_distribution<uint32_t>             m_rank_dist;
        RNG                                                 m_rng;
        std::unordered_map<table_id_type, std::vector<item>>          m_local_items;
        std::unordered_map<table_id_type, std::vector<table_item>>    m_local_alias_tables;
        std::map<table_id_type, double>                     m_each_tables_local_weight; 
};

}  // namespace ygm::random