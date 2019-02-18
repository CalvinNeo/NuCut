/*************************************************************************
*  NuCut -- A streaming graph partitioning framework
*  Copyright (C) 2018  Calvin Neo 
*  Email: calvinneo@calvinneo.com;calvinneo1995@gmail.com
*  Github: https://github.com/CalvinNeo/NuCut/
*  
*  This program is free software: you can redistribute it and/or modify
*  it under the terms of the GNU General Public License as published by
*  the Free Software Foundation, either version 3 of the License, or
*  (at your option) any later version.
*  
*  This program is distributed in the hope that it will be useful,
*  but WITHOUT ANY WARRANTY; without even the implied warranty of
*  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*  GNU General Public License for more details.
*  
*  You should have received a copy of the GNU General Public License
*  along with this program.  If not, see <https://www.gnu.org/licenses/>.
**************************************************************************/

#pragma once

#include <map>
#include <vector>
#include <unordered_map>
#include <set>
#include <unordered_set>
#include <cstring>
#include <cstdlib>
#include <atomic>
#include <thread>
#include <mutex>
#include <limits>
#include <cassert>
#include <algorithm>
#include <queue>
#include <cmath>
#include <condition_variable>


typedef long long LL;
typedef LL E; // Index for edge
typedef LL V; // Index for vertex
typedef LL P; // Index for partition
template <typename K, typename V>
using Map = std::map<K, V>;
template <typename T>
using Set = std::set<T>;

struct Vertex{
    std::atomic<int> deg;
    // Use to sync with shared state by delta
    int delta_deg;
    // TODO Need protecting.
    // All partitions which related to me.
    Set<P> parts;

    void add_part(P p){
        // Add a partition that related to me.
        parts.insert(p);
    }
    Vertex(){
        deg.store(0);
        delta_deg = 0;
    }
    Vertex(const Vertex & r){
        deg.store(r.deg.load());
        delta_deg = r.delta_deg;
        parts = r.parts;
    }
    Vertex & operator=(const Vertex & r){
        if(&r == this){
            return *this;
        }
        deg.store(r.deg.load());
        delta_deg = r.delta_deg;
        parts = r.parts;
        return *this;
    }
};

struct Edge{
    V u;
    V v;
    Edge(V uu, V vv){
        if(uu > vv){
            std::swap(uu, vv);
        }
        u = uu;
        v = vv;
    }
    bool operator==(const Edge & r) const{
        if(&r == this) return true;
        return u == r.u && v == r.v;
    }
    bool operator<(const Edge & r) const {
        if(u == r.u) {
            return v < r.v;
        }
        return u < r.u;
    }
};

struct Partition{
    void add_edge(const Edge & e){
        // NOTICE This function should be idempotent.
        edges.insert(e);
    }
    Set<V> get_verts(){
        Set<V> verts;
        for(const Edge & edge: edges){
            verts.insert(edge.u);
            verts.insert(edge.v);
        }
        return verts;
    }
    Set<Edge> edges;
};


struct PartitionState{
    virtual std::set<Edge> get_edges() const = 0;
    virtual int edges_size() const = 0;
    virtual Map<V, Vertex> get_verts() = 0;
    virtual Map<V, Vertex> get_verts(const Set<V> & vs) = 0;
    virtual std::vector<Partition> get_parts() = 0;
    virtual void put_verts(const Map<V, Vertex> & delta) = 0;
    virtual void put_part(P i, const Partition & delta_part) = 0;
    virtual void put_parts(const std::vector<Partition> & delta) = 0;

    virtual Edge get_edge(bool & valid) = 0;
    virtual ~PartitionState(){

    }
};

struct PartitionConfig {
    int k; // How many partitions
    int window;
    int subp; // How many subpartitions
    std::string dataset;
    PartitionState * state;
};

template<typename HeuristicFunction>
struct Subpartitioner{
    PartitionConfig config;
    HeuristicFunction * select_partition;
    std::thread * ths;

    ~Subpartitioner(){
        if(ths->joinable()){
            ths->join();
        }
        delete ths;
    }

    void main_proc(){
        bool valid = false;
        Set<Edge> window;
        Set<V> vs;
        while(1){
            Edge e = config.state->get_edge(valid);
            if(!valid){
                // No edges
                break;
            }
            window.insert(e);
            vs.insert(e.u);
            vs.insert(e.v);
            if(window.size() % config.window == 0){
                partition_with_window(window, vs);
                window.clear();
                vs.clear();
            }
        }
        if(window.size()){
            partition_with_window(window, vs);
            window.clear();
            vs.clear();
        }
        printf("Thread Finished.\n");
    }

    void partition_with_window(const Set<Edge> & window, const Set<V> & vs){
        // NOTICE We should fetch a copy rather than a reference. To avoid sync problems.
        Map<V, Vertex> verts = config.state->get_verts(vs);
        std::vector<Partition> parts = config.state->get_parts();

        printf("vs.size() = %u, parts.size() = %u.\n", vs.size(), parts.size());
        for(const Edge & e: window){
            Vertex & u = verts[e.u];
            Vertex & v = verts[e.v];
            u.deg.fetch_add(1);
            v.deg.fetch_add(1);
            u.delta_deg++;
            v.delta_deg++;

            printf("Select partition Edge{%lld, %lld}\n", e.u, e.v);
            P p = select_partition(u, v, parts);
            assert(p != -1);
            // If u/v is already related to p, the following stmt changes nothing.
            u.add_part(p);
            v.add_part(p);
            // If e is already related to p, the following stmt changes nothing
            printf("Assign Edge{%lld, %lld} to %lld. Prev size %u\n", e.u, e.v, p, parts[p].edges.size());
            parts[p].add_edge(e);
        }
        // Merge results
        // NOTICE All the changes made(verts and parts) are idempotent,
        // We can just simply merge them.
        config.state->put_verts(verts);
        config.state->put_parts(parts);
        printf("partition_with_window end.\n");
    }

    void run(){
        ths = new std::thread(&Subpartitioner::main_proc, this);
    }

    void join(){
        if(ths->joinable()){
            ths->join();
        }
    }

};



template<typename HeuristicFunction>
struct MajorPartitionerBase{
public:
    PartitionConfig config;
    HeuristicFunction * select_partition;

    MajorPartitionerBase(PartitionConfig c, HeuristicFunction f): config(c), select_partition(f){
    }

    virtual ~MajorPartitionerBase(){}

    double replicate_factor;
    double load_relative_stddev;
    virtual void assess(){
        int tote = 0, totv = 0;
        std::vector<Partition> parts = config.state->get_parts();
        std::map<Edge, int> edges_loc;
        std::set<Edge> all_edges = config.state->get_edges();
        for(int i = 0; i < config.k; i++){
            tote += parts[i].edges.size();
            totv += parts[i].get_verts().size();
            printf("Partition[%d] edge size %u vertex size %u\n", i, parts[i].edges.size(), parts[i].get_verts().size());
            for(auto && e: parts[i].edges){
                if(edges_loc.find(e) != edges_loc.end()){
                    printf("Duplicate edge [%lld,%lld], prev %lld, current %lld\n", e.u, e.v, edges_loc[e], i);
                }
                if(all_edges.find(e) == all_edges.end()){
                    printf("Not-included edge [%lld,%lld], current %lld\n", e.u, e.v, i);
                }
                edges_loc[e] = i;
            }
        }
        printf("Total edge %d, edges in partition %d\n", config.state->edges_size(), tote);
        
        int total_replica = 0;
        Map<V, Vertex> verts = config.state->get_verts();
        for(const auto & pr: verts){
            const Vertex & vert = pr.second;
            total_replica += vert.parts.size();
        }
        replicate_factor = total_replica * 1.0 / verts.size();
        double mean = config.state->edges_size() * 1.0 / config.k;
        double sqr_sum = 0;
        for(Partition & part: parts){
            sqr_sum += std::pow(part.edges.size() - mean, 2);
        }
        load_relative_stddev = std::pow(sqr_sum / (config.k - 1), 0.5) / mean;
    }
    virtual void run() = 0;
    virtual void join() = 0;
};

template<typename HeuristicFunction>
struct MajorPartitioner : public MajorPartitionerBase<HeuristicFunction>{
    Subpartitioner<HeuristicFunction> * subs;

    ~MajorPartitioner(){
        delete [] subs;
    }
    MajorPartitioner(PartitionConfig c, HeuristicFunction f): MajorPartitionerBase<HeuristicFunction>(c, f){
    }
    virtual void run() override{
        subs = new Subpartitioner<HeuristicFunction>[this->config.subp];
        for(int i = 0; i < this->config.subp; i++){
            subs[i].config = this->config;
            subs[i].select_partition = this->select_partition;
        }
        printf("Run\n");
        for(int i = 0; i < this->config.subp; i++){
            subs[i].run();
        }
    }
    virtual void join() override{
        for(int i = 0; i < this->config.subp; i++){
            subs[i].join();
        }
    }
};

