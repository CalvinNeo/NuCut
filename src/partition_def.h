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
#include <chrono>

#define COMPUTE_OVERHEAD

typedef long long LL;
typedef LL E; // Index for edge
typedef LL V; // Index for vertex
typedef LL P; // Index for partition
template <typename K, typename V>
using Map = std::map<K, V>;
template <typename T>
using Set = std::set<T>;

inline uint64_t get_current_ms(){
    using namespace std::chrono;
    time_point<system_clock, milliseconds> timepoint_now = time_point_cast<milliseconds>(system_clock::now());;
    auto tmp = duration_cast<milliseconds>(timepoint_now.time_since_epoch());  
    std::time_t timestamp = tmp.count();  
    return (uint64_t)timestamp;  
}

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
    std::string to_string() const{
        return std::to_string(u) + " " + std::to_string(v);
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
    bool contains(const Edge & e){
        return edges.find(e) != edges.end();
    }
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
    virtual void recover(std::lock_guard<std::mutex> & guard, const std::vector<Partition> & parts) = 0;
    virtual void crash(std::lock_guard<std::mutex> & guard) = 0;
    virtual bool is_crashed() = 0;
    virtual Edge get_edge(bool & valid) = 0;
    virtual ~PartitionState(){

    }    
    void check_crashed(){
        using namespace std::chrono_literals;
        while(is_crashed()){
            // printf("CRASHED\n");
            // std::this_thread::sleep_for(10ms);   
        }
    }
};

template<typename T>
void update_max(std::atomic<T>& mv, T const& value) noexcept
{
    T prev_value = mv;
    while(prev_value < value &&
            !mv.compare_exchange_weak(prev_value, value))
        ;
}
template<typename T>
void update_min(std::atomic<T>& mv, T const& value) noexcept
{
    T prev_value = mv;
    while(prev_value > value &&
            !mv.compare_exchange_weak(prev_value, value))
        ;
}

struct DebugStruct{
    std::atomic<int> total_e;
    std::atomic<int> useful_e;
    FILE * f;
    std::atomic<uint64_t> max_t;
    std::atomic<uint64_t> min_t;
    DebugStruct(){
        max_t.store(0);
        min_t.store(999999999);
    }
};

typedef std::function<P(const Vertex & u, const Vertex & v, const std::vector<Partition> & parts)> HF;

struct PartitionConfig {
    int k; // How many partitions
    int window;
    int subp; // How many subpartitions
    std::string dataset;
    PartitionState * state;
    DebugStruct * ds;
    bool lazy_load = false;
    HF hf;
    int crash_mode = 0;
};
