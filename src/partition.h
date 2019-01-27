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
struct MajorPartitioner{
    PartitionConfig config;
    Subpartitioner<HeuristicFunction> * subs;
    HeuristicFunction * select_partition;

    ~MajorPartitioner(){
        delete [] subs;
    }
    MajorPartitioner(PartitionConfig c, HeuristicFunction f): config(c), select_partition(f){
    }
    void run(){
        subs = new Subpartitioner<HeuristicFunction>[config.subp];
        for(int i = 0; i < config.subp; i++){
            subs[i].config = config;
            subs[i].select_partition = select_partition;
        }
        printf("Run\n");
        for(int i = 0; i < config.subp; i++){
            subs[i].run();
        }
    }
    void join(){
        for(int i = 0; i < config.subp; i++){
            subs[i].join();
        }
        for(int i = 0; i < config.k; i++){
            std::vector<Partition> parts = config.state->get_parts();
            printf("Partition[%d] edge size %u vertex size %u\n", i, parts[i].edges.size(), parts[i].get_verts().size());
        }
    }

    double replicate_factor;
    double load_relative_stddev;
    void assess(){
        int total_replica = 0;
        Map<V, Vertex> verts = config.state->get_verts();
        std::vector<Partition> parts = config.state->get_parts();
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
};