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
#include "partition_def.h"

struct Subpartitioner{
    PartitionConfig config;
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
        uint64_t start_time = get_current_ms();
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
            P p = config.hf(u, v, parts);
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
        uint64_t end_time = get_current_ms();
        #if defined(COMPUTE_OVERHEAD)
            int pk = 0;
            for(P i = 0; i < parts.size(); i++){
                pk += parts[i].edges.size();
                config.ds->total_e.fetch_add(pk);
            }
            config.ds->useful_e.fetch_add(window.size());
            fprintf(config.ds->f, "%d %d %llu\n", pk, window.size(), end_time - start_time);
            update_max(config.ds->max_t, end_time - start_time);
            update_min(config.ds->min_t, end_time - start_time);
        #endif
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


struct MajorPartitionerBase{
public:
    PartitionConfig config;

    MajorPartitionerBase(PartitionConfig c): config(c){
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
            fprintf(config.ds->f, "Partition[%d] edge size %u vertex size %u\n", i, parts[i].edges.size(), parts[i].get_verts().size());
            for(auto && e: parts[i].edges){
                if(edges_loc.find(e) != edges_loc.end()){
                    printf("Duplicate edge [%lld, %lld], prev %lld, current %lld\n", e.u, e.v, edges_loc[e], i);
                    fprintf(config.ds->f, "Duplicate edge [%lld, %lld], prev %lld, current %lld\n", e.u, e.v, edges_loc[e], i);
                }
                if(all_edges.find(e) == all_edges.end()){
                    printf("Invalid edge [%lld, %lld], current %lld\n", e.u, e.v, i);
                    fprintf(config.ds->f, "Invalid edge [%lld, %lld], current %lld\n", e.u, e.v, i);
                }
                edges_loc[e] = i;
            }
        }
        for (auto && e: all_edges)
        {
            bool ex = false;
            for(int i = 0; i < config.k; i++){
                if(parts[i].edges.find(e) == parts[i].edges.end()){
                    continue;
                }else{
                    ex = true;
                    break;
                }
            }
            if(!ex){
                printf("Missing edge [%lld, %lld]\n", e.u, e.v);
                fprintf(config.ds->f, "Missing edge [%lld, %lld]\n", e.u, e.v);
            }
        }
        printf("Total edge %d, edges in partition %d\n", config.state->edges_size(), tote);
        fprintf(config.ds->f, "Total edge %d, edges in partition %d\n", config.state->edges_size(), tote);
        
        printf("Max Time %d, Min Time %d D %d\n", config.ds->max_t.load(), config.ds->min_t.load(), 
            config.ds->max_t.load() - config.ds->min_t.load());
        fprintf(config.ds->f, "Max Time %d, Min Time %d D %d\n", config.ds->max_t.load(), 
            config.ds->min_t.load(), config.ds->max_t.load() - config.ds->min_t.load());
        
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

struct MajorPartitioner : public MajorPartitionerBase{
    Subpartitioner * subs;

    ~MajorPartitioner(){
        delete [] subs;
    }
    MajorPartitioner(PartitionConfig c): MajorPartitionerBase(c){
    }
    virtual void run() override{
        this->config.ds->total_e.store(0);
        this->config.ds->useful_e.store(0);
        subs = new Subpartitioner[this->config.subp];
        for(int i = 0; i < this->config.subp; i++){
            subs[i].config = this->config;
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
        printf("total_e %d, useful_e %d\n", this->config.ds->total_e.load(), this->config.ds->useful_e.load());
    }
};

