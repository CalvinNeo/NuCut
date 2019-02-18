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
#include "partition.h"

template<typename HeuristicFunction>
struct SubpartitionerAsync{
    PartitionConfig config;
    HeuristicFunction * select_partition;
    std::thread * ths;
    std::vector<Partition> parts;
    std::queue<std::pair<P, Edge>> out_queue;
    int acc_window = -1;
    int acc_window_thres_factor = 5;

    ~SubpartitionerAsync(){
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

    // void merge_from_delta(const std::vector<std::vector<Partition>> & delta_p){
    //     for(auto && dp: delta_p){
    //         for(int i = 0; i < config.k; i++){
    //             for(int j = 0; j < dp[i].edges.size(); j++){
    //                 parts[i].add_edge(dp[i].edges[j]);
    //             }
    //         }
    //     }
    // }

    void partition_with_window(const Set<Edge> & window, const Set<V> & vs){
        // NOTICE We should fetch a copy rather than a reference. To avoid sync problems.
        Map<V, Vertex> verts = config.state->get_verts(vs);
        if(acc_window == -1 || acc_window % acc_window_thres_factor == 0){
            acc_window = 0;
            parts = config.state->get_parts();
        }
        acc_window++;

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
            out_queue.push(std::make_pair(p, e));
        }
        // Merge results
        // NOTICE All the changes made(verts and parts) are idempotent,
        // We can just simply merge them.
        config.state->put_verts(verts);
        // We do not put_parts
        // config.state->put_parts(parts);
        printf("partition_with_window end.\n");
    }

    void run(){
        ths = new std::thread(&SubpartitionerAsync::main_proc, this);
    }

    void join(){
        if(ths->joinable()){
            ths->join();
        }
    }

};

template<typename HeuristicFunction>
struct MajorPartitionerAsync : public MajorPartitionerBase<HeuristicFunction>{
    SubpartitionerAsync<HeuristicFunction> * subs;
    std::vector<std::thread *> ths;
    bool stop = false;

    ~MajorPartitionerAsync(){
        delete [] subs;
    }
    MajorPartitionerAsync(PartitionConfig c, HeuristicFunction f): MajorPartitionerBase<HeuristicFunction>(c, f){
    }
    virtual void run() override{
        subs = new SubpartitionerAsync<HeuristicFunction>[this->config.subp];
        ths.resize(this->config.subp);
        for(int i = 0; i < this->config.subp; i++){
            subs[i].config = this->config;
            subs[i].select_partition = this->select_partition;
        }
        for(int i = 0; i < this->config.subp; i++){
            assert(i < this->config.subp);
            ths[i] = new std::thread([this, cid=i, subs=subs](){
                while(!stop){
                    std::vector<Partition> dp;
                    dp.resize(this->config.k);
                    while(subs[cid].out_queue.size()){
                        std::pair<P, Edge> pr = subs[cid].out_queue.front();
                        subs[cid].out_queue.pop();
                        dp[pr.first].add_edge(pr.second);
                    }
                    this->config.state->put_parts(dp);
                }
            });
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
        stop = true;
        for(auto && t: ths){
            if(t->joinable()){
                t->join();
            }
            delete t;
        }
        ths.clear();
    }
};