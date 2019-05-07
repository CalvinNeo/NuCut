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

struct SubpartitionerAsync{
    PartitionConfig config;
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

    void partition_with_window(const Set<Edge> & window, const Set<V> & vs){
        // NOTICE We should fetch a copy rather than a reference. To avoid sync problems.
        Map<V, Vertex> verts = config.state->get_verts(vs);
        if(acc_window == -1 || acc_window % acc_window_thres_factor == 0){
            acc_window = 0;
            parts = config.state->get_parts();
        }
        acc_window++;

        uint64_t start_time = get_current_ms();
        printf("vs.size() = %u, parts.size() = %u.\n", vs.size(), parts.size());
        for(const Edge & e: window){
            Vertex & u = verts[e.u];
            Vertex & v = verts[e.v];
            u.deg.fetch_add(1);
            v.deg.fetch_add(1);
            u.delta_deg++;
            v.delta_deg++;

            printf("---\nSelect partition Edge{%lld, %lld}\n", e.u, e.v);
            P p = config.hf(u, v, parts);
            assert(p != -1);
            // If u/v is already related to p, the following stmt changes nothing.
            u.add_part(p);
            v.add_part(p);
            // If e is already related to p, the following stmt changes nothing
            config.state->check_crashed();
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
        uint64_t end_time = get_current_ms();
        #if defined(COMPUTE_OVERHEAD)
        fprintf(config.ds->f, "%d %d %llu\n", -1, window.size(), end_time - start_time);
        update_max(config.ds->max_t, end_time - start_time);
        update_min(config.ds->min_t, end_time - start_time);
        #endif
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

struct MajorPartitionerAsync : public MajorPartitionerBase{
    SubpartitionerAsync * subs;
    std::vector<std::thread *> thsq;
    // std::thread * tq;
    bool stop = false;

    ~MajorPartitionerAsync(){
        delete [] subs;
    }
    MajorPartitionerAsync(PartitionConfig c): MajorPartitionerBase(c){
    }
    virtual void run() override{
        subs = new SubpartitionerAsync[this->config.subp];
        thsq.resize(this->config.subp);
        for(int i = 0; i < this->config.subp; i++){
            subs[i].config = this->config;
        }
        for(int i = 0; i < this->config.subp; i++){
            assert(i < this->config.subp);
            thsq[i] = new std::thread([this, cid=i, subs=subs](){
                do{
                    std::vector<Partition> dp;
                    dp.resize(this->config.k);
                    while(subs[cid].out_queue.size()){
                        std::pair<P, Edge> pr = subs[cid].out_queue.front();
                        subs[cid].out_queue.pop();
                        // NOTICE Need protect??
                        dp[pr.first].add_edge(pr.second);
                    }
                    this->config.state->check_crashed();
                    this->config.state->put_parts(dp);
                } while(!stop);
            });
        }
        // tq = new std::thread([this, subs=subs](){
        //     do{
        //         std::vector<Partition> dp;
        //         dp.resize(this->config.k);
        //         for(int cid = 0; cid < this->config.subp; cid++){
        //             while(subs[cid].out_queue.size()){
        //                 std::pair<P, Edge> pr = subs[cid].out_queue.front();
        //                 subs[cid].out_queue.pop();
        //                 dp[pr.first].add_edge(pr.second);
        //             }
        //         }
        //         this->config.state->put_parts(dp);
        //     } while(!stop);
        // });
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
        for(auto && t: thsq){
            if(t->joinable()){
                t->join();
            }
            delete t;
        }
        thsq.clear();
        // if(tq->joinable()){
        //     tq->join();
        // }
        // delete tq;
    }
};