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
#include "bloom_filter.hpp"
#include <sstream>
#include <chrono>

struct PartitionStateLocal : public PartitionState{
protected:
    PartitionConfig config;
    Map<V, Vertex> verts;
    std::vector<Partition> parts;
    std::set<Edge> edges;
    mutable std::mutex mut;
    std::set<Edge>::iterator cursor;
    int ei = 0;
    bloom_filter * bfilter = nullptr;
    FILE * f;
    struct PartitionStateNuft * pstate_nuft;
    bool crashed = false;
public:
    void init_bloom(){
        bloom_parameters parameters;
        parameters.projected_element_count = 10000;
        parameters.false_positive_probability = 0.0001; // 1 in 10000
        parameters.random_seed = 0xA5A5A5A5;
        parameters.compute_optimal_parameters();
        bfilter = new bloom_filter(parameters);
    }

    bool is_crashed();
    int edges_size() const{
        // check_crashed();
        return edges.size();
    }
    std::set<Edge> get_edges() const{
        // check_crashed();
        return edges;
    }
    Map<V, Vertex> get_verts(){
        // check_crashed();
        return verts;
    }
    Map<V, Vertex> get_verts(const Set<V> & vs){
        // check_crashed();
        Map<V, Vertex> res;
        for(auto v : vs){
            if(verts.find(v) == verts.end()){
                verts[v] = Vertex();
            }
            res[v] = verts[v];
        }
        return res;
    }
    std::vector<Partition> get_parts(){
        // NOTICE We should provide a copy rather than a reference. To avoid sync problems.
        // check_crashed();
        return parts;
    }
    void put_verts(const Map<V, Vertex> & delta){
        // check_crashed();
        std::lock_guard<std::mutex> guard((mut));
        for(auto && pr: delta){
            verts[pr.first].deg.fetch_add(pr.second.delta_deg);
            for(auto p : pr.second.parts){
                verts[pr.first].parts.insert(p);
            }
        }
    }
    void put_part(std::lock_guard<std::mutex> & guard, P i, const Partition & delta_part);
    void put_parts(const std::vector<Partition> & delta);
    void put_part(P i, const Partition & delta_part){
        std::lock_guard<std::mutex> guard((mut));
        put_part(guard, i, delta_part); 
    }

    void strict_load(){
        LL u, v;
        while(~fscanf(f, "%lld %lld\n", &u, &v)){
            if(u != v){
                Edge e = Edge{u, v};
                edges.insert(e);
                verts[u] = Vertex();
                verts[v] = Vertex();
            }
        }
    }
    void recover(std::lock_guard<std::mutex> & guard, const std::vector<Partition> & old_parts){
        parts = old_parts;
        cursor = edges.begin();
        for(int i = 0; i < parts.size(); i++){
            const Partition & p = parts[i];
            for(int jj = 0; jj < p.edges.size(); jj++){
                cursor ++;
            }
            for(const Edge & e: p.edges){
                if(verts.find(e.v) == verts.end()){
                    verts[e.v] = Vertex();
                }
                verts[e.v].deg.fetch_add(1);
                verts[e.v].parts.insert(i);
                if(verts.find(e.u) == verts.end()){
                    verts[e.u] = Vertex();
                }
                verts[e.u].deg.fetch_add(1);
                verts[e.u].parts.insert(i);
            }
        }
        crashed = false;
    }
    void crash(std::lock_guard<std::mutex> & guard){
        crashed = true;
        parts.clear();
        verts.clear();
    }
    bool is_repeated(const Edge & e){
        bool con = bfilter->contains(e.to_string());
        bfilter->insert(e.to_string());
        if(con){
            // Test FP
            for(const Partition & p: parts){
                if(p.contains(e)){
                    return true;
                }
            }
            return false;
        }else{
            return false;
        }
    }
    PartitionStateLocal(PartitionConfig c);
    ~PartitionStateLocal();
    Edge get_edge(bool & valid);
};


struct PartitionStateTemplate : public PartitionState{
protected:
    PartitionConfig config;
    std::mutex mut;
public:
    int edges_size() const{

    }
    int verts_size() const{

    }
    Map<V, Vertex> get_verts(const Set<V> & vs){

    }
    Map<V, Vertex> get_verts(){

    }
    std::vector<Partition> get_parts(){

    }
    void put_verts(const Map<V, Vertex> & delta){

    }
    void put_part(P i, const Partition & delta_part){

    }
    void put_parts(const std::vector<Partition> & delta){

    }

    PartitionStateTemplate(PartitionConfig c) : config(c){

    }
    ~PartitionStateTemplate() override{
    }
    Edge get_edge(bool & valid){

    }
};
