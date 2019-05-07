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
#include <sstream>

#include "subprocess.h"
static char buff[100000];

namespace Nuke{
inline std::vector<std::string> split(const std::string & s, const std::string & splitter){
    std::vector<std::string> res;
    size_t start = 0;
    size_t p;
    while((p = s.find(splitter, start)) != std::string::npos){
        res.push_back(std::string(s.begin()+start, s.begin()+p));
        start = p + splitter.size();
    }
    res.push_back(std::string(s.begin()+start, s.end()));
    if(res.size() == 1 && res[0] == ""){
        return {};
    }
    return res;
}
};

inline std::string read_until(int fd, char term = '\n'){
    char b[10];
    std::string s;
    while(1){
        read(fd, b, 1);
        s += b[0];
        if(b[0] == '\n'){
            break;
        }
    }
    return s;
}

struct PartitionStateNuft : public PartitionState{
protected:
    PartitionConfig config;
    Map<V, Vertex> verts;
    std::set<Edge> edges;
    std::mutex mut;
    std::set<Edge>::iterator cursor;
    int ei = 0;
    subprocess::Popen * proc;
    // FILE * pwrite;
    // FILE * fread;
public:
    std::set<Edge> get_edges() const{
        return edges;
    }
    int edges_size() const{
        return edges.size();
    }
    Map<V, Vertex> get_verts(){
        return verts;
    }
    Map<V, Vertex> get_verts(const Set<V> & vs){
        Map<V, Vertex> res;
        for(auto v : vs){
            if(verts.find(v) == verts.end()){
                verts[v] = Vertex();
            }
            res[v] = verts[v];
        }
        return res;
    }
    bool is_crashed(){
        return false;
    }
    std::vector<Partition> get_parts(){
        std::lock_guard<std::mutex> guard((mut));
        std::vector<Partition> res;
        res.resize(config.k);
        for(P i = 0; i < config.k; i++){
            sprintf(buff, "SGET P%lld\n", i);

            fprintf(proc->input(), "%s\n", buff);
            fflush(proc->input());
            auto ans = read_until(fileno(proc->output()), '\n');
            // printf("At get_parts(%lld) fread says %s\n", i, ans.c_str());
            printf("At get_parts(%lld) fread\n", i);
            std::vector<std::string> ess = Nuke::split(ans, ";");
            // printf("Split Res %d\n", ess.size());
            for(auto && es: ess){
                LL u, v;
                if(~sscanf(es.c_str(), "%lld,%lld", &u, &v)){
                    res[i].add_edge(Edge{u, v});
                }
            }
        }
        return res;
    }
    void put_verts(const Map<V, Vertex> & delta){
        for(auto && pr: delta){
            verts[pr.first].deg.fetch_add(pr.second.delta_deg);
            for(auto p : pr.second.parts){
                verts[pr.first].parts.insert(p);
            }
        }
    }
    void put_part(P i, const Partition & delta_part){
        std::lock_guard<std::mutex> guard((mut));
        std::string s;
        s += "'";
        int flag = 0;
        if(!delta_part.edges.size()){
            // printf("No edges, put_part() return\n");
            return;
        }
        for(auto && edge: delta_part.edges){
            if(flag){
                sprintf(buff, ";%lld,%lld", edge.u, edge.v);
            }else{
                flag = 1;
                sprintf(buff, "%lld,%lld", edge.u, edge.v);
            }
            s += buff;
        }
        s += "'";
        sprintf(buff, "SADD P%lld %s\n", i, s.c_str());

        // printf("At put_part() write %u bytes: \n", strlen(buff));
        fprintf(proc->input(), buff);
        auto ans = read_until(fileno(proc->output()), '\n');
        assert(ans == "OK\n");
    }
    void put_parts(const std::vector<Partition> & delta){
        assert(delta.size() == config.k);
        for(P i = 0; i < delta.size(); i++){
            put_part(i, delta[i]);
        }
    }
    void recover(std::lock_guard<std::mutex> & guard, const std::vector<Partition> & parts){
        assert(false);
    }
    void crash(std::lock_guard<std::mutex> & guard){
        assert(false);
    }
    PartitionStateNuft(PartitionConfig c) : config(c){
        config.state = this;
        FILE * f;
        f = std::fopen(config.dataset.c_str(), "r");
        LL u, v;
        while(~fscanf(f, "%lld %lld\n", &u, &v)){
            if(u != v){
                edges.insert(Edge{u, v});
                verts[u] = Vertex();
                verts[v] = Vertex();
            }
        }
        std::fclose(f);
        printf("Edges %u Vertexs %u\n", edges.size(), verts.size());
        cursor = edges.begin();

        // pwrite = popen("./kv", "w");
        // fread = fopen("temp.swap", "r");
        proc = new subprocess::Popen({"./kv"}, subprocess::input{subprocess::PIPE}, subprocess::output{subprocess::PIPE}, subprocess::error{stderr});
    }
    ~PartitionStateNuft(){
        proc->kill();
        delete proc;
    }
    Edge get_edge(bool & valid){
        std::lock_guard<std::mutex> guard((mut));
        if(cursor != edges.end()){
            valid = 1;
            ei ++;
            return *(cursor++);
        }else{
            printf("ei: %d\n", ei);
            valid = 0;
            return Edge{0, 0};
        }
    }

};