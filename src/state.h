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

#include "partition.h"
#include <sstream>

struct PartitionStateLocal : public PartitionState{
protected:
    PartitionConfig config;
    Map<V, Vertex> verts;
    std::vector<Partition> parts;
    std::set<Edge> edges;
    std::mutex mut;
    std::set<Edge>::iterator cursor;
    int ei = 0;
public:
    int edges_size() const{
        return edges.size();
    }
    std::set<Edge> get_edges() const{
        return edges;
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
    std::vector<Partition> get_parts(){
        // NOTICE We should provide a copy rather than a reference. To avoid sync problems.
        return parts;
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
        for(auto && edge: delta_part.edges){
            parts[i].add_edge(edge);
        }
    }
    void put_parts(const std::vector<Partition> & delta){
        assert(delta.size() == parts.size());
        for(P i = 0; i < delta.size(); i++){
            put_part(i, delta[i]);
        }
    }

    PartitionStateLocal(PartitionConfig c) : config(c){
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
        parts.resize(config.k);
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

#include <hiredis/hiredis.h>

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

struct HiRedisScanner{
    // See https://redis.io/commands/scan
    std::vector<std::string> cache;
    int scan_cursor = -1;
    std::function<redisReply*(int)> command;

    bool has_next(){
        return !(scan_cursor == 0);
    }
    std::string get(bool & valid){
        while(!cache.size()){
            if(scan_cursor == 0){
                valid = false;
                return "";
            }else if(scan_cursor == -1){
                scan_cursor = 0;
            }
            // redisCommand(conn, "SSCAN E %d", cur)
            redisReply * reply = command(scan_cursor);
            if(reply->type == REDIS_REPLY_ARRAY){
                if(reply->elements == 0){
                    freeReplyObject(reply);
                    continue;
                }
                scan_cursor = atoi(reply->element[0]->str);
                if(reply->element[1]->elements == 0){

                }else{
                    for(int j = 0; j < reply->element[1]->elements; j++){
                        cache.push_back(reply->element[1]->element[j]->str);
                    }
                }
            }else if(reply->type == REDIS_REPLY_INTEGER){
                scan_cursor = reply->integer;
            }
            freeReplyObject(reply);
        }
        auto res = cache.back();
        cache.pop_back();
        valid = true;
        return res;
    }
    void start_scan(std::function<redisReply*(int)> cmd){
        cache.clear();
        command = cmd;
        scan_cursor = -1;
    }
};

struct PartitionStateRedis : public PartitionState{
protected:
    PartitionConfig config;
    // Hiredis is not thread-safe
    mutable std::mutex mut;
    redisContext * conn;
    LL esize = 0, vsize = 0;
    HiRedisScanner scanner;
    LL ei = 0;
public:
    std::set<Edge> get_edges() const{
        // TODO NOT IMPL!!
        std::lock_guard<std::mutex> guard((mut));
        std::set<Edge> res;
        redisReply * reply = redisCommand(conn, "SMEMBERS E");
        for(int j = 0; j < reply->elements; j++){
            LL u, v;
            sscanf(reply->element[j]->str, "%lld,%lld", &u, &v);
            res.insert(Edge{u, v});
        }
        freeReplyObject(reply);
        return res;
    }
    int edges_size() const{
        std::lock_guard<std::mutex> guard((mut));
        redisReply * reply = redisCommand(conn, "SCARD E");
        int ans = reply->integer;
        freeReplyObject(reply);
        return ans;
    }
    int verts_size() const{
        std::lock_guard<std::mutex> guard((mut));
        redisReply * reply = redisCommand(conn, "SCARD V");
        int ans = reply->integer;
        freeReplyObject(reply);
        return ans;
    }
    Map<V, Vertex> get_verts(std::lock_guard<std::mutex> & guard, const Set<V> & vs){
        Map<V, Vertex> res;
        for(auto v : vs){
            res[v] = Vertex{};

            redisReply * reply;
            reply = redisCommand(conn, "SMEMBERS VP%lld", v);
            // All partitions related to v
            for(int j = 0; j < reply->elements; j++){
                if(reply->element[j]->type == REDIS_REPLY_STRING){
                    char * pstr = reply->element[j]->str;
                    assert(pstr);
                    res[v].add_part(std::atoi(pstr));
                }else{
                    assert(reply->element[j]->type == REDIS_REPLY_ARRAY);
                    for(int j1 = 0; j1 < reply->element[j]->elements; j1++){
                        char * pstr = reply->element[j]->element[j1]->str;
                        assert(pstr);
                        res[v].add_part(std::atoi(pstr));
                    }
                }
            }
            freeReplyObject(reply);
            // v's degree
            reply = redisCommand(conn, "GET VD%lld", v);
            if(reply->str){
                res[v].deg.store(std::atoi(reply->str));
            }else{
                res[v].deg.store(0);
            }
            freeReplyObject(reply);
        }
        return res;
    }
    Map<V, Vertex> get_verts(const Set<V> & vs){
        std::lock_guard<std::mutex> guard((mut));
        return get_verts(guard, vs);
    }
    Map<V, Vertex> get_verts(){
        std::lock_guard<std::mutex> guard((mut));
        Set<V> req;
        redisReply * reply = redisCommand(conn, "SMEMBERS V");
        for(int j = 0; j < reply->elements; j++){
            LL v;
            sscanf(reply->element[j]->str, "%lld", &v);
            req.insert(v);
        }
        auto ans = get_verts(guard, req);
        freeReplyObject(reply);
        return ans;
    }
    std::vector<Partition> get_parts(){
        std::lock_guard<std::mutex> guard((mut));
        redisReply * reply;
        std::vector<Partition> res;
        res.resize(config.k);
        for(P i = 0; i < config.k; i++){
            redisReply * reply = redisCommand(conn, "SCARD P%lld", i);
            int ans = reply->integer;
            freeReplyObject(reply);
            reply = redisCommand(conn, "SMEMBERS P%lld", i);
            for(int j = 0; j < reply->elements; j++){
                if(reply->element[j]->type == REDIS_REPLY_STRING){
                    LL u, v;
                    sscanf(reply->element[j]->str, "%lld,%lld", &u, &v);
                    res[i].add_edge(Edge{u, v});
                }else if(reply->element[j]->type == REDIS_REPLY_ARRAY){
                    for(int j1 = 0; j1 < reply->element[j]->elements; j1++){
                        LL u, v;
                        sscanf(reply->element[j]->element[j1]->str, "%lld,%lld", &u, &v);
                        res[i].add_edge(Edge{u, v});
                    }
                }
            }
            freeReplyObject(reply);
        }
        return res;
    }
    void put_verts(const Map<V, Vertex> & delta){
        std::lock_guard<std::mutex> guard((mut));
        for(auto && pr: delta){
            redisReply * reply;
            // Increase v's degree
            reply = redisCommand(conn, "INCRBY VD%lld %lld", pr.first, pr.second.delta_deg);
            freeReplyObject(reply);
            for(auto p : pr.second.parts){
                // Update V's partition
                reply = redisCommand(conn, "SADD VP%lld %lld", pr.first, p);
                freeReplyObject(reply);
            }
        }
    }
    void put_part(std::lock_guard<std::mutex> & guard, P i, const Partition & delta_part){
        for(auto && edge: delta_part.edges){
            redisReply * reply;
            reply = redisCommand(conn, "SADD P%lld %lld,%lld", i, edge.u, edge.v);
            freeReplyObject(reply);
        }
    }
    void put_part(P i, const Partition & delta_part){
        std::lock_guard<std::mutex> guard((mut));
        put_part(guard, i, delta_part);
    }
    void put_parts(const std::vector<Partition> & delta){
        std::lock_guard<std::mutex> guard((mut));
        assert(delta.size() == config.k);
        for(int i = 0; i < delta.size(); i++){
            put_part(guard, i, delta[i]);
        }
    }

    PartitionStateRedis(PartitionConfig c) : config(c){
        config.state = this;
        conn = redisConnect("127.0.0.1", 6379);
        assert(!(conn == NULL && conn->err));
        freeReplyObject(redisCommand(conn, "FLUSHALL"));
        
        FILE * f;
        f = std::fopen(config.dataset.c_str(), "r");
        LL u, v;
        redisReply * reply;
        while(~fscanf(f, "%lld %lld\n", &u, &v)){
            if(u != v){
                if(u > v){
                    std::swap(u, v);
                }
                reply = redisCommand(conn, "SADD E %lld,%lld", u, v);
                freeReplyObject(reply);
                reply = redisCommand(conn, "SADD V %lld", u);
                freeReplyObject(reply);
                reply = redisCommand(conn, "SADD V %lld", v);
                freeReplyObject(reply);
            }
        }

        esize = edges_size();
        vsize = verts_size();
        printf("Edges %u Vertexs %u\n", esize, vsize);

        scanner.start_scan([&](int cur) -> redisReply*{
            return redisCommand(conn, "SSCAN E %d", cur);
        });
    }
    ~PartitionStateRedis() override{
        redisFree(conn);
    }
    Edge get_edge(bool & valid){
        std::lock_guard<std::mutex> guard((mut));
        LL u, v;
        auto r = scanner.get(valid);
        if(valid){
            ei ++;
            sscanf(r.c_str(), "%lld,%lld", &u, &v);
            return Edge{u, v};
        }else{
            return Edge{0, 0};
        }
    }
};


#include "subprocess.h"
char buff[100000];

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

std::string read_until(int fd, char term = '\n'){
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
    std::vector<Partition> get_parts(){
        std::lock_guard<std::mutex> guard((mut));
        std::vector<Partition> res;
        res.resize(config.k);
        for(P i = 0; i < config.k; i++){
            sprintf(buff, "SGET P%lld\n", i);
            // proc->send(buff, strlen(buff));
            // fprintf(proc->input(), buff);
            // fflush(proc->input());
            // std::string req = buff;
            // auto ans = proc->communicate(buff, strlen(buff)).first;
            // fscanf(proc->output(), "%s", buff);
            // printf("DATA '%s' = '%s'\n", req.c_str(), buff);


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
        // auto ans = proc->communicate(buff, strlen(buff)).first;
        // fprintf(pwrite, buff);
        // fscanf(fread, "%s\n", buff);
        // assert(strlen(buff) > 0);

        // printf("At put_part() write %u bytes %s\n", strlen(buff), buff);
        printf("At put_part() write %u bytes\n", strlen(buff));
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
