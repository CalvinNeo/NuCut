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
            redisReply * reply = command(scan_cursor);
            scan_cursor = atoi(reply->element[0]->str);
            if(reply->element[1]->elements == 0){

            }else{
                for(int j = 0; j < reply->element[1]->elements; j++){
                    cache.push_back(reply->element[1]->element[j]->str);
                }
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
    std::mutex mut;
    redisContext * conn;
    LL esize = 0, vsize = 0;
    HiRedisScanner scanner;
    LL ei = 0;
public:
    int edges_size() const{
        redisReply * reply = redisCommand(conn, "SCARD E");
        int ans = reply->integer;
        freeReplyObject(reply);
        return ans;
    }
    int verts_size() const{
        redisReply * reply = redisCommand(conn, "SCARD V");
        int ans = reply->integer;
        freeReplyObject(reply);
        return ans;
    }
    Map<V, Vertex> get_verts(const Set<V> & vs){
        Map<V, Vertex> res;
        for(auto v : vs){
            res[v] = Vertex{};

            redisReply * reply;
            reply = redisCommand(conn, "SMEMBERS VP%lld", v);
            // All partitions related to v
            for(int j = 0; j < reply->elements; j++){
                char * pstr = reply->element[j]->str;
                res[v].add_part(std::atoi(pstr));
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
    Map<V, Vertex> get_verts(){
        Set<V> req;
        redisReply * reply = redisCommand(conn, "SMEMBERS V");
        for(int j = 0; j < reply->elements; j++){
            LL v;
            sscanf(reply->element[j]->str, "%lld", &v);
            req.insert(v);
        }
        auto ans = get_verts(req);
        freeReplyObject(reply);
        return ans;
    }
    std::vector<Partition> get_parts(){
        redisReply * reply;
        std::vector<Partition> res;
        res.resize(config.k);
        for(int i = 0; i < config.k; i++){
            redisReply * reply = redisCommand(conn, "SCARD P%lld", i);
            int ans = reply->integer;
            freeReplyObject(reply);
            reply = redisCommand(conn, "SMEMBERS P%lld", i);
            for(int j = 0; j < reply->elements; j++){
                LL u, v;
                sscanf(reply->element[j]->str, "%lld,%lld", &u, &v);
                res[i].add_edge(Edge{u, v});
            }
            freeReplyObject(reply);
        }
        return res;
    }
    void put_verts(const Map<V, Vertex> & delta){
        for(auto && pr: delta){
            redisReply * reply;
            // Increase v's degree
            reply = redisCommand(conn, "INCRBY VD%lld %lld", pr.first, pr.second.delta_deg);
            freeReplyObject(reply);
            for(auto p : pr.second.parts){
                reply = redisCommand(conn, "SADD VP%lld %lld", pr.first, p);
                freeReplyObject(reply);
            }
        }
    }
    void put_part(P i, const Partition & delta_part){
        for(auto && edge: delta_part.edges){
            redisReply * reply;
            reply = redisCommand(conn, "SADD P%lld %lld,%lld", i, edge.u, edge.v);
            freeReplyObject(reply);
        }
    }
    void put_parts(const std::vector<Partition> & delta){
        assert(delta.size() == config.k);
        for(int i = 0; i < delta.size(); i++){
            put_part(i, delta[i]);
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
        // std::lock_guard<std::mutex> guard((mut));
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
