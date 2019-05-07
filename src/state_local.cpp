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
#include "state_local.h"
#include "state_nuft.h"

PartitionStateLocal::PartitionStateLocal(PartitionConfig c) : config(c){
    config.state = this;
    f = std::fopen(config.dataset.c_str(), "r");
    if(config.lazy_load){
        init_bloom();
    }else{
        strict_load();
    }
    printf("Edges %u Vertexs %u\n", edges.size(), verts.size());
    cursor = edges.begin();
    parts.resize(config.k);
    if(config.crash_mode != 0){
        pstate_nuft = new PartitionStateNuft(config);
    }
}

PartitionStateLocal::~PartitionStateLocal(){
    delete bfilter;
    std::fclose(f);
    if(config.crash_mode != 0){
        delete pstate_nuft;
    }
}


void PartitionStateLocal::put_part(std::lock_guard<std::mutex> & guard, P i, const Partition & delta_part){
    // check_crashed();
    for(auto && edge: delta_part.edges){
        parts[i].add_edge(edge);
    }
    if(config.crash_mode != 0){
        pstate_nuft->put_part(i, delta_part);
    }
}

bool PartitionStateLocal::is_crashed(){
    return crashed;
}

void PartitionStateLocal::put_parts(const std::vector<Partition> & delta){
    // check_crashed();
    std::lock_guard<std::mutex> guard((mut));
    assert(delta.size() == parts.size());
    for(P i = 0; i < delta.size(); i++){
        put_part(guard, i, delta[i]);
    }    
    if(config.crash_mode != 0){
        pstate_nuft->put_parts(delta);
    }
}

int all_saved_edges(PartitionConfig config){
    int tot = 0;
    auto parts = config.state->get_parts();
    for(auto && p: parts){
        tot += p.edges.size();
    }
    return tot;
}

Edge PartitionStateLocal::get_edge(bool & valid){
    std::lock_guard<std::mutex> guard((mut));
    if(config.lazy_load){
        LL u, v;
        REP:
        if(~fscanf(f, "%lld %lld\n", &u, &v)){
            if(is_repeated(Edge{u, v})){
                goto REP;
            }else{
                valid = 1;
                ei ++;
                edges.insert(Edge{u, v});
                return Edge{u, v};
            }
        }
        printf("ei: %d\n", ei);
        valid = 0;
        return Edge{0, 0};
    }else{
        if(config.crash_mode == 2){
            if(ei == 2000){
                int X = all_saved_edges(config);
                printf("Test crash all edge is %d\n", X);
                fprintf(config.ds->f, "Test crash all edge is %d\n", X);
                for(auto && p: parts){
                    fprintf(config.ds->f, "TP %d\n", p.edges.size());
                    printf("TP %d\n", p.edges.size());
                }
                crash(guard);
                printf("Finish crash\n");
                fprintf(config.ds->f, "Finish crash\n");
                uint64_t start_time = get_current_ms();
                std::vector<Partition> p = pstate_nuft->get_parts();
                printf("get parts from p %d\n", p.size());
                fprintf(config.ds->f, "get parts from p %d\n", p.size());
                recover(guard, p);
                uint64_t end_time = get_current_ms();
                X = all_saved_edges(config);
                printf("Finish recover all edge is %d\n", X);
                fprintf(config.ds->f, "Finish recover all edge is %d\n", X);
                for(auto && p: parts){
                    fprintf(config.ds->f, "TP %d\n", p.edges.size());
                    printf("TP %d\n", p.edges.size());
                }
                printf("Recover Elapsed %llu\n", end_time - start_time);
                fprintf(config.ds->f, "Recover Elapsed %llu\n", end_time - start_time);
            }
        }
        if(cursor != edges.end()){
            valid = 1;
            ei ++;
            return *(cursor++);
        }else{
            printf("At the end, ei: %d\n", ei);
            valid = 0;
            return Edge{0, 0};
        }
    }
}