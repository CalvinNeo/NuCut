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

inline std::vector<double> evaluate_partition_greedy(const Vertex & u, const Vertex & v, const std::vector<Partition> & parts){
    int max_size = std::max_element(parts.begin(), parts.end(), [](const Partition & p1, const Partition & p2){ return p1.edges.size() < p2.edges.size();})->edges.size();
    int min_size = std::min_element(parts.begin(), parts.end(), [](const Partition & p1, const Partition & p2){ return p1.edges.size() < p2.edges.size();})->edges.size();
    int n = parts.size();
    printf("In all %u parts: max_size %u, min_size %u\n", parts.size(), max_size, min_size);
    assert(u.deg.load() > 0);
    assert(v.deg.load() > 0);
    double d1 = u.deg.load(), d2 = v.deg.load();

    double epsilon = 1.0;
    double lambda = 1.0;

    auto compute_replication_score = [&](P p) -> double {
        double sr = 0.0;
        if(u.parts.find(p) != u.parts.end()){
            sr += 1;
        }
        if(v.parts.find(p) != v.parts.end()){
            sr += 1;
        }
        return sr;
    };

    auto compute_balance_score = [&](P p) -> double {
        int es = parts[p].edges.size();
        // es++, score--
        return lambda * (max_size - es) / (epsilon + max_size - min_size);
    };

    std::vector<double> ans;
    ans.resize(n);

    for(P i = 0; i < n; i++){
        double rep_score = compute_replication_score(i);
        double bal_score = compute_balance_score(i);
        double score = rep_score + bal_score;
        // printf("score[%d] = %lf + %lf = %lf\n", i, rep_score, bal_score, score);
        ans[i] = score;
    }

    return ans;
}


inline std::vector<double> evaluate_partition_hdrf(const Vertex & u, const Vertex & v, const std::vector<Partition> & parts){
    // Use heuristic to predict.
    // HDRF
    int max_size = std::max_element(parts.begin(), parts.end(), [](const Partition & p1, const Partition & p2){ return p1.edges.size() < p2.edges.size();})->edges.size();
    int min_size = std::min_element(parts.begin(), parts.end(), [](const Partition & p1, const Partition & p2){ return p1.edges.size() < p2.edges.size();})->edges.size();
    int n = parts.size();
    printf("In all %u parts: max_size %u, min_size %u\n", parts.size(), max_size, min_size);
    assert(u.deg.load() > 0);
    assert(v.deg.load() > 0);
    double d1 = u.deg.load(), d2 = v.deg.load();

    double lambda = 1.0;
    double epsilon = 1.0;
    double theta1 = d1 / (d1 + d2);
    double theta2 = 1 - theta1;

    auto g = [&](P p, Vertex x, double theta) -> double {
        if(x.parts.find(p) == x.parts.end()){
            return 0;
        }
        return 1 + (1 - theta);
    };

    auto compute_replication_score = [&](P p) -> double {
        return g(p, u, theta1) + g(p, v, theta2);
    };

    auto compute_balance_score = [&](P p) -> double {
        int es = parts[p].edges.size();
        return lambda * (max_size - es) / (epsilon + max_size - min_size);
    };

    std::vector<double> ans;
    ans.resize(n);

    for(P i = 0; i < n; i++){
        double rep_score = compute_replication_score(i);
        double bal_score = compute_balance_score(i);
        double score = rep_score + bal_score;
        // printf("score[%d] = %lf + %lf = %lf\n", i, rep_score, bal_score, score);
        ans[i] = score;
    }

    return ans;
}

template<typename F>
inline P select_partition_with(F f, const Vertex & u, const Vertex & v, const std::vector<Partition> & parts){
    auto ans = f(u, v, parts);
    int max_part = std::max_element(ans.begin(), ans.end(), std::less<double>()) - ans.begin();
    printf("max_part %d\n", max_part);
    return max_part;
}

inline P select_partition_with_hrdf(const Vertex & u, const Vertex & v, const std::vector<Partition> & parts){
    return select_partition_with(evaluate_partition_hdrf, u, v, parts);
}

inline P select_partition_with_greedy(const Vertex & u, const Vertex & v, const std::vector<Partition> & parts){
    return select_partition_with(evaluate_partition_greedy, u, v, parts);
}

inline P select_partition_with_mixed(const Vertex & u, const Vertex & v, const std::vector<Partition> & parts){
    auto ans1 = evaluate_partition_hdrf(u, v, parts);
    auto ans2 = evaluate_partition_greedy(u, v, parts);
    int n = ans1.size();

    std::vector<double> ans;
    ans.resize(n);
    for(int i = 0; i < n; i++){
        ans[i] = (ans2[i] + ans1[i]) / 2.0;
    }
    int max_part = std::max_element(ans.begin(), ans.end(), std::less<double>()) - ans.begin();
    printf("max_part %d\n", max_part);
    return max_part;
}