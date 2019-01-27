#include "partition.h"

inline P select_partition_greedy(const Vertex & u, const Vertex & v, const std::vector<Partition> & parts){
    double max_score = std::numeric_limits<float>::lowest();
    P max_part = -1;
    int max_size = std::max_element(parts.begin(), parts.end(), [](const Partition & p1, const Partition & p2){ return p1.edges.size() < p2.edges.size();})->edges.size();
    int min_size = std::min_element(parts.begin(), parts.end(), [](const Partition & p1, const Partition & p2){ return p1.edges.size() < p2.edges.size();})->edges.size();
    printf("In all %u parts: max_size %u, min_size %u\n", parts.size(), max_size, min_size);
    int n = parts.size();
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

    for(P i = 0; i < n; i++){
        double rep_score = compute_replication_score(i);
        double bal_score = compute_balance_score(i);
        double score = rep_score + bal_score;
        printf("score[%d] = %lf + %lf = %lf\n", i, rep_score, bal_score, score);
        if(score > max_score){
            max_score = score;
            max_part = i;
        }
    }

    printf("max_part %d\n", max_part);
    return max_part;
}

inline P select_partition_hdrf(const Vertex & u, const Vertex & v, const std::vector<Partition> & parts){
    // Use heuristic to predict.
    // HDRF
    double max_score = std::numeric_limits<float>::lowest();
    P max_part = -1;
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

    for(P i = 0; i < n; i++){
        double rep_score = compute_replication_score(i);
        double bal_score = compute_balance_score(i);
        double score = rep_score + bal_score;
        printf("score[%d] = %lf + %lf = %lf\n", i, rep_score, bal_score, score);
        if(score > max_score){
            max_score = score;
            max_part = i;
        }
    }

    printf("max_part %d\n", max_part);
    return max_part;
}