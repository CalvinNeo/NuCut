#include "partition.h"
#include "heuristic.h"
#include "state.h"

int main(){
    PartitionConfig config{10, 100, 1, "dataset/tmin.txt", nullptr};
    // PartitionState * state = new PartitionStateLocal(config);
    PartitionState * state = new PartitionStateRedis(config);
    config.state = state;
    MajorPartitioner<decltype(select_partition_greedy)> pa{config, select_partition_greedy};
    pa.run();
    pa.join();
    pa.assess();
    printf("replicate_factor %lf, load_relative_stddev %lf\n", pa.replicate_factor, pa.load_relative_stddev);
    delete state;
}