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
#include "partition_async.h"
#include "heuristic.h"
#include "state.h"

inline uint64_t get_current_ms(){
    using namespace std::chrono;
    time_point<system_clock, milliseconds> timepoint_now = time_point_cast<milliseconds>(system_clock::now());;
    auto tmp = duration_cast<milliseconds>(timepoint_now.time_since_epoch());  
    std::time_t timestamp = tmp.count();  
    return (uint64_t)timestamp;  
}

int test_origin(){
    PartitionConfig config{10, 100, 3, "dataset/tmin.txt", nullptr};
    // PartitionState * state = new PartitionStateLocal(config);
    PartitionState * state = new PartitionStateRedis(config);
    // PartitionState * state = new PartitionStateNuft(config);
    config.state = state;
    MajorPartitioner<decltype(select_partition_with_hrdf)> pa{config, select_partition_with_hrdf};
    uint64_t start_time = get_current_ms();
    pa.run();
    pa.join();
    uint64_t end_time = get_current_ms();
    pa.assess();
    printf("replicate_factor %lf, load_relative_stddev %lf elapsed %llu\n", pa.replicate_factor, pa.load_relative_stddev, end_time - start_time);
    delete state;
}

int test_enhance(){
    PartitionConfig config{10, 100, 3, "dataset/tmin.txt", nullptr};
    PartitionState * state = new PartitionStateRedis(config);
    config.state = state;
    MajorPartitionerAsync<decltype(select_partition_with_hrdf)> pa{config, select_partition_with_hrdf};
    uint64_t start_time = get_current_ms();
    pa.run();
    pa.join();
    uint64_t end_time = get_current_ms();
    pa.assess();
    printf("replicate_factor %lf, load_relative_stddev %lf elapsed %llu\n", pa.replicate_factor, pa.load_relative_stddev, end_time - start_time);
    delete state;
}

int main(){
    test_enhance();
    // test_origin();
}