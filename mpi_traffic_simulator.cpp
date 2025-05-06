#include <mpi.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <algorithm>
#include <sstream>

struct TrafficData {
    int timestamp; // in minutes
    int light_id;
    int cars;
};

int parseTimeToMinutes(const std::string& time_str) {
    try {
        int hour = std::stoi(time_str.substr(0, 2));
        int minute = std::stoi(time_str.substr(3, 2));
        return hour * 60 + minute;
    } catch (...) {
        return -1;
    }
}

int parseLightID(const std::string& id_str) {
    try {
        if (id_str.substr(0, 2) != "TL") return -1;
        return std::stoi(id_str.substr(2));
    } catch (...) {
        return -1;
    }
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int world_rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    std::vector<TrafficData> all_data;

    if (world_rank == 0) {
        std::ifstream file("traffic_data.txt");
        if (!file) {
            std::cerr << "Error opening file!\n";
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        std::string line;
        while (std::getline(file, line)) {
            std::istringstream iss(line);
            std::string time_str, light_str, cars_str;

            if (std::getline(iss, time_str, ',') &&
                std::getline(iss, light_str, ',') &&
                std::getline(iss, cars_str)) {

                TrafficData td;
                td.timestamp = parseTimeToMinutes(time_str);
                td.light_id = parseLightID(light_str);
                try {
                    td.cars = std::stoi(cars_str);
                } catch (...) {
                    td.cars = -1;
                }

                if (td.timestamp >= 0 && td.light_id >= 0 && td.cars >= 0) {
                    all_data.push_back(td);
                } else {
                    std::cerr << "Warning: Skipped invalid line: " << line << "\n";
                }
            }
        }
        file.close();
    }

    int total_records = all_data.size();
    MPI_Bcast(&total_records, 1, MPI_INT, 0, MPI_COMM_WORLD);

    int chunk_size = (total_records + world_size - 1) / world_size;
    std::vector<TrafficData> local_data(chunk_size);

    MPI_Scatter(all_data.data(), chunk_size * sizeof(TrafficData), MPI_BYTE,
                local_data.data(), chunk_size * sizeof(TrafficData), MPI_BYTE,
                0, MPI_COMM_WORLD);

    std::map<int, std::map<int, int>> local_hour_light_cars;
    for (auto& d : local_data) {
        if (d.timestamp == 0 && d.light_id == 0 && d.cars == 0) continue;
        int hour = d.timestamp / 60;
        local_hour_light_cars[hour][d.light_id] += d.cars;
    }

    std::vector<int> send_buffer;
    for (auto& hour_entry : local_hour_light_cars) {
        for (auto& light_entry : hour_entry.second) {
            send_buffer.push_back(hour_entry.first);
            send_buffer.push_back(light_entry.first);
            send_buffer.push_back(light_entry.second);
        }
    }

    int local_size = send_buffer.size();
    std::vector<int> recv_sizes(world_size);
    MPI_Gather(&local_size, 1, MPI_INT, recv_sizes.data(), 1, MPI_INT, 0, MPI_COMM_WORLD);

    std::vector<int> displs(world_size, 0);
    int total_recv = 0;
    if (world_rank == 0) {
        for (int i = 0; i < world_size; ++i) {
            displs[i] = total_recv;
            total_recv += recv_sizes[i];
        }
    }

    std::vector<int> recv_buffer(total_recv);
    MPI_Gatherv(send_buffer.data(), local_size, MPI_INT,
                recv_buffer.data(), recv_sizes.data(), displs.data(), MPI_INT,
                0, MPI_COMM_WORLD);

    if (world_rank == 0) {
        std::map<int, std::map<int, int>> final_hour_light_cars;
        for (int i = 0; i < total_recv; i += 3) {
            int hour = recv_buffer[i];
            int light = recv_buffer[i + 1];
            int cars = recv_buffer[i + 2];
            final_hour_light_cars[hour][light] += cars;
        }

        int N = 3;
        for (auto& hour_entry : final_hour_light_cars) {
            std::vector<std::pair<int, int>> lights(hour_entry.second.begin(), hour_entry.second.end());
            std::sort(lights.begin(), lights.end(), [](auto& a, auto& b) { return a.second > b.second; });
            std::cout << "Hour: " << hour_entry.first << "\n";
            for (int i = 0; i < std::min(N, (int)lights.size()); ++i) {
                std::cout << "Light TL" << lights[i].first << ": " << lights[i].second << " cars\n";
            }
            std::cout << "\n";
        }
    }

    MPI_Finalize();
    return 0;
}
