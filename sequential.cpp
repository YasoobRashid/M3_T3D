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

int main() {
    std::ifstream file("traffic_data.txt");
    if (!file) {
        std::cerr << "Error opening file!\n";
        return 1;
    }

    std::vector<TrafficData> all_data;
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

    std::map<int, std::map<int, int>> hour_light_cars;

    for (auto& d : all_data) {
        int hour = d.timestamp / 60;
        hour_light_cars[hour][d.light_id] += d.cars;
    }

    int N = 3; // Top N lights
    for (auto& hour_entry : hour_light_cars) {
        std::vector<std::pair<int, int>> lights(hour_entry.second.begin(), hour_entry.second.end());
        std::sort(lights.begin(), lights.end(), [](auto& a, auto& b) {
            return a.second > b.second;
        });

        std::cout << "Hour: " << hour_entry.first << "\n";
        for (int i = 0; i < std::min(N, (int)lights.size()); ++i) {
            std::cout << "Light TL" << lights[i].first << ": " << lights[i].second << " cars\n";
        }
        std::cout << "\n";
    }

    return 0;
}
