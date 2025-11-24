// Copyright 2025 mfaferek93
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ros2_medkit_gateway/data_access_manager.hpp"
#include <chrono>
#include <sstream>
#include <cmath>

namespace ros2_medkit_gateway {

DataAccessManager::DataAccessManager(rclcpp::Node* node)
    : node_(node),
      cli_wrapper_(std::make_unique<ROS2CLIWrapper>()),
      output_parser_(std::make_unique<OutputParser>())
{
    if (!cli_wrapper_->is_command_available("ros2")) {
        RCLCPP_ERROR(node_->get_logger(), "ROS 2 CLI not found!");
        throw std::runtime_error("ros2 command not available");
    }

    RCLCPP_INFO(node_->get_logger(), "DataAccessManager initialized (CLI-based)");
}

json DataAccessManager::get_topic_sample(
    const std::string& topic_name,
    double timeout_sec
) {
    try {
        // TODO(mfaferek93): Check timeout command availability
        // GNU coreutils 'timeout' may not be available on all systems (BSD, containers)
        // Should check in constructor or provide fallback mechanism
        std::ostringstream cmd;
        cmd << "timeout " << static_cast<int>(std::ceil(timeout_sec)) << "s "
            << "ros2 topic echo " << ROS2CLIWrapper::escape_shell_arg(topic_name)
            << " --once --no-arr";

        RCLCPP_INFO(node_->get_logger(), "Executing: %s", cmd.str().c_str());

        std::string output = cli_wrapper_->exec(cmd.str());

        // Check for warning messages in raw output (before parsing)
        // ROS 2 CLI prints warnings as text, not structured YAML
        if (output.find("WARNING") != std::string::npos) {
            throw std::runtime_error("Topic not available or timeout");
        }

        json data = output_parser_->parse_yaml(output);

        // Check for empty/null parsed data
        if (data.is_null()) {
            throw std::runtime_error("Topic not available or timeout");
        }

        json result = {
            {"topic", topic_name},
            {"timestamp", std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count()},
            {"data", data}
        };

        return result;
    } catch (const std::exception& e) {
        RCLCPP_ERROR(node_->get_logger(),
                    "Failed to get sample from topic '%s': %s",
                    topic_name.c_str(),
                    e.what());

        throw std::runtime_error(
            "Failed to get sample from topic '" + topic_name + "': " + std::string(e.what())
        );
    }
}

std::vector<std::string> DataAccessManager::find_component_topics(
    const std::string& component_namespace
) {
    std::vector<std::string> topics;

    try {
        std::string cmd = "ros2 topic list";
        std::string output = cli_wrapper_->exec(cmd);

        // Parse output line by line
        std::istringstream stream(output);
        std::string line;

        while (std::getline(stream, line)) {
            // Remove whitespace
            line.erase(0, line.find_first_not_of(" \t\r\n"));
            line.erase(line.find_last_not_of(" \t\r\n") + 1);

            // Check if topic starts with component namespace
            // and is followed by '/' or end of string
            if (!line.empty() && line[0] == '/' &&
                line.find(component_namespace) == 0) {
                size_t ns_len = component_namespace.length();
                if (line.length() == ns_len || line[ns_len] == '/') {
                    topics.push_back(line);
                }
            }
        }

        RCLCPP_INFO(node_->get_logger(),
                    "Found %zu topics under namespace '%s'",
                    topics.size(),
                    component_namespace.c_str());
    } catch (const std::exception& e) {
        RCLCPP_ERROR(node_->get_logger(),
                    "Failed to list topics for namespace '%s': %s",
                    component_namespace.c_str(),
                    e.what());
    }

    return topics;
}

json DataAccessManager::get_component_data(
    const std::string& component_namespace,
    double timeout_sec
) {
    json result = json::array();

    // Find all topics under this namespace
    auto topics = find_component_topics(component_namespace);

    if (topics.empty()) {
        RCLCPP_WARN(node_->get_logger(),
                   "No topics found under namespace '%s'",
                   component_namespace.c_str());
        return result;
    }

    // Get data from each topic
    // TODO(mfaferek93): Implement parallel topic sampling to improve performance
    // Current sequential approach: N topics × timeout = long response time
    // Future: Use threads/async to sample topics in parallel
    for (const auto& topic : topics) {
        try {
            json topic_data = get_topic_sample(topic, timeout_sec);
            result.push_back(topic_data);
        } catch (const std::exception& e) {
            RCLCPP_WARN(node_->get_logger(),
                       "Failed to get data from topic '%s': %s",
                       topic.c_str(),
                       e.what());
            // Continue with other topics
        }
    }

    return result;
}

}  // namespace ros2_medkit_gateway
