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

#include <algorithm>
#include <chrono>
#include <cmath>
#include <future>
#include <sstream>

#include <ament_index_cpp/get_package_share_directory.hpp>

#include "ros2_medkit_gateway/exceptions.hpp"

namespace ros2_medkit_gateway {

DataAccessManager::DataAccessManager(rclcpp::Node * node)
  : node_(node)
  , cli_wrapper_(std::make_unique<ROS2CLIWrapper>())
  , output_parser_(std::make_unique<OutputParser>())
  , type_introspection_(std::make_unique<TypeIntrospection>(
        ament_index_cpp::get_package_share_directory("ros2_medkit_gateway") + "/scripts"))
  , max_parallel_samples_(node->declare_parameter<int>("max_parallel_topic_samples", 10)) {
  // Validate max_parallel_samples_ against allowed range [1, 50]
  if (max_parallel_samples_ < 1 || max_parallel_samples_ > 50) {
    RCLCPP_WARN(node_->get_logger(), "max_parallel_topic_samples (%d) out of valid range (1-50), using default: 10",
                max_parallel_samples_);
    max_parallel_samples_ = 10;
  }

  if (!cli_wrapper_->is_command_available("ros2")) {
    RCLCPP_ERROR(node_->get_logger(), "ROS 2 CLI not found!");
    throw CommandNotAvailableException("ros2");
  }

  RCLCPP_INFO(node_->get_logger(), "DataAccessManager initialized (CLI-based, max_parallel_samples=%d)",
              max_parallel_samples_);
}

json DataAccessManager::get_topic_sample(const std::string & topic_name, double timeout_sec) {
  try {
    // TODO(mfaferek93): Check timeout command availability
    // GNU coreutils 'timeout' may not be available on all systems (BSD, containers)
    // Should check in constructor or provide fallback mechanism
    std::ostringstream cmd;
    cmd << "timeout " << static_cast<int>(std::ceil(timeout_sec)) << "s " << "ros2 topic echo "
        << ROS2CLIWrapper::escape_shell_arg(topic_name) << " --once --no-arr";

    RCLCPP_INFO(node_->get_logger(), "Executing: %s", cmd.str().c_str());

    std::string output = cli_wrapper_->exec(cmd.str());

    // Check for warning messages in raw output (before parsing)
    // ROS 2 CLI prints warnings as text, not structured YAML
    if (output.find("WARNING") != std::string::npos) {
      throw TopicNotAvailableException(topic_name);
    }

    json data = output_parser_->parse_yaml(output);

    // Check for empty/null parsed data
    if (data.is_null()) {
      throw TopicNotAvailableException(topic_name);
    }

    json result = {{"topic", topic_name},
                   {"timestamp", std::chrono::duration_cast<std::chrono::nanoseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count()},
                   {"data", data}};

    return result;
  } catch (const TopicNotAvailableException &) {
    // Re-throw TopicNotAvailableException as-is for proper handling upstream
    throw;
  } catch (const std::exception & e) {
    RCLCPP_ERROR(node_->get_logger(), "Failed to get sample from topic '%s': %s", topic_name.c_str(), e.what());

    // For other errors (CLI failures, parsing errors), wrap as TopicNotAvailableException
    throw TopicNotAvailableException(topic_name);
  }
}

std::vector<std::string> DataAccessManager::find_component_topics(const std::string & component_namespace) {
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
      if (!line.empty() && line[0] == '/' && line.find(component_namespace) == 0) {
        size_t ns_len = component_namespace.length();
        if (line.length() == ns_len || line[ns_len] == '/') {
          topics.push_back(line);
        }
      }
    }

    RCLCPP_INFO(node_->get_logger(), "Found %zu topics under namespace '%s'", topics.size(),
                component_namespace.c_str());
  } catch (const std::exception & e) {
    RCLCPP_ERROR(node_->get_logger(), "Failed to list topics for namespace '%s': %s", component_namespace.c_str(),
                 e.what());
  }

  return topics;
}

json DataAccessManager::get_component_data(const std::string & component_namespace, double timeout_sec) {
  json result = json::array();

  // Find all topics under this namespace
  auto topics = find_component_topics(component_namespace);

  if (topics.empty()) {
    RCLCPP_WARN(node_->get_logger(), "No topics found under namespace '%s'", component_namespace.c_str());
    return result;
  }

  // Get data from each topic using batched parallel sampling
  // Process topics in batches to limit concurrent operations
  RCLCPP_INFO(node_->get_logger(), "Sampling %zu topics in parallel (batch size: %d)", topics.size(),
              max_parallel_samples_);

  for (size_t i = 0; i < topics.size(); i += max_parallel_samples_) {
    // Calculate batch size (handle last batch which may be smaller)
    size_t batch_size = std::min(static_cast<size_t>(max_parallel_samples_), topics.size() - i);

    // Launch async tasks for this batch
    std::vector<std::future<json>> futures;
    futures.reserve(batch_size);

    for (size_t j = 0; j < batch_size; ++j) {
      const auto & topic = topics[i + j];
      futures.push_back(std::async(std::launch::async, [this, topic, timeout_sec]() -> json {
        return get_topic_sample(topic, timeout_sec);
      }));
    }

    // Collect results from this batch
    for (size_t j = 0; j < batch_size; ++j) {
      const auto & topic = topics[i + j];
      try {
        json topic_data = futures[j].get();
        result.push_back(topic_data);
      } catch (const std::exception & e) {
        RCLCPP_WARN(node_->get_logger(), "Failed to get data from topic '%s': %s", topic.c_str(), e.what());
        // Continue with other topics
      }
    }
  }

  return result;
}

json DataAccessManager::publish_to_topic(const std::string & topic_path, const std::string & msg_type,
                                         const json & data, double timeout_sec) {
  try {
    // Convert JSON data to string for ros2 topic pub
    // Note: data.dump() produces JSON format, but ROS 2 CLI accepts JSON
    // as valid YAML (JSON is a subset of YAML 1.2)
    std::string yaml_data = data.dump();

    // TODO(mfaferek93) #32: Check timeout command availability
    // GNU coreutils 'timeout' may not be available on all systems (BSD, containers)
    // Should check in constructor or provide fallback mechanism
    std::ostringstream cmd;
    cmd << "timeout " << static_cast<int>(std::ceil(timeout_sec)) << "s " << "ros2 topic pub --once -w 0 "
        << ROS2CLIWrapper::escape_shell_arg(topic_path) << " " << ROS2CLIWrapper::escape_shell_arg(msg_type) << " "
        << ROS2CLIWrapper::escape_shell_arg(yaml_data);

    RCLCPP_INFO(node_->get_logger(), "Executing: %s", cmd.str().c_str());

    std::string output = cli_wrapper_->exec(cmd.str());

    RCLCPP_INFO(node_->get_logger(), "Published to topic '%s' with type '%s'", topic_path.c_str(), msg_type.c_str());

    json result = {{"topic", topic_path},
                   {"type", msg_type},
                   {"status", "published"},
                   {"timestamp", std::chrono::duration_cast<std::chrono::nanoseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count()}};

    return result;
  } catch (const std::exception & e) {
    RCLCPP_ERROR(node_->get_logger(), "Failed to publish to topic '%s': %s", topic_path.c_str(), e.what());
    throw std::runtime_error("Failed to publish to topic '" + topic_path + "': " + e.what());
  }
}

json DataAccessManager::get_topic_sample_with_fallback(const std::string & topic_name, double timeout_sec) {
  json data_result;
  bool has_data = false;

  // First, try to get actual data
  try {
    data_result = get_topic_sample(topic_name, timeout_sec);
    has_data = true;
  } catch (const TopicNotAvailableException &) {
    RCLCPP_INFO(node_->get_logger(), "Topic '%s' data unavailable, returning metadata", topic_name.c_str());
  }

  // Try to enrich with type information (for both data and metadata-only cases)
  try {
    TopicMetadata metadata = type_introspection_->get_topic_metadata(topic_name);
    TopicTypeInfo type_info = type_introspection_->get_type_info(metadata.type_name);

    if (has_data) {
      data_result["status"] = "data";
      data_result["type"] = metadata.type_name;
      data_result["type_info"] = {{"schema", type_info.schema}, {"default_value", type_info.default_value}};
      data_result["publisher_count"] = metadata.publisher_count;
      data_result["subscriber_count"] = metadata.subscriber_count;
      return data_result;
    } else {
      json result = {{"topic", topic_name},
                     {"status", "metadata_only"},
                     {"type", metadata.type_name},
                     {"type_info", {{"schema", type_info.schema}, {"default_value", type_info.default_value}}},
                     {"publisher_count", metadata.publisher_count},
                     {"subscriber_count", metadata.subscriber_count},
                     {"timestamp", std::chrono::duration_cast<std::chrono::nanoseconds>(
                                       std::chrono::system_clock::now().time_since_epoch())
                                       .count()}};
      return result;
    }
  } catch (const std::exception & e) {
    RCLCPP_ERROR(node_->get_logger(), "Failed to get metadata for topic '%s': %s", topic_name.c_str(), e.what());
    // If we had data but metadata failed, return data without enrichment
    if (has_data) {
      data_result["status"] = "data";
      return data_result;
    }
    throw TopicNotAvailableException(topic_name);
  }
}

json DataAccessManager::get_component_data_with_fallback(const std::string & component_namespace, double timeout_sec) {
  json result = json::array();

  auto topics = find_component_topics(component_namespace);

  if (topics.empty()) {
    RCLCPP_WARN(node_->get_logger(), "No topics found under namespace '%s'", component_namespace.c_str());
    return result;
  }

  RCLCPP_INFO(node_->get_logger(), "Sampling %zu topics with fallback (batch size: %d)", topics.size(),
              max_parallel_samples_);

  for (size_t i = 0; i < topics.size(); i += max_parallel_samples_) {
    size_t batch_size = std::min(static_cast<size_t>(max_parallel_samples_), topics.size() - i);

    std::vector<std::future<json>> futures;
    futures.reserve(batch_size);

    for (size_t j = 0; j < batch_size; ++j) {
      const auto & topic = topics[i + j];
      futures.push_back(std::async(std::launch::async, [this, topic, timeout_sec]() -> json {
        return get_topic_sample_with_fallback(topic, timeout_sec);
      }));
    }

    for (size_t j = 0; j < batch_size; ++j) {
      const auto & topic = topics[i + j];
      try {
        json topic_data = futures[j].get();
        result.push_back(topic_data);
      } catch (const std::exception & e) {
        RCLCPP_WARN(node_->get_logger(), "Failed to get data/metadata from topic '%s': %s", topic.c_str(), e.what());
      }
    }
  }

  return result;
}

}  // namespace ros2_medkit_gateway
