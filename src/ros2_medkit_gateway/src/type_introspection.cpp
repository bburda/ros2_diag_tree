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

#include "ros2_medkit_gateway/type_introspection.hpp"

#include <regex>
#include <sstream>
#include <stdexcept>

#include "ros2_medkit_gateway/output_parser.hpp"
#include "ros2_medkit_gateway/ros2_cli_wrapper.hpp"

namespace ros2_medkit_gateway {

TypeIntrospection::TypeIntrospection(const std::string & scripts_path)
  : scripts_path_(scripts_path)
  , cli_wrapper_(std::make_unique<ROS2CLIWrapper>())
  , output_parser_(std::make_unique<OutputParser>()) {
}

TopicMetadata TypeIntrospection::get_topic_metadata(const std::string & topic_path) {
  TopicMetadata metadata;
  metadata.topic_path = topic_path;

  // Call: ros2 topic info {topic}
  std::string cmd = "ros2 topic info " + ROS2CLIWrapper::escape_shell_arg(topic_path);
  std::string output = cli_wrapper_->exec(cmd);

  // Parse output:
  // Type: sensor_msgs/msg/Temperature
  // Publisher count: 1
  // Subscription count: 0

  // Extract type
  std::regex type_regex(R"(Type:\s*(\S+))");
  std::smatch type_match;
  if (std::regex_search(output, type_match, type_regex)) {
    metadata.type_name = type_match[1].str();
  }

  // Extract publisher count
  std::regex pub_regex(R"(Publisher count:\s*(\d+))");
  std::smatch pub_match;
  if (std::regex_search(output, pub_match, pub_regex)) {
    metadata.publisher_count = std::stoi(pub_match[1].str());
  }

  // Extract subscription count
  std::regex sub_regex(R"(Subscription count:\s*(\d+))");
  std::smatch sub_match;
  if (std::regex_search(output, sub_match, sub_regex)) {
    metadata.subscriber_count = std::stoi(sub_match[1].str());
  }

  // Validate that we got the critical type_name field
  if (metadata.type_name.empty()) {
    throw std::runtime_error("Failed to parse type name from topic info for: " + topic_path);
  }

  return metadata;
}

nlohmann::json TypeIntrospection::get_type_template(const std::string & type_name) {
  // Call: ros2 interface proto {type_name}
  std::string cmd = "ros2 interface proto " + ROS2CLIWrapper::escape_shell_arg(type_name);
  std::string output = cli_wrapper_->exec(cmd);

  // The output is a quoted YAML string, e.g.:
  // "header:\n  stamp:\n    sec: 0\n..."
  // We need to remove the surrounding quotes and parse as YAML

  // Remove surrounding quotes if present
  if (output.size() >= 2 && output.front() == '"' && output.back() == '"') {
    output = output.substr(1, output.size() - 2);
  }

  // Unescape common escape sequences (\n, \t, \r, \\, \")
  std::string unescaped;
  for (size_t i = 0; i < output.size(); ++i) {
    if (i + 1 < output.size() && output[i] == '\\') {
      char next = output[i + 1];
      switch (next) {
        case 'n':
          unescaped += '\n';
          ++i;
          break;
        case 't':
          unescaped += '\t';
          ++i;
          break;
        case 'r':
          unescaped += '\r';
          ++i;
          break;
        case '\\':
          unescaped += '\\';
          ++i;
          break;
        case '"':
          unescaped += '"';
          ++i;
          break;
        default:
          unescaped += output[i];
          break;
      }
    } else {
      unescaped += output[i];
    }
  }

  return output_parser_->parse_yaml(unescaped);
}

nlohmann::json TypeIntrospection::get_type_schema(const std::string & type_name) {
  // Call Python script for recursive schema
  if (scripts_path_.empty()) {
    throw std::runtime_error("scripts_path not configured for TypeIntrospection");
  }

  std::string script_path = "python3 " + ROS2CLIWrapper::escape_shell_arg(scripts_path_ + "/get_type_schema.py") + " " +
                            ROS2CLIWrapper::escape_shell_arg(type_name);

  std::string output = cli_wrapper_->exec(script_path);

  try {
    nlohmann::json result = nlohmann::json::parse(output);

    // Check for error in result
    if (result.contains("error")) {
      throw std::runtime_error(result["error"].get<std::string>());
    }

    return result;
  } catch (const nlohmann::json::parse_error & e) {
    throw std::runtime_error("Failed to parse schema output: " + std::string(e.what()));
  }
}

TopicTypeInfo TypeIntrospection::get_type_info(const std::string & type_name) {
  // Check cache first
  {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    auto it = type_cache_.find(type_name);
    if (it != type_cache_.end()) {
      return it->second;
    }
  }

  // Build type info
  TopicTypeInfo info;
  info.name = type_name;

  // Get template (default values)
  try {
    info.default_value = get_type_template(type_name);
  } catch (const std::exception & e) {
    // If template fails, use empty object
    info.default_value = nlohmann::json::object();
  }

  // Get schema (recursive type info)
  try {
    nlohmann::json schema_result = get_type_schema(type_name);
    if (schema_result.contains("schema")) {
      info.schema = schema_result["schema"];
    } else {
      info.schema = nlohmann::json::object();
    }
  } catch (const std::exception & e) {
    // If schema fails, use empty object
    info.schema = nlohmann::json::object();
  }

  // Cache it (use try_emplace to avoid overwriting if another thread added it first)
  {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    auto [it, inserted] = type_cache_.try_emplace(type_name, info);
    return it->second;  // Return cached version (may be from another thread)
  }
}

void TypeIntrospection::clear_cache() {
  std::lock_guard<std::mutex> lock(cache_mutex_);
  type_cache_.clear();
}

}  // namespace ros2_medkit_gateway
