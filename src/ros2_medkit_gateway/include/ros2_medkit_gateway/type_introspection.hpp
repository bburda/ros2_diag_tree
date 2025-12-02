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

#pragma once

#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>

namespace ros2_medkit_gateway {

// Forward declarations
class ROS2CLIWrapper;
class OutputParser;

/**
 * @brief Information about a ROS 2 message type including schema and template
 */
struct TopicTypeInfo {
  std::string name;              ///< Full type name (e.g., "sensor_msgs/msg/Temperature")
  nlohmann::json schema;         ///< Recursive field type information
  nlohmann::json default_value;  ///< Template with default values
};

/**
 * @brief Metadata about a topic (without actual data)
 */
struct TopicMetadata {
  std::string topic_path;   ///< Full topic path (e.g., "/powertrain/engine/temperature")
  std::string type_name;    ///< Message type name
  int publisher_count{0};   ///< Number of publishers
  int subscriber_count{0};  ///< Number of subscribers
};

/**
 * @brief Provides type introspection capabilities for ROS 2 message types
 *
 * This class allows querying information about ROS 2 topics and message types
 * without requiring actual message data. It uses CLI commands and a Python
 * helper script to gather type information.
 *
 * Type information is cached to avoid repeated CLI calls for the same types.
 */
class TypeIntrospection {
 public:
  /**
   * @brief Construct a new TypeIntrospection object
   *
   * @param scripts_path Path to the directory containing helper scripts
   */
  explicit TypeIntrospection(const std::string & scripts_path = "");

  ~TypeIntrospection() = default;

  // Disable copy (due to mutex and unique_ptrs)
  TypeIntrospection(const TypeIntrospection &) = delete;
  TypeIntrospection & operator=(const TypeIntrospection &) = delete;

  // Disable move (mutex is not movable)
  TypeIntrospection(TypeIntrospection &&) = delete;
  TypeIntrospection & operator=(TypeIntrospection &&) = delete;

  /**
   * @brief Get metadata for a topic (type, publisher/subscriber counts)
   *
   * This method is fast and doesn't require the topic to be publishing.
   *
   * @param topic_path Full path to the topic (e.g., "/powertrain/engine/temperature")
   * @return TopicMetadata containing topic information
   * @throws std::runtime_error if topic info cannot be retrieved
   */
  TopicMetadata get_topic_metadata(const std::string & topic_path);

  /**
   * @brief Get full type information including schema (with caching)
   *
   * @param type_name Full type name (e.g., "sensor_msgs/msg/Temperature")
   * @return TopicTypeInfo containing schema and default values
   * @throws std::runtime_error if type info cannot be retrieved
   */
  TopicTypeInfo get_type_info(const std::string & type_name);

  /**
   * @brief Get the default value template for a message type
   *
   * Uses `ros2 interface proto` to generate a template with default values.
   *
   * @param type_name Full type name
   * @return nlohmann::json Template with default field values
   * @throws std::runtime_error if template cannot be generated
   */
  nlohmann::json get_type_template(const std::string & type_name);

  /**
   * @brief Get the JSON schema for a message type
   *
   * Uses a Python helper script to generate recursive type schema.
   *
   * @param type_name Full type name
   * @return nlohmann::json Schema with field types
   * @throws std::runtime_error if schema cannot be generated
   */
  nlohmann::json get_type_schema(const std::string & type_name);

  /**
   * @brief Clear the type info cache
   */
  void clear_cache();

 private:
  std::string scripts_path_;                     ///< Path to helper scripts
  std::unique_ptr<ROS2CLIWrapper> cli_wrapper_;  ///< CLI wrapper for commands
  std::unique_ptr<OutputParser> output_parser_;  ///< Parser for CLI output

  std::unordered_map<std::string, TopicTypeInfo> type_cache_;  ///< Cache for type info
  mutable std::mutex cache_mutex_;                             ///< Mutex for thread-safe cache access
};

}  // namespace ros2_medkit_gateway
