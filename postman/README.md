# ROS 2 Medkit Gateway Postman Collection

This folder contains Postman collections for testing the ROS 2 Medkit Gateway REST API.

## API Base Path

All endpoints are prefixed with `/api/v1` for API versioning.

## Collection Contents

**Collection:** `collections/ros2-medkit-gateway.postman_collection.json`

### Discovery Endpoints
- ✅ GET `/api/v1/` - Server capabilities and entry points
- ✅ GET `/api/v1/version-info` - Gateway status and version
- ✅ GET `/api/v1/areas` - List all areas
- ✅ GET `/api/v1/components` - List all components with operations and type schemas
- ✅ GET `/api/v1/areas/{area_id}/components` - List components in specific area

### Component Data Endpoints
- ✅ GET `/api/v1/components/{component_id}/data` - Read all topic data from a component
- ✅ GET `/api/v1/components/{component_id}/data/{topic_path}` - Read specific topic data
- ✅ PUT `/api/v1/components/{component_id}/data/{topic_path}` - Publish to a topic

### Operations Endpoints (Services & Actions)
- ✅ POST `/api/v1/components/{component_id}/operations/{operation}` - Call service or send action goal
- ✅ GET `/api/v1/components/{component_id}/operations/{operation}/status` - Get action status
- ✅ DELETE `/api/v1/components/{component_id}/operations/{operation}?goal_id=...` - Cancel action

### Configurations Endpoints (ROS 2 Parameters)
- ✅ GET `/api/v1/components/{component_id}/configurations` - List all parameters
- ✅ GET `/api/v1/components/{component_id}/configurations/{param}` - Get parameter value
- ✅ PUT `/api/v1/components/{component_id}/configurations/{param}` - Set parameter value
- ❌ DELETE `/api/v1/components/{component_id}/configurations/{param}` - Not supported (405)

## Quick Start

### 1. Import Collection

**In Postman Desktop:**
1. Click **Import** button (top-left)
2. Select: `postman/collections/ros2-medkit-gateway.postman_collection.json`
3. Click **Import**

### 2. Import Environment

1. Click **Environments** icon (left sidebar)
2. Click **Import**
3. Select: `postman/environments/local.postman_environment.json`
4. Activate environment: Select **"ROS 2 Medkit Gateway - Local"** from dropdown (top-right)

### 3. Start Gateway & Demo Nodes

```bash
# Terminal 1 - Demo Nodes (sensors, actuators, services, actions)
ros2 launch ros2_medkit_gateway demo_nodes.launch.py

# Terminal 2 - Gateway
ros2 launch ros2_medkit_gateway gateway.launch.py
```

### 4. Test Endpoints

**Discovery:**
1. Expand **"Discovery"** folder
2. Click **"GET Server Capabilities"** → **Send**
3. Click **"GET List Components"** → **Send** (shows all components with operations)

**Component Data:**
1. Expand **"Component Data"** folder
2. Click **"GET Component Data (All Topics)"** → **Send**
3. Click **"PUT Publish Brake Command"** → **Send** (publishes 50.0 bar to brake actuator)

**Operations:**
1. Expand **"Operations" → "Sync Operations (Services)"**
2. Click **"POST Call Calibrate Service"** → **Send** (calls std_srvs/srv/Trigger)

1. Expand **"Operations" → "Async Operations (Actions)"**
2. Click **"POST Send Action Goal (Long Calibration)"** → **Send**
3. Copy the `goal_id` from response
4. Click **"GET Action Status (Latest)"** → **Send** (shows executing/succeeded)

**Configurations:**
1. Expand **"Configurations"** folder
2. Click **"GET List Component Configurations"** → **Send**
3. Click **"PUT Set Configuration (publish_rate)"** → **Send** (changes temp_sensor rate)

## URL Encoding for Topics

Topic paths use standard percent-encoding (`%2F` for `/`):

| Topic Path | URL Encoding |
|-----------|--------------|
| `/powertrain/engine/temperature` | `powertrain%2Fengine%2Ftemperature` |
| `/chassis/brakes/command` | `chassis%2Fbrakes%2Fcommand` |

Example: `GET /api/v1/components/temp_sensor/data/powertrain%2Fengine%2Ftemperature`

## Environment Variables

The environment includes:
- `base_url`: `http://localhost:8080/api/v1` (default gateway address with API prefix)
- `goal_id`: Used for action status queries (set manually after sending a goal)

---

**Happy Testing! 🚀**
