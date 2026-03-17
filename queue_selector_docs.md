# RTS2 Queue Selector Documentation

## Overview

The RTS2 Queue Selector (`queue_selector.py`) is a **production-ready** Python replacement for the original C++ `rts2-selector` daemon. It focuses exclusively on **queue execution** and **calibration management**, eliminating the complex target selection algorithms of the original implementation.

**Status**: Production ready as of August 2025. Successfully deployed and tested in operational environments.

## Purpose

The original RTS2 selector attempted to fulfill three conflicting roles simultaneously:
1. Real-time astronomical target selection and optimization
2. Queue management and execution  
3. Calibration scheduling (flats/darks)

This created a complex, hard-to-maintain system with overlapping responsibilities. The new architecture separates these concerns:

- **External Scheduler**: Handles astronomical calculations and optimization (separate service)
- **Queue Selector**: Simple queue executor + calibration manager (this daemon)
- **Executor**: Unchanged telescope/instrument control

## Architecture

### Three-Level Priority System

The selector operates with a clean three-level priority hierarchy:

#### Level 0: External Activity Detection (Pseudo-Queue)
- **Not a real queue** - behavioral conflict avoidance
- Detects when external commands bypass the selector:
  - GRB daemon: `EXEC.grb <target_id>`
  - Manual users: `EXEC.now <target_id>` or `EXEC.next <target_id>`
- **Response**: 20-minute grace period - selector stays quiet to avoid conflicts
- **Implementation**: Monitors executor state, sets grace period when external activity detected

#### Level 1: Manual Queue (queue_id=1)
- **Purpose**: Hard-timed observations that bypass scheduler optimization
- **Use cases**:
  - Star occultations requiring precise timing
  - Complex observations scheduler can't optimize
  - One-off events (asteroid followup, special targets)
  - Testing and calibration sequences
- **Timing**: Executes targets at exactly specified times
- **Database**: `queues_targets` with `queue_id=1`, `time_start` set

#### Level 2: Scheduler Queue (queue_id=2)  
- **Purpose**: Optimized astronomical observing schedule
- **Source**: External scheduler service writes optimized plans
- **Content**: Science targets, scheduled calibrations, surveys
- **Timing**: Executes targets at scheduled times with configurable advance notice
- **Database**: `queues_targets` with `queue_id=2`, timestamps set by scheduler

### Calibration Management

The selector handles automatic calibrations based on RTS2 system state using proper CentralState constants:

- **Calibration Conditions**: `ON + (DUSK or DAWN) + good weather`
  - **DUSK** (`CentralState.ON + CentralState.DUSK`): System transitioning from day to night  
  - **DAWN** (`CentralState.ON + CentralState.DAWN`): System transitioning from night to day
  
- **Calibration Script**: Single script (target ID 2) handles both flats and darks
  - Script internally decides between flats/darks based on actual light conditions
  - Runs automatically when system state indicates twilight periods
  
- **Science Observations**: During `ON + NIGHT + good weather`

- **Priority**: Calibrations override queue execution during twilight system states

**System State Examples**:
- `ON_DUSK`: Calibration script active
- `ON_NIGHT`: Science observations from scheduler queue  
- `ON_DAWN`: Calibration script active
- `STANDBY_DAY`: System in standby, no observations
- `HARD_OFF_*`: System disabled, no observations
- `*_BAD_WEATHER`: Informational only - centrald automatically changes ON→STANDBY when weather is bad

The centrald state machine handles all timing and weather decisions using proper astronomical calculations.

## Implementation Details

### Database Interface

Uses the existing RTS2 PostgreSQL schema:

```sql
-- Queue configuration
queues (queue_id, queue_type, remove_after_execution, ...)

-- Queue contents  
queues_targets (qid, queue_id, tar_id, time_start, time_end, queue_order, ...)

-- Target definitions
targets (tar_id, tar_name, tar_ra, tar_dec, ...)
```

**Key Design Decision**: Queue names exist only as runtime configuration. The database uses numeric IDs:
- queue_id=0: (unused - external activity detection is behavioral)
- queue_id=1: Manual queue
- queue_id=2: Scheduler queue

### Timing Control

The selector uses intelligent timing to optimize telescope efficiency:

- **`next` Commands**: Issued `time_slice` seconds (default 300s) before scheduled start
  - Allows executor to prepare while current observation completes
  - Minimizes gaps between observations
  
- **`now` Commands**: Issued when target should start immediately
  - Used for overdue targets or immediate execution
  
- **Grace Period**: 20-minute pause when external activity detected
  - Prevents conflicts with GRB responses or manual operations
  - Automatic recovery after grace period expires

### Network Communication

- **Device Type**: `DEVICE_TYPE_SELECTOR` 
- **RTS2 Protocol**: Full compatibility with existing RTS2 network layer
- **Executor Interface**: Sends `next <target_id>` and `now <target_id>` commands
- **Monitoring**: Provides RTS2 values for system status and queue state

## Usage

### Basic Startup

```bash
# Minimal startup - reads configuration from /etc/rts2/rts2.ini
python queue_selector.py -d SEL -p 0

# Override specific settings from rts2.ini
python queue_selector.py -d SEL -p 0 --latitude 50.1 --db-host remote-db
```

### Configuration Sources

The selector uses a **hierarchical configuration system** available to all rtspy devices:

1. **Built-in defaults**
2. **RTS2 configuration file** (`/etc/rts2/rts2.ini`)
   - Device-specific section `[SEL]` → single-word parameters (`latitude`, `port`)
   - Global sections → dotted parameters (`observatory.latitude`, `database.name`)
3. **Legacy rtspy config** (`/etc/rts2/rts2.conf`) 
4. **User config files** (`~/.rts2/rts2.conf`)
5. **Environment variables** (`RTS2_DEVICE_LATITUDE`)
6. **Command line arguments** (highest priority)

#### RTS2.ini Integration

The system automatically reads configuration from RTS2.ini:

```ini
# Device-specific section maps to single-word parameters
[SEL]
port = 40047           # → config.get('port')
db_host = remote       # → config.get('db_host')

# Global sections available with automatic mapping
[database]  
name = stars           # → config.get('db_name')
username = observer    # → config.get('db_user')
```

**Calibration timing** is handled automatically by RTS2 system states - no configuration needed.

### Configuration Options

### Configuration Options

### Configuration Options

### Configuration Options

#### Database Connection (overrides rts2.ini)
```bash
--db-host localhost          # Database host
--db-name stars             # Database name  
--db-user mates            # Database user
--db-password pasewcic25   # Database password
```

#### Timing Control
```bash
--time-slice 300           # Seconds before target start to issue 'next'
--grb-grace-period 1200    # Grace period duration (seconds)
--update-interval 30       # Selector loop interval (seconds)
```

#### Executor Interface
```bash
--executor EXEC            # Executor device name
```

#### Execution Timing
```bash
--time-slice 300           # Seconds before target start to issue 'next'
--grb-grace-period 1200    # Grace period duration (seconds)
--update-interval 30       # Selector loop interval (seconds)
```

### Monitoring and Control Values

The selector provides RTS2 monitoring values and control parameters:

#### Read-Only Monitoring Values
- `queue_size`: Number of targets in scheduler queue
- `next_target`: Name of next target to observe
- `system_state`: Current RTS2 system state description (ON_NIGHT, ON_DUSK, etc.)
- `grb_grace_until`: Grace period end time (0.0 when inactive)
- `last_update`: Last selector update timestamp
- `executor_current_target`: Current target running on executor (-1 if none)
- `last_target_id`: Last target sent to executor

#### Writable Control Values
- **`enabled`** (boolean): Master enable/disable for selector operation
  - `true`: Normal operation (default)
  - `false`: Selector stops sending commands to executor, allows manual control
  - Use for maintenance, manual observations, or emergency situations

- **`time_slice`** (integer): Advance notice time in seconds (default: 300)
  - Controls when to send `next` command before target start time
  - Larger values = more preparation time, but less flexibility
  - Smaller values = tighter scheduling, but risk of gaps between targets

- **`grb_grace_active`** (boolean): Manual grace period control  
  - `false→true`: Immediately initiates 20-minute grace period with current target
  - `true→false`: Immediately cancels active grace period, resumes normal operation
  - Useful for debugging or manual override of automatic grace period detection
  - Grace period prevents selector from interfering with external operations

#### Runtime Control Examples

Control the selector during operation using RTS2 commands:

```bash
# Disable selector for manual operations
rts2-value SEL enabled false

# Re-enable selector  
rts2-value SEL enabled true

# Adjust timing for faster target switching
rts2-value SEL time_slice 180

# Manually initiate grace period (debugging)
rts2-value SEL grb_grace_active true

# Cancel active grace period
rts2-value SEL grb_grace_active false

# Monitor current status
rts2-value SEL enabled
rts2-value SEL grb_grace_active  
rts2-value SEL queue_size
```

### Manual Queue Operations

Add hard-timed manual target:
```sql
INSERT INTO queues_targets (qid, queue_id, tar_id, time_start, queue_order)
VALUES (nextval('qid'), 1, 1234, '2025-07-25 02:30:00'::timestamp, 1);
```

Or use a simple command-line tool:
```bash
rts2-queue-manual --target 1234 --at "2025-07-25 02:30:00"
```

## Differences from Original RTS2 Selector

### Removed Functionality
- **Target Selection Algorithms**: No bonus calculations, merit functions, or optimization
- **Multiple Queue Types**: Eliminated `integral_targets`, `regular_targets`, `longscript` etc.
- **Complex Queue Management**: No `--add-queue` runtime queue creation
- **Network Queue Commands**: No `queue`, `clear`, `remove`, `insert` command handling
- **GRB Command Interface**: No `grb` command handling (replaced by behavioral grace period detection)
- **Automatic Target Discovery**: No database scanning for new targets

### Simplified Behavior
- **Pure Queue Executor**: Only executes pre-computed schedules
- **External Scheduling**: Relies on separate scheduler service for optimization
- **Fixed Queue Structure**: Two real queues plus external activity detection
- **Database-Centric**: Minimal network command interface

### Enhanced Features
- **Grace Period Management**: Automatic conflict avoidance with external commands
- **Manual Control Interface**: Runtime control via writable RTS2 values (`enabled`, `time_slice`, `grb_grace_active`)
- **Intelligent Timing**: Optimized `next`/`now` command timing for efficiency
- **Robust Device Authentication**: Fixed device-to-device connection authentication for reliable operation
- **Clean Architecture**: Separated concerns with clear responsibilities
- **Python Implementation**: Modern codebase following rtspy patterns
- **System-wide RTS2.ini Integration**: Unified configuration across all rtspy devices
- **Automatic Calibration Timing**: Derived from existing RTS2 horizon settings
- **Production Stability**: Extensively tested and debugged for operational deployment

## Operational Workflow

### Normal Operation
1. **External Scheduler** computes optimized observing plan
2. **Scheduler** writes targets to database (`queue_id=2`)
3. **Queue Selector** reads targets in time order
4. **Queue Selector** issues `next` commands to executor with appropriate timing
5. **Executor** performs observations

### Manual Override
1. **User** executes `EXEC.now 1234` directly
2. **Queue Selector** detects external activity
3. **Queue Selector** enters 20-minute grace period
4. **Queue Selector** resumes normal operation after grace period

### GRB Response  
1. **GRB Daemon** executes `EXEC.grb 1234` directly to executor
2. **Queue Selector** detects external activity via executor monitoring
3. **Grace period** prevents selector interference with alert response
4. **Scheduler** may incorporate GRB into future plans (automatic recovery)

### Hard-Timed Manual Targets
1. **Observer** adds target to manual queue (`queue_id=1`) with precise timing
2. **Queue Selector** executes manual target at specified time
3. **Manual targets** override scheduler queue during their time slots

## System Integration

### Required Components
- **PostgreSQL Database**: RTS2 "stars" database with queue tables
- **RTS2 Centrald**: Central daemon for network coordination  
- **RTS2 Executor**: Telescope/instrument control (unchanged)
- **External Scheduler**: Separate service for astronomical optimization

### Optional Components
- **GRB Daemon**: Alert reception and response
- **Manual Queue Tools**: Command-line utilities for manual target insertion

## Design Philosophy

The queue selector embodies several key design principles:

1. **Single Responsibility**: Each component has one clear job
2. **Separation of Concerns**: Scheduling separate from execution  
3. **Graceful Conflict Resolution**: Automatic detection and avoidance of conflicts
4. **Minimal Complexity**: Simple, predictable behavior
5. **Database-Centric**: Configuration and state in database, not memory
6. **Backward Compatibility**: Works within existing RTS2 ecosystem

This approach trades the flexibility of the original selector for reliability, maintainability, and operational simplicity. The result is a system that "just works" for the common case while providing escape hatches for special situations.
