# Balsam Test Scenarios

This document outlines the current test scenarios implemented in the Balsam framework and proposes additional scenarios to improve test coverage and confidence.

## Current Test Scenarios

### Basic Functionality Tests (`balsam_test.exs`)
- **Basic greeting test** - Simple smoke test to verify module loading
  - Tests `Balsam.hello()` returns `:world`
  - Purpose: Minimal functionality verification

### Orchestrator Tests (`orchestrator_test.exs`)

#### Core Orchestrator Operations
- **Orchestrator startup** - Verifies orchestrator process starts successfully
  - Checks process is alive
  - Validates initial state (0 jobs, empty workflow list)

- **Workflow registration** - Tests workflow registration with orchestrator
  - Registers simple workflow with built-in String module
  - Verifies workflow appears in registered workflows list

- **Job execution** - Tests basic job running capability
  - Uses simple String.length function as test job
  - Validates job reference creation
  - Checks job status tracking

- **Job cancellation** - Tests ability to cancel running jobs
  - Attempts to cancel jobs (may complete before cancellation)
  - Handles graceful failure when job not found

- **Progress tracking** - Tests progress callback functionality
  - Registers progress callback function
  - Verifies job can be started with progress tracking

#### Multi-step Pipeline Tests
- **Pipeline registration** - Tests DAG workflow registration
  - Multi-node workflow with dependencies
  - 3-step sequential pipeline (step1 → step2 → step3)

- **DAG execution** - Tests running multi-node workflows
  - Handles case where DAG functionality may not be implemented
  - Falls back gracefully when DAG execution fails

#### Error Handling
- **Non-existent workflow** - Tests graceful handling of invalid workflow IDs
  - Expects `{:error, :job_not_found}` for unknown workflows

- **Invalid workflow registration** - Tests validation of workflow configurations
  - Missing required fields (nodes)
  - Expects registration to fail with error

- **Status reporting** - Tests orchestrator status accuracy
  - Validates status structure and data types
  - Tracks running job counts correctly

### Workflow Registry Tests (`workflow_registry_test.exs`)

#### Workflow Discovery
- **Module discovery** - Tests automatic workflow detection
  - Discovers modules implementing `@behaviour Balsam.Workflow`
  - Validates at least one workflow is found
  - Checks for expected workflow IDs

#### Configuration Validation
- **Config structure validation** - Tests workflow configuration format
  - Validates required fields (name, nodes)
  - Checks node configuration structure
  - Verifies module and function specifications

### Comprehensive Workflow Registry Tests (`workflow_registry_comprehensive_test.exs`)

#### Advanced Discovery
- **Detailed workflow discovery** - Comprehensive workflow detection testing
  - Logs discovered workflows for debugging
  - Categorizes single-node vs multi-node workflows
  - Tests database integration for job definitions

#### Database Integration
- **Job definition persistence** - Tests database storage of workflow configs
  - Creates job_definitions table entries
  - Handles database migration and setup
  - Tests single-node workflow to job definition conversion

#### Multi-node Workflow Testing
- **DAG structure validation** - Tests complex workflow configurations
  - Validates multi-node workflow discovery
  - Tests node dependency structures
  - Verifies required node fields

### Fault Tolerance Tests (`fault_tolerance_comprehensive.exs`)

#### Basic Fault Tolerance
- **Orchestrator crash survival** - Tests orchestrator resilience to job crashes
  - Jobs that crash immediately
  - Multiple exception types (RuntimeError, ArgumentError, throw, exit)
  - Process kill attempts
  - Verifies orchestrator PID remains unchanged (no restart)

#### Concurrent Failure Handling
- **Multiple simultaneous crashes** - Tests handling of concurrent failures
  - 5 jobs crashing with different delays
  - Validates orchestrator survival and state integrity

- **High concurrency stress test** - Tests system under heavy load
  - 20+ concurrent jobs with random crash behavior
  - Tests backoff and retry mechanisms when max concurrent reached
  - Validates system stability under stress

#### Resource Attack Resistance
- **Memory bomb resistance** - Tests handling of memory-intensive jobs
  - Jobs that consume large amounts of memory
  - Timeout-based job termination (5 second limit)

- **Infinite loop handling** - Tests timeout mechanisms
  - Jobs with infinite loops
  - 2-second timeout enforcement
  - Process cleanup verification

#### DAG Fault Tolerance
- **DAG node failure handling** - Tests multi-node workflow resilience
  - 3-step DAG with middle step failure
  - Graceful fallback to single job execution
  - State preservation after DAG failures

#### State Persistence and Recovery
- **Workflow persistence after failures** - Tests state durability
  - Multiple workflow registration
  - State verification after job crashes
  - Workflow count preservation

- **Post-failure recovery** - Tests system recovery capabilities
  - Running successful jobs after crashes
  - Orchestrator operational status after failures

#### System Health Monitoring
- **Status reporting accuracy** - Tests status reporting after failures
  - Mixed successful and failing jobs
  - Accurate job counts and state tracking
  - Workflow registration persistence

## Proposed Additional Test Scenarios

### Performance and Load Testing

#### Scalability Tests
- [ ] **High-volume job queue test** - Test with 100+ queued jobs
  - Measure job processing throughput
  - Memory usage monitoring during high load
  - Queue overflow handling

- [ ] **Long-running job management** - Test jobs running for extended periods
  - Jobs running for 1+ hours
  - Resource cleanup after completion
  - Memory leak detection

- [ ] **Database performance under load** - Test database operations under stress
  - Concurrent read/write operations
  - Large result set handling
  - Connection pool exhaustion scenarios

#### Resource Management Tests
- [ ] **Memory limit enforcement** - Test job memory constraints
  - Jobs exceeding memory limits
  - Graceful termination and cleanup
  - System stability after memory exhaustion

- [ ] **CPU throttling tests** - Test CPU-intensive job management
  - Jobs consuming 100% CPU
  - Multi-core job distribution
  - System responsiveness during high CPU usage

- [ ] **Disk I/O intensive scenarios** - Test file-heavy operations
  - Large file processing jobs
  - Concurrent file access
  - Disk space exhaustion handling

### Data Integrity and Consistency

#### Database Transaction Tests
- [ ] **Concurrent job state updates** - Test database consistency
  - Multiple processes updating job states
  - Transaction isolation verification
  - Deadlock detection and recovery

- [ ] **Job result persistence** - Test data durability
  - Large result set storage
  - Data corruption detection
  - Backup and recovery scenarios

#### Workflow Dependencies
- [ ] **Complex DAG dependency resolution** - Test advanced dependency scenarios
  - Diamond dependencies (A→B,C; B,C→D)
  - Circular dependency detection
  - Dynamic dependency modification

- [ ] **Conditional workflow execution** - Test dynamic workflow behavior
  - Skip nodes based on previous results
  - Branch execution based on conditions
  - Dynamic node generation

### Security and Access Control

#### Input Validation Tests
- [ ] **Malicious workflow configuration** - Test against crafted inputs
  - SQL injection attempts in workflow configs
  - Code injection through module/function names
  - Path traversal in file operations

- [ ] **Resource exhaustion attacks** - Test DoS resistance
  - Fork bombs in job execution
  - Network flooding attempts
  - Recursive job creation

#### Authentication and Authorization
- [ ] **Workflow access control** - Test permission systems
  - User-specific workflow access
  - Role-based execution permissions
  - Audit logging for security events

### Integration and External Dependencies

#### External Service Integration
- [ ] **HTTP service reliability** - Test external API interactions
  - Network timeout handling
  - Service unavailability scenarios
  - Rate limiting compliance

- [ ] **Database connection resilience** - Test database connectivity
  - Connection loss during job execution
  - Automatic reconnection mechanisms
  - Transaction rollback on connection failure

#### File System Operations
- [ ] **File permission handling** - Test file access scenarios
  - Read-only file system operations
  - Permission denied scenarios
  - Concurrent file access conflicts

- [ ] **Network storage reliability** - Test remote file operations
  - Network file system failures
  - Large file transfer scenarios
  - Partial file corruption handling

### User Experience and Monitoring

#### Error Reporting and Debugging
- [ ] **Detailed error context** - Test error information quality
  - Stack trace preservation
  - Context information in error messages
  - Error correlation across job steps

- [ ] **Job execution tracing** - Test observability features
  - Step-by-step execution logging
  - Performance profiling data
  - Resource usage tracking

#### Configuration and Deployment
- [ ] **Configuration validation** - Test config file handling
  - Invalid configuration detection
  - Configuration hot-reloading
  - Environment-specific configurations

- [ ] **Deployment scenarios** - Test different deployment configurations
  - Single-node vs distributed deployment
  - Rolling update scenarios
  - Blue-green deployment testing

### Edge Cases and Boundary Conditions

#### Time and Scheduling
- [ ] **Timezone handling** - Test time-sensitive operations
  - DST transitions during job execution
  - Cross-timezone job scheduling
  - Time-based dependency resolution

- [ ] **Leap second handling** - Test time precision scenarios
  - Job scheduling around leap seconds
  - Duration calculation accuracy
  - Timestamp consistency

#### Data Boundaries
- [ ] **Empty and null data handling** - Test edge case inputs
  - Empty job configurations
  - Null parameter handling
  - Zero-length file processing

- [ ] **Unicode and encoding** - Test international character support
  - Unicode in job names and descriptions
  - File encoding detection and conversion
  - Character set consistency

## Test Implementation Strategy

### Prioritization
1. **Critical Path** - Core orchestrator and workflow execution
2. **Fault Tolerance** - System resilience under failure conditions
3. **Performance** - Scalability and resource management
4. **Security** - Input validation and access control
5. **Integration** - External service and dependency testing

### Test Environment Setup
- **Isolated test databases** - Prevent test interference
- **Mock external services** - Reliable, repeatable testing
- **Resource monitoring** - Track system usage during tests
- **Parallel test execution** - Faster test suite execution

### Coverage Goals
- **Unit tests** - 90%+ code coverage for core modules
- **Integration tests** - All major workflow scenarios
- **End-to-end tests** - Complete user journeys
- **Performance benchmarks** - Establish baseline metrics

### Test Data Management
- **Synthetic data generation** - Realistic but controlled test data
- **Data cleanup** - Automated test environment reset
- **Test data versioning** - Reproducible test scenarios
- **Production data testing** - Sanitized production scenario testing