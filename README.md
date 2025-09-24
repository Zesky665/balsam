# Balsam

**A fault-tolerant ETL orchestration framework built with Elixir**

Balsam is a robust ETL (Extract, Transform, Load) framework that provides workflow orchestration, job scheduling, and monitoring capabilities. It's designed for reliability with built-in fault tolerance, retry mechanisms, and comprehensive error handling.

## Features

- **Fault-tolerant orchestration** - Survives job failures and maintains system stability
- **Flexible workflow definitions** - Support for both single jobs and multi-node DAGs
- **Built-in scheduling** - Manual and automated job execution
- **Progress tracking** - Real-time job progress monitoring
- **Retry mechanisms** - Configurable retry policies for failed jobs
- **Database integration** - SQLite-based job storage and tracking
- **Web scraping & data processing** - Built-in support for HTTP requests and DataFrame operations

## Prerequisites

- **Elixir 1.18** or higher
- **Erlang/OTP 25** or higher
- **SQLite** (for job storage)

## Installation & Setup

### 1. Clone the repository

```bash
git clone <repository-url>
cd balsam
```

### 2. Install dependencies

```bash
mix deps.get
```

### 3. Set up the database

```bash
# Create and migrate the database
mix ecto.setup

# Or manually:
mix ecto.create
mix ecto.migrate
```

### 4. Compile the project

```bash
mix compile
```

## Getting Started

### 1. Start the application

```bash
# Start interactive Elixir shell with the application
iex -S mix

# Or run in the background
mix run --no-halt
```

### 2. Register workflows

```elixir
# Register all available workflows
workflows = Balsam.WorkflowRegistry.register_all()

# Check orchestrator status
status = Balsam.Orchestrator.get_status()
```

### 3. Run jobs

```elixir
# Run a single job
{:ok, job_ref} = Balsam.Orchestrator.run_job(:jokes_jokes_etl)

# Run a multi-node DAG
{:ok, dag_run_id} = Balsam.Orchestrator.run_dag(:internal_data_pipeline_test)

# Check job status
{:ok, job_status} = Balsam.Orchestrator.get_job_status(job_ref)
```

## Creating Workflows

### Single-node Job Example

```elixir
defmodule MyWorkflows.DataProcessor do
  @behaviour Balsam.Workflow

  @impl true
  def workflow_config do
    %{
      name: "Data Processing Job",
      description: "Processes CSV data and exports results",
      schedule: :manual,
      enable_progress: true,
      max_retries: 3,
      timeout: :timer.minutes(10),
      nodes: %{
        main: %{
          module: __MODULE__,
          function: :run,
          args: [],
          depends_on: []
        }
      }
    }
  end

  @impl true
  def run(_progress_callback \\ nil) do
    # Your ETL logic here
    IO.puts("Processing data...")
    :ok
  end
end
```

### Multi-node DAG Example

```elixir
defmodule MyWorkflows.DataPipeline do
  @behaviour Balsam.Workflow

  @impl true
  def workflow_config do
    %{
      name: "Multi-stage Data Pipeline",
      description: "Extract, transform, and load data through multiple stages",
      nodes: %{
        extract: %{
          module: MyExtractor,
          function: :extract_data,
          depends_on: []
        },
        transform: %{
          module: MyTransformer,
          function: :transform_data,
          depends_on: [:extract]
        },
        load: %{
          module: MyLoader,
          function: :load_data,
          depends_on: [:transform]
        }
      }
    }
  end
end
```

## Configuration

The application can be configured via `config/config.exs`:

```elixir
# Database configuration
config :balsam, Balsam.Repo,
  database: "balsam_#{config_env()}.db",
  pool_size: 5

# Logging configuration
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id, :job_id, :job_run_id]
```

## Testing

```bash
# Run all tests
mix test

# Run with coverage
mix test --cover

# Run specific test file
mix test test/balsam/orchestrator_test.exs
```

## Project Structure

```
balsam/
├── lib/balsam/              # Core framework code
│   ├── orchestrator.ex      # Main orchestration engine
│   ├── workflow_registry.ex # Workflow discovery and registration
│   ├── job_runner.ex        # Individual job execution
│   ├── dag_runner.ex        # Multi-node DAG execution
│   └── schema/              # Database schemas
├── workflows/               # User-defined workflows
│   ├── jokes/              # Example: Jokes API ETL
│   ├── analytics/          # Example: User behavior analysis
│   └── datalake/           # Example: Data lake ingestion
├── config/                 # Application configuration
├── priv/repo/migrations/   # Database migrations
└── test/                   # Test suites
```

## Available Commands

### Orchestrator Operations
```elixir
# Get system status
Balsam.Orchestrator.get_status()

# Run a job with progress tracking
{:ok, job_ref} = Balsam.Orchestrator.run_job(:my_job, &progress_callback/1)

# Cancel a running job
:ok = Balsam.Orchestrator.cancel_job(job_ref)

# Retry a failed job
{:ok, new_job_ref} = Balsam.Orchestrator.retry_job(failed_job_ref)
```

### Workflow Management
```elixir
# List all available workflows
workflows = Balsam.WorkflowRegistry.list_workflows()

# Get detailed workflow information
{:ok, config} = Balsam.WorkflowRegistry.get_workflow_info(:my_workflow)

# Test a workflow configuration
{:ok, message} = Balsam.WorkflowRegistry.test_workflow(:my_workflow)
```

## Database Management

```bash
# Reset database (drops and recreates)
mix ecto.reset

# Check migration status
mix ecto.migrations

# Create a new migration
mix ecto.gen.migration add_new_feature
```

## Troubleshooting

### Common Issues

1. **Database locked error**: Stop all running processes and restart
2. **Module not found**: Ensure workflows are properly compiled with `mix compile`
3. **Job hanging**: Check for infinite loops or missing dependencies in DAG workflows

### Debugging

```elixir
# Enable verbose logging
Logger.configure(level: :debug)

# Check running processes
Process.list() |> Enum.filter(&Process.alive?/1)

# Monitor job execution
:observer.start()
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Run tests (`mix test`)
4. Commit your changes (`git commit -am 'Add some amazing feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.