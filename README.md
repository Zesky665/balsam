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