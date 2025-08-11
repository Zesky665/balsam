defmodule Balsam.Orchestrator do
  use GenServer
  require Logger
  alias Balsam.JobStorage

  @moduledoc """
  Production-ready orchestrator for ETL jobs with database persistence.

  Features:
  - Concurrent job execution with configurable limits
  - Database persistence for job history and logs
  - Real-time progress tracking
  - Job scheduling and cancellation
  - Comprehensive error handling and recovery
  """

  defstruct [
    :jobs,
    :running_jobs,
    :supervisor_pid,
    :schedule_timer,
    :max_concurrent_jobs
  ]

  ## Client API

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Register a new ETL job with the orchestrator.

  ## Options

    * `:module` - Module containing the job function (required)
    * `:function` - Function to call (required)
    * `:args` - Arguments to pass to the function (default: [])
    * `:schedule` - When to run: :manual or {:interval, milliseconds} (default: :manual)
    * `:enable_progress` - Whether to enable progress tracking (default: false)
    * `:max_retries` - Maximum retry attempts (default: 3)
    * `:timeout` - Job timeout in milliseconds (default: :infinity)

  ## Examples

      Balsam.Orchestrator.register_job(:daily_etl, %{
        module: MyETL,
        function: :process_daily_data,
        args: [],
        schedule: {:interval, :timer.hours(24)},
        enable_progress: true,
        max_retries: 2
      })
  """
  def register_job(orchestrator \\ __MODULE__, job_id, job_config) do
    GenServer.call(orchestrator, {:register_job, job_id, job_config})
  end

  @doc """
  Run a job immediately.

  Returns `{:ok, task_ref}` on success or `{:error, reason}` on failure.
  """
  def run_job(orchestrator \\ __MODULE__, job_id) do
    GenServer.call(orchestrator, {:run_job, job_id}, :infinity)
  end

  @doc """
  Cancel a running job.
  """
  def cancel_job(orchestrator \\ __MODULE__, job_id) do
    GenServer.call(orchestrator, {:cancel_job, job_id})
  end

  @doc """
  Get detailed status of a specific job.
  """
  def job_status(orchestrator \\ __MODULE__, job_id) do
    GenServer.call(orchestrator, {:job_status, job_id})
  end

  @doc """
  List all currently running jobs.
  """
  def list_running_jobs(orchestrator \\ __MODULE__) do
    GenServer.call(orchestrator, :list_running_jobs)
  end

  @doc """
  Get overall orchestrator status.
  """
  def get_status(orchestrator \\ __MODULE__) do
    GenServer.call(orchestrator, :get_status)
  end

  @doc """
  Get job execution history from database.
  """
  def get_job_history(job_id, opts \\ []) do
    GenServer.call(__MODULE__, {:get_job_history, job_id, opts})
  end

  @doc """
  Get job logs from database.
  """
  def get_job_logs(job_id, opts \\ []) do
    GenServer.call(__MODULE__, {:get_job_logs, job_id, opts})
  end

  @doc """
  Get job statistics from database.
  """
  def get_job_statistics(job_id, days_back \\ 30) do
    GenServer.call(__MODULE__, {:get_job_statistics, job_id, days_back})
  end

  ## Server Implementation

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    {:ok, supervisor_pid} = Task.Supervisor.start_link()

    state = %__MODULE__{
      jobs: %{},
      running_jobs: %{},
      supervisor_pid: supervisor_pid,
      schedule_timer: nil,
      max_concurrent_jobs: Keyword.get(opts, :max_concurrent_jobs, 3)
    }

    # Load persisted job definitions
    loaded_state = load_persisted_jobs(state)

    Logger.info("Orchestrator started with max #{loaded_state.max_concurrent_jobs} concurrent jobs")
    Logger.info("Loaded #{map_size(loaded_state.jobs)} persisted job definitions")

    {:ok, schedule_next_check(loaded_state)}
  end

  @impl true
  def handle_call({:register_job, job_id, job_config}, _from, state) do
    case validate_job_config(job_config) do
      {:ok, validated_config} ->
        # Store in database for persistence
        case persist_job_definition(job_id, validated_config) do
          {:ok, _job_def} ->
            updated_jobs = Map.put(state.jobs, job_id, validated_config)
            Logger.info("Registered job: #{job_id} (schedule: #{inspect(validated_config.schedule)})")
            {:reply, :ok, %{state | jobs: updated_jobs}}

          {:error, reason} ->
            Logger.error("Failed to persist job #{job_id}: #{inspect(reason)}")
            {:reply, {:error, :persistence_failed}, state}
        end

      {:error, reason} ->
        Logger.error("Failed to register job #{job_id}: #{reason}")
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:run_job, job_id}, _from, state) do
    cond do
      not Map.has_key?(state.jobs, job_id) ->
        {:reply, {:error, :job_not_found}, state}

      Map.has_key?(state.running_jobs, job_id) ->
        {:reply, {:error, :job_already_running}, state}

      map_size(state.running_jobs) >= state.max_concurrent_jobs ->
        {:reply, {:error, :max_concurrent_jobs_reached}, state}

      true ->
        job_config = Map.get(state.jobs, job_id)

        case start_job_run(state, job_id, job_config) do
          {:ok, job_info} ->
            new_running_jobs = Map.put(state.running_jobs, job_id, job_info)
            Logger.info("Started job: #{job_id} (run_id: #{job_info.job_run_id})")

            {:reply, {:ok, job_info.task_ref}, %{state | running_jobs: new_running_jobs}}

          {:error, reason} = error ->
            Logger.error("Failed to start job #{job_id}: #{inspect(reason)}")
            {:reply, error, state}
        end
    end
  end

  @impl true
  def handle_call({:cancel_job, job_id}, _from, state) do
    case Map.get(state.running_jobs, job_id) do
      nil ->
        {:reply, {:error, :job_not_running}, state}

      job_info ->
        Task.Supervisor.terminate_child(state.supervisor_pid, job_info.task_pid)

        # Mark as cancelled in database
        JobStorage.complete_job_run(
          job_info.job_run_id,
          :cancelled,
          nil,
          "Job cancelled by user",
          job_info.progress
        )
        JobStorage.log_job_message(job_id, :warning, "Job cancelled by user", %{}, job_info.job_run_id)

        new_running_jobs = Map.delete(state.running_jobs, job_id)
        Logger.info("Cancelled job: #{job_id}")

        {:reply, :ok, %{state | running_jobs: new_running_jobs}}
    end
  end

  @impl true
  def handle_call({:job_status, job_id}, _from, state) do
    case Map.get(state.running_jobs, job_id) do
      nil ->
        if Map.has_key?(state.jobs, job_id) do
          # Check last run from database
          case JobStorage.get_latest_job_run(job_id) do
            nil ->
              {:reply, {:ok, %{status: :never_run}}, state}

            last_run ->
              status = %{
                status: :not_running,
                last_run: %{
                  status: last_run.status,
                  started_at: last_run.started_at,
                  completed_at: last_run.completed_at,
                  duration_ms: last_run.duration_ms
                }
              }
              {:reply, {:ok, status}, state}
          end
        else
          {:reply, {:error, :job_not_found}, state}
        end

      job_info ->
        status = %{
          status: job_info.status,
          started_at: job_info.started_at,
          running_time_ms: System.system_time(:millisecond) - job_info.started_at,
          progress: job_info.progress,
          job_run_id: job_info.job_run_id
        }

        {:reply, {:ok, status}, state}
    end
  end

  @impl true
  def handle_call(:list_running_jobs, _from, state) do
    running_jobs = state.running_jobs
    |> Enum.map(fn {job_id, job_info} ->
      {job_id, %{
        status: job_info.status,
        started_at: job_info.started_at,
        running_time_ms: System.system_time(:millisecond) - job_info.started_at,
        progress: job_info.progress
      }}
    end)
    |> Enum.into(%{})

    {:reply, running_jobs, state}
  end

  @impl true
  def handle_call(:get_status, _from, state) do
    status = %{
      registered_jobs: Map.keys(state.jobs),
      running_jobs: Map.keys(state.running_jobs),
      running_count: map_size(state.running_jobs),
      max_concurrent: state.max_concurrent_jobs,
      available_slots: state.max_concurrent_jobs - map_size(state.running_jobs)
    }
    {:reply, status, state}
  end

  @impl true
  def handle_call({:get_job_history, job_id, opts}, _from, state) do
    history = JobStorage.list_job_runs(job_id, opts)
    {:reply, history, state}
  end

  @impl true
  def handle_call({:get_job_logs, job_id, opts}, _from, state) do
    logs = JobStorage.get_job_logs(job_id, opts)
    {:reply, logs, state}
  end

  @impl true
  def handle_call({:get_job_statistics, job_id, days_back}, _from, state) do
    stats = JobStorage.get_job_statistics(job_id, days_back)
    {:reply, stats, state}
  end

  ## Handle job completion
  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case find_job_by_ref(state.running_jobs, ref) do
      {job_id, job_info} ->
        job_run_id = job_info.job_run_id

        {db_status, result, error_message} = case reason do
          :normal ->
            JobStorage.log_job_message(job_id, :info, "Job completed successfully", %{}, job_run_id)
            Logger.info("Job #{job_id} completed successfully")
            {:completed, %{success: true}, nil}

          {:shutdown, :timeout} ->
            error_msg = "Job timeout"
            JobStorage.log_job_message(job_id, :error, error_msg, %{}, job_run_id)
            Logger.warning("Job #{job_id} timed out")
            {:failed, nil, error_msg}

          error ->
            error_msg = "Job failed: #{inspect(error)}"
            JobStorage.log_job_message(job_id, :error, error_msg, %{error: error}, job_run_id)
            Logger.error("Job #{job_id} failed: #{inspect(error)}")
            {:failed, nil, error_msg}
        end

        # Complete job run in database
        JobStorage.complete_job_run(job_run_id, db_status, result, error_message, job_info.progress)

        # Update last run time in both memory and database
        current_time = System.system_time(:millisecond)
        updated_job_config = Map.put(state.jobs[job_id], :last_run, current_time)
        new_jobs = Map.put(state.jobs, job_id, updated_job_config)

        # Persist the last run time
        update_job_last_run(job_id, current_time)

        # Remove from running jobs
        new_running_jobs = Map.delete(state.running_jobs, job_id)

        {:noreply, %{state | running_jobs: new_running_jobs, jobs: new_jobs}}

      nil ->
        Logger.warning("Received DOWN message for unknown job reference: #{inspect(ref)}")
        {:noreply, state}
    end
  end

  ## Handle progress updates
  @impl true
  def handle_info({:job_progress, job_id, progress_data}, state) do
    case Map.get(state.running_jobs, job_id) do
      nil ->
        {:noreply, state}

      job_info ->
        job_run_id = job_info.job_run_id

        # Store progress in database
        JobStorage.record_progress(
          job_id,
          progress_data.current,
          progress_data.total,
          progress_data.message,
          job_run_id
        )

        # Log significant progress milestones
        if should_log_progress_milestone?(progress_data.current, progress_data.total) do
          percentage = calculate_percentage(progress_data.current, progress_data.total)
          log_message = "Progress: #{progress_data.current}#{if progress_data.total, do: "/#{progress_data.total}"} #{if percentage, do: "(#{percentage}%)"} - #{progress_data.message}"

          # Simple progress context for database storage
          simple_progress_context = %{
            "current" => progress_data.current,
            "total" => progress_data.total,
            "percentage" => percentage
          }

          JobStorage.log_job_message(job_id, :info, log_message, simple_progress_context, job_run_id)
        end

        # Update in-memory state
        updated_job_info = put_in(job_info.progress, progress_data)
        new_running_jobs = Map.put(state.running_jobs, job_id, updated_job_info)

        {:noreply, %{state | running_jobs: new_running_jobs}}
    end
  end

  ## Handle scheduled job execution
  @impl true
  def handle_info(:check_scheduled_jobs, state) do
    available_slots = state.max_concurrent_jobs - map_size(state.running_jobs)

    if available_slots > 0 do
      try do
        due_jobs = state.jobs
        |> Enum.filter(fn {job_id, job_config} ->
          should_run_job?(job_config) and not Map.has_key?(state.running_jobs, job_id)
        end)
        |> Enum.take(available_slots)

        Enum.each(due_jobs, fn {job_id, _config} ->
          GenServer.cast(self(), {:start_scheduled_job, job_id})
        end)
      rescue
        error ->
          Logger.error("Error in scheduled job check: #{inspect(error)}")
      end
    end

    {:noreply, schedule_next_check(state)}
  end

  @impl true
  def handle_cast({:start_scheduled_job, job_id}, state) do
    case handle_call({:run_job, job_id}, nil, state) do
      {:reply, {:ok, _ref}, new_state} ->
        {:noreply, new_state}

      {:reply, error, new_state} ->
        Logger.warning("Failed to start scheduled job #{job_id}: #{inspect(error)}")
        {:noreply, new_state}
    end
  end

  ## Private Functions

  defp validate_job_config(config) when is_map(config) do
    required_fields = [:module, :function]

    case Enum.find(required_fields, fn field -> not Map.has_key?(config, field) end) do
      nil ->
        defaults = %{
          args: [],
          schedule: :manual,
          last_run: nil,
          max_retries: 3,
          timeout: :infinity,
          enable_progress: false
        }

        validated_config = Map.merge(defaults, config)
        {:ok, validated_config}

      missing_field ->
        {:error, "Missing required field: #{missing_field}"}
    end
  end
  defp validate_job_config(_), do: {:error, "Job config must be a map"}

  defp start_job_run(state, job_id, job_config) do
    # Create job run record in database
    case JobStorage.start_job_run(job_id, %{config: job_config}) do
      {:ok, job_run} ->
        case start_job_task(state, job_id, job_config, job_run.id) do
          {:ok, task_pid, task_ref} ->
            job_info = %{
              task_pid: task_pid,
              task_ref: task_ref,
              job_run_id: job_run.id,
              started_at: System.system_time(:millisecond),
              status: :running,
              progress: %{message: "Starting...", current: 0, total: nil}
            }

            JobStorage.log_job_message(job_id, :info, "Job started", %{}, job_run.id)
            {:ok, job_info}

          error ->
            JobStorage.complete_job_run(job_run.id, :failed, nil, "Failed to start: #{inspect(error)}")
            error
        end

      error ->
        Logger.error("Failed to create job run record: #{inspect(error)}")
        {:error, :database_error}
    end
  end

  defp start_job_task(state, job_id, job_config, job_run_id) do
    orchestrator_pid = self()

    task_fun = fn ->
      # Set up progress callback
      progress_callback = if job_config.enable_progress do
        fn current, total, message ->
          progress_data = %{current: current, total: total, message: message}
          send(orchestrator_pid, {:job_progress, job_id, progress_data})
        end
      else
        nil
      end

      try do
        module = job_config.module
        function = job_config.function
        args = job_config.args

        # Add progress callback to args if enabled
        enhanced_args = if progress_callback do
          args ++ [progress_callback]
        else
          args
        end

        JobStorage.log_job_message(job_id, :info, "Executing #{module}.#{function}", %{args: length(enhanced_args)}, job_run_id)

        result = apply(module, function, enhanced_args)
        {:ok, result}
      rescue
        error ->
          # Create a simple error context that can be serialized
          simple_error_context = %{
            "error_type" => error.__struct__ |> to_string(),
            "error_message" => Exception.message(error)
          }

          JobStorage.log_job_message(job_id, :error, "Job execution failed: #{Exception.message(error)}", simple_error_context, job_run_id)
          {:error, error}
      end
    end

    case Task.Supervisor.start_child(state.supervisor_pid, task_fun) do
      {:ok, task_pid} ->
        task_ref = Process.monitor(task_pid)
        {:ok, task_pid, task_ref}

      error ->
        error
    end
  end

  defp find_job_by_ref(running_jobs, ref) do
    Enum.find(running_jobs, fn {_job_id, job_info} ->
      job_info.task_ref == ref
    end)
  end

  defp should_run_job?(job_config) when is_map(job_config) do
    case Map.get(job_config, :schedule) do
      {:interval, interval_ms} ->
        case Map.get(job_config, :last_run) do
          nil -> true
          last_run -> System.system_time(:millisecond) - last_run >= interval_ms
        end

      :manual -> false
      _ -> false
    end
  end

  defp should_log_progress_milestone?(current, total) when is_integer(total) and total > 0 do
    percentage = trunc(current / total * 100)
    rem(percentage, 25) == 0 and percentage > 0  # Log at 25%, 50%, 75%, 100%
  end
  defp should_log_progress_milestone?(_, _), do: false

  defp calculate_percentage(current, total) when is_integer(total) and total > 0 do
    trunc(current / total * 100)
  end
  defp calculate_percentage(_, _), do: nil

  defp schedule_next_check(state) do
    if state.schedule_timer do
      Process.cancel_timer(state.schedule_timer)
    end

    # Check for scheduled jobs every minute
    timer = Process.send_after(self(), :check_scheduled_jobs, :timer.minutes(1))
    %{state | schedule_timer: timer}
  end

  defp load_persisted_jobs(state) do
    try do
      import Ecto.Query
      alias Balsam.{Repo, Schema.JobDefinition}

      job_definitions = from(jd in JobDefinition, where: jd.active == true)
      |> Repo.all()

      persisted_jobs = job_definitions
      |> Enum.reduce(%{}, fn job_def, acc ->
        case JobDefinition.to_config(job_def) do
          {:ok, config} ->
            job_id = String.to_existing_atom(job_def.job_id)
            Map.put(acc, job_id, config)

          {:error, reason} ->
            Logger.warning("Failed to load job definition #{job_def.job_id}: #{inspect(reason)}")
            acc
        end
      end)

      %{state | jobs: persisted_jobs}
    rescue
      error ->
        Logger.warning("Failed to load persisted jobs: #{inspect(error)}")
        state
    end
  end

  defp persist_job_definition(job_id, job_config) do
    alias Balsam.{Repo, Schema.JobDefinition}

    attrs = JobDefinition.from_config(job_id, job_config)

    %JobDefinition{}
    |> JobDefinition.changeset(attrs)
    |> Repo.insert(on_conflict: :replace_all, conflict_target: :job_id)
  end

  defp update_job_last_run(job_id, last_run_time) do
    try do
      import Ecto.Query
      alias Balsam.{Repo, Schema.JobDefinition}

      from(jd in JobDefinition, where: jd.job_id == ^to_string(job_id))
      |> Repo.update_all(set: [last_run: last_run_time])
    rescue
      error ->
        Logger.warning("Failed to update last run time for #{job_id}: #{inspect(error)}")
    end
  end

  @impl true
  def terminate(reason, state) do
    Logger.info("Orchestrator terminating: #{inspect(reason)}")

    if state.schedule_timer do
      Process.cancel_timer(state.schedule_timer)
    end

    # Gracefully stop all running jobs
    Enum.each(state.running_jobs, fn {_job_id, job_info} ->
      Task.Supervisor.terminate_child(state.supervisor_pid, job_info.task_pid)
      JobStorage.complete_job_run(job_info.job_run_id, :cancelled, nil, "Orchestrator shutdown")
    end)

    :ok
  end
end
