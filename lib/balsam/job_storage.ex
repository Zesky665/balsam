defmodule Balsam.JobStorage do
  alias Balsam.Repo
  alias Balsam.Schema.{JobRun, JobLog, JobProgress}
  import Ecto.Query
  require Logger

  @moduledoc """
  Service layer for storing and retrieving job execution data.
  """

  ## Job Run Management

  def start_job_run(job_id, metadata \\ %{}) do
    # Convert metadata to JSON string for storage
    metadata_string = inspect(metadata)  # Simple approach - just convert to string representation

    attrs = %{
      job_id: to_string(job_id),
      started_at: DateTime.utc_now(),
      metadata: metadata_string
    }

    %JobRun{}
    |> JobRun.start_changeset(attrs)
    |> Repo.insert()
  end

  def complete_job_run(job_run_id, status, result \\ nil, error_message \\ nil, progress_data \\ nil) do
    job_run = Repo.get!(JobRun, job_run_id)

    attrs = %{
      status: status,
      completed_at: DateTime.utc_now(),
      result: result,
      error_message: error_message,
      progress_data: progress_data
    }

    job_run
    |> JobRun.complete_changeset(attrs)
    |> Repo.update()
  end

  def get_job_run(job_run_id) do
    Repo.get(JobRun, job_run_id)
  end

  def list_job_runs(job_id, opts \\ []) do
    limit = Keyword.get(opts, :limit, 50)
    job_id_str = to_string(job_id)  # Convert atom to string

    JobRun
    |> where([jr], jr.job_id == ^job_id_str)
    |> order_by([jr], desc: jr.started_at)
    |> limit(^limit)
    |> Repo.all()
  end

  def get_latest_job_run(job_id) do
    job_id_str = to_string(job_id)  # Convert atom to string

    JobRun
    |> where([jr], jr.job_id == ^job_id_str)
    |> order_by([jr], desc: jr.started_at)
    |> limit(1)
    |> Repo.one()
  end

  ## Job Logging

  def log_job_message(job_id, level, message, context \\ %{}, job_run_id \\ nil) do
    # Convert context to a simple string representation
    context_string = case context do
      ctx when is_map(ctx) and map_size(ctx) == 0 -> ""
      ctx when is_map(ctx) ->
        # Only store simple, serializable data
        ctx
        |> Enum.filter(fn {_k, v} -> is_binary(v) or is_number(v) or is_boolean(v) end)
        |> Enum.into(%{})
        |> inspect(limit: 100, printable_limit: 500)
      ctx -> inspect(ctx, limit: 50)
    end

    attrs = %{
      job_id: to_string(job_id),  # Convert atom to string
      job_run_id: job_run_id,
      level: level,
      message: message,
      context: context_string,
      logged_at: DateTime.utc_now()
    }

    %JobLog{}
    |> JobLog.changeset(attrs)
    |> Repo.insert()
  end

  def get_job_logs(job_id, opts \\ []) do
    limit = Keyword.get(opts, :limit, 100)
    level = Keyword.get(opts, :level)
    job_run_id = Keyword.get(opts, :job_run_id)
    job_id_str = to_string(job_id)  # Convert atom to string

    query = JobLog
    |> where([jl], jl.job_id == ^job_id_str)
    |> order_by([jl], desc: jl.logged_at)
    |> limit(^limit)

    query = if level do
      where(query, [jl], jl.level == ^level)
    else
      query
    end

    query = if job_run_id do
      where(query, [jl], jl.job_run_id == ^job_run_id)
    else
      query
    end

    Repo.all(query)
  end

  ## Progress Tracking

  def record_progress(job_id, current, total \\ nil, message \\ "", job_run_id \\ nil) do
    attrs = %{
      job_id: to_string(job_id),  # Convert atom to string
      job_run_id: job_run_id,
      current: current,
      total: total,
      message: message,
      recorded_at: DateTime.utc_now()
    }

    %JobProgress{}
    |> JobProgress.changeset(attrs)
    |> Repo.insert()
  end

  def get_job_progress_history(job_id, opts \\ []) do
    limit = Keyword.get(opts, :limit, 50)
    job_run_id = Keyword.get(opts, :job_run_id)
    job_id_str = to_string(job_id)  # Convert atom to string

    query = JobProgress
    |> where([jp], jp.job_id == ^job_id_str)
    |> order_by([jp], desc: jp.recorded_at)
    |> limit(^limit)

    query = if job_run_id do
      where(query, [jp], jp.job_run_id == ^job_run_id)
    else
      query
    end

    Repo.all(query)
  end

  def get_latest_progress(job_id) do
    job_id_str = to_string(job_id)  # Convert atom to string

    JobProgress
    |> where([jp], jp.job_id == ^job_id_str)
    |> order_by([jp], desc: jp.recorded_at)
    |> limit(1)
    |> Repo.one()
  end

  ## Analytics & Reporting

  def get_job_statistics(job_id, days_back \\ 30) do
    cutoff_date = DateTime.utc_now() |> DateTime.add(-days_back, :day)
    job_id_str = to_string(job_id)  # Convert atom to string

    stats_query = from jr in JobRun,
      where: jr.job_id == ^job_id_str and jr.started_at >= ^cutoff_date,
      group_by: jr.status,
      select: {jr.status, count(jr.id)}

    duration_query = from jr in JobRun,
      where: jr.job_id == ^job_id_str and jr.started_at >= ^cutoff_date and not is_nil(jr.duration_ms),
      select: %{
        avg_duration: avg(jr.duration_ms),
        min_duration: min(jr.duration_ms),
        max_duration: max(jr.duration_ms)
      }

    status_counts = Repo.all(stats_query) |> Enum.into(%{})
    duration_stats = Repo.one(duration_query)

    %{
      status_counts: status_counts,
      duration_stats: duration_stats,
      total_runs: Map.values(status_counts) |> Enum.sum(),
      success_rate: calculate_success_rate(status_counts)
    }
  end

  def cleanup_old_data(days_to_keep \\ 90) do
    cutoff_date = DateTime.utc_now() |> DateTime.add(-days_to_keep, :day)

    # Delete old job runs (logs and progress will cascade delete)
    {count, _} = from(jr in JobRun, where: jr.started_at < ^cutoff_date)
    |> Repo.delete_all()

    Logger.info("Cleaned up #{count} old job runs")
    :ok
  end

  # Private helpers

  defp calculate_success_rate(status_counts) do
    total = Map.values(status_counts) |> Enum.sum()
    completed = Map.get(status_counts, :completed, 0)

    if total > 0 do
      (completed / total * 100) |> Float.round(2)
    else
      0.0
    end
  end
end
