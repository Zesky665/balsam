defmodule Balsam.JobRunner do
  @moduledoc """
  Handles execution of single workflows/jobs.
  """

  require Logger
  alias Balsam.JobStorage

  @doc """
  Start a single job execution.
  """
  def start_job(job_id, workflow_config, progress_callback \\ nil) do
    job_ref = make_ref()

    case JobStorage.start_job_run(job_id) do
      {:ok, job_run} ->
        pid = spawn_link(fn ->
          execute_job(job_id, job_ref, workflow_config, progress_callback, job_run.id)
        end)

        Process.monitor(pid)
        Logger.info("Started job #{job_id} with ref #{inspect(job_ref)}")
        {:ok, job_ref}

      error ->
        Logger.error("Failed to start job #{job_id}: #{inspect(error)}")
        error
    end
  end

  @doc """
  Cancel a running job.
  """
  def cancel_job(_job_ref) do
    # In a real implementation, you'd track the process PID and kill it
    Logger.info("Cancelling job")
    :ok
  end

  @doc """
  Get status of a job.
  """
  def get_job_status(job_ref) do
    # In a real implementation, you'd track job states
    {:ok, %{status: :running, job_ref: job_ref}}
  end

  ## Private Functions

  defp execute_job(job_id, job_ref, workflow_config, progress_callback, job_run_id) do
    try do
      result = if is_single_node_workflow?(workflow_config) do
        execute_single_node(job_id, workflow_config, progress_callback, job_run_id)
      else
        {:error, :multi_node_not_supported_in_job_runner}
      end

      JobStorage.complete_job_run(job_run_id, :completed, result)
      send_completion_message(job_ref)

    rescue
      error ->
        Logger.error("Job #{job_id} failed: #{inspect(error)}")
        JobStorage.complete_job_run(job_run_id, :failed, nil, inspect(error))
        send_failure_message(job_ref, error)
    end
  end

  defp is_single_node_workflow?(workflow_config) do
    nodes = workflow_config[:nodes] || %{}
    map_size(nodes) == 1 and Map.has_key?(nodes, :main)
  end

  defp execute_single_node(job_id, workflow_config, progress_callback, job_run_id) do
    main_node = workflow_config.nodes[:main]
    module = main_node[:module]
    function = main_node[:function] || :run
    args = main_node[:args] || []

    JobStorage.log_job_message(job_id, :info, "Starting job execution", %{}, job_run_id)

    # Wrap progress callback to store in database
    wrapped_callback = wrap_progress_callback(job_id, job_run_id, progress_callback)

    # Execute the workflow
    result = apply(module, function, args ++ [wrapped_callback])

    JobStorage.log_job_message(job_id, :info, "Job completed", %{result: result}, job_run_id)
    result
  end

  defp wrap_progress_callback(job_id, job_run_id, user_callback) do
    fn current, total, message ->
      JobStorage.record_progress(job_id, current, total, message, job_run_id)
      if user_callback, do: user_callback.(current, total, message)
    end
  end

  defp send_completion_message(job_ref) do
    # Send to orchestrator
    if Process.whereis(Balsam.Orchestrator) do
      send(Process.whereis(Balsam.Orchestrator), {:job_completed, job_ref})
    end
  end

  defp send_failure_message(job_ref, error) do
    # Send to orchestrator
    if Process.whereis(Balsam.Orchestrator) do
      send(Process.whereis(Balsam.Orchestrator), {:job_failed, job_ref, error})
    end
  end
end
