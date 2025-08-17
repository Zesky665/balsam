defmodule Balsam.JobSupervisor do
  @moduledoc """
  Dynamic supervisor for job execution processes.
  Allows jobs to crash without affecting the orchestrator or other jobs.
  """

  use DynamicSupervisor

  def start_link(opts) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one, max_restarts: 0)
  end

  def start_job(job_spec) do
    DynamicSupervisor.start_child(__MODULE__, job_spec)
  end

  def list_jobs do
    DynamicSupervisor.which_children(__MODULE__)
  end

  def count_jobs do
    DynamicSupervisor.count_children(__MODULE__)
  end
end

defmodule Balsam.JobRegistry do
  @moduledoc """
  Registry for tracking active jobs by their reference.
  Allows the orchestrator to find and manage job processes.
  """

  def child_spec(_) do
    Registry.child_spec(
      keys: :unique,
      name: __MODULE__
    )
  end

  def register_job(job_ref, job_pid) do
    Registry.register(__MODULE__, job_ref, job_pid)
  end

  def lookup_job(job_ref) do
    case Registry.lookup(__MODULE__, job_ref) do
      [{pid, _}] -> {:ok, pid}
      [] -> :not_found
    end
  end

  def unregister_job(job_ref) do
    Registry.unregister(__MODULE__, job_ref)
  end

  def list_jobs do
    Registry.select(__MODULE__, [{{:"$1", :"$2", :"$3"}, [], [{{:"$1", :"$2"}}]}])
  end
end

defmodule Balsam.DagSupervisor do
  @moduledoc """
  Dynamic supervisor for DAG execution processes.
  Similar to JobSupervisor but for multi-step workflows.
  """

  use DynamicSupervisor

  def start_link(opts) do
    DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one, max_restarts: 0)
  end

  def start_dag(dag_spec) do
    DynamicSupervisor.start_child(__MODULE__, dag_spec)
  end

  def list_dags do
    DynamicSupervisor.which_children(__MODULE__)
  end

  def count_dags do
    DynamicSupervisor.count_children(__MODULE__)
  end
end
