defmodule Balsam.ETLJob do
  @moduledoc """
  Behavior for ETL jobs that should be registered with the orchestrator.
  """

  @callback run(progress_callback :: function() | nil) :: {:ok, any()} | {:error, any()}
  @callback job_config() :: map()
  @callback validate_config(config :: map()) :: :ok | {:error, String.t()}

  @optional_callbacks [validate_config: 1]

  def default_config do
    %{
      enable_progress: false,
      max_retries: 3,
      timeout: :timer.minutes(10),
      schedule: :manual
    }
  end

  def merge_config(job_config) when is_map(job_config) do
    Map.merge(default_config(), job_config)
  end
  def merge_config(_), do: default_config()
end
