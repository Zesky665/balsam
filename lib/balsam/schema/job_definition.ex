defmodule Balsam.Schema.JobDefinition do
  use Ecto.Schema
  import Ecto.Changeset
  require Logger

  @moduledoc """
  Persistent storage for job definitions.
  Jobs registered here will survive orchestrator restarts.
  """

  schema "job_definitions" do
    field :job_id, :string
    field :module, :string
    field :function, :string
    field :args, :string  # JSON string
    field :schedule, :string  # JSON string
    field :enable_progress, :boolean, default: false
    field :max_retries, :integer, default: 3
    field :timeout, :string, default: "infinity"
    field :active, :boolean, default: true
    field :last_run, :integer  # timestamp in milliseconds

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(job_def, attrs) do
    job_def
    |> cast(attrs, [:job_id, :module, :function, :args, :schedule, :enable_progress,
                    :max_retries, :timeout, :active, :last_run])
    |> validate_required([:job_id, :module, :function])
    |> unique_constraint(:job_id)
  end

  def from_config(job_id, config) do
    %{
      job_id: to_string(job_id),
      module: to_string(config.module),
      function: to_string(config.function),
      args: inspect(config.args || []),
      schedule: inspect(config[:schedule] || :manual),
      enable_progress: config[:enable_progress] || false,
      max_retries: config[:max_retries] || 3,
      timeout: to_string(config[:timeout] || :infinity),
      active: true,
      last_run: config[:last_run]
    }
  end

  def to_config(job_def) do
    try do
      {args, _} = Code.eval_string(job_def.args)
      {schedule, _} = Code.eval_string(job_def.schedule)

      timeout = case job_def.timeout do
        "infinity" -> :infinity
        other -> String.to_integer(other)
      end

      # Safely convert module and function to atoms
      module_atom = case job_def.module do
        "Elixir." <> _ = full_name -> String.to_existing_atom(full_name)
        name -> String.to_existing_atom("Elixir.#{name}")
      end

      config = %{
        module: module_atom,
        function: String.to_existing_atom(job_def.function),
        args: args,
        schedule: schedule,
        enable_progress: job_def.enable_progress,
        max_retries: job_def.max_retries,
        timeout: timeout,
        last_run: job_def.last_run
      }

      {:ok, config}
    rescue
      error ->
        Logger.error("Failed to convert job definition to config: #{inspect(error)}")
        {:error, error}
    end
  end
end
