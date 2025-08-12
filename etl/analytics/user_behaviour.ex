defmodule ETL.Analytics.UserBehavior do
  @moduledoc """
  User behavior analytics ETL pipeline.
  """

  @behaviour Balsam.ETLJob

  require Logger

  @impl Balsam.ETLJob
  def run(progress_callback \\ nil) do
    Logger.info("Starting User Behavior ETL")

    try do
      if progress_callback, do: progress_callback.(1, 3, "Extracting user events")
      :timer.sleep(1000)

      if progress_callback, do: progress_callback.(2, 3, "Processing behavior patterns")
      :timer.sleep(1000)

      if progress_callback, do: progress_callback.(3, 3, "Loading analytics results")
      :timer.sleep(1000)

      result = %{
        users_processed: 1000,
        events_analyzed: 25000,
        segments_created: 5,
        completed_at: DateTime.utc_now()
      }

      Logger.info("User Behavior ETL completed successfully")
      {:ok, result}
    rescue
      error ->
        Logger.error("User Behavior ETL failed: #{Exception.message(error)}")
        {:error, error}
    end
  end

  @impl Balsam.ETLJob
  def job_config do
    %{
      enable_progress: true,
      max_retries: 2,
      timeout: :timer.minutes(15),
      schedule: {:interval, :timer.hours(6)}
    }
  end
end
