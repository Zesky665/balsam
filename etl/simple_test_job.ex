defmodule ETL.SimpleTestJob do
  @moduledoc """
  A simple test ETL job to verify the registration system works.
  """

  @behaviour Balsam.ETLJob

  require Logger

  @impl Balsam.ETLJob
  def run(progress_callback \\ nil) do
    Logger.info("Starting Simple Test Job")

    if progress_callback do
      progress_callback.(1, 3, "Step 1: Initializing")
      :timer.sleep(500)
      progress_callback.(2, 3, "Step 2: Processing")
      :timer.sleep(500)
      progress_callback.(3, 3, "Step 3: Completing")
      :timer.sleep(500)
    else
      :timer.sleep(1500)
    end

    Logger.info("Simple Test Job completed successfully")
    {:ok, %{message: "Test job completed", processed_at: DateTime.utc_now()}}
  end

  @impl Balsam.ETLJob
  def job_config do
    %{
      enable_progress: true,
      max_retries: 1,
      timeout: :timer.minutes(2),
      schedule: :manual
    }
  end
end
