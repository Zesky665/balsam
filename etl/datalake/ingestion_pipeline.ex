defmodule ETL.DataLake.IngestionPipeline do
  @moduledoc """
  Main ingestion pipeline for data lake operations.
  This is the MAIN job that gets registered.
  """

  @behaviour Balsam.ETLJob

  alias ETL.DataLake.{S3Connector, DataValidator, Cataloger}
  require Logger

  @impl Balsam.ETLJob
  def run(progress_callback \\ nil) do
    Logger.info("Starting Data Lake Ingestion Pipeline")

    try do
      total_steps = 4

      if progress_callback, do: progress_callback.(1, total_steps, "Discovering source files")
      source_files = S3Connector.discover_new_files()

      if progress_callback, do: progress_callback.(2, total_steps, "Validating data quality")
      validated_files = DataValidator.validate_files(source_files)

      if progress_callback, do: progress_callback.(3, total_steps, "Ingesting to data lake")
      ingested_files = S3Connector.ingest_files(validated_files)

      if progress_callback, do: progress_callback.(4, total_steps, "Updating data catalog")
      result = Cataloger.update_catalog(ingested_files)

      Logger.info("Data Lake Ingestion completed successfully")
      {:ok, result}
    rescue
      error ->
        Logger.error("Data Lake Ingestion failed: #{Exception.message(error)}")
        {:error, error}
    end
  end

  @impl Balsam.ETLJob
  def job_config do
    %{
      enable_progress: true,
      max_retries: 1,
      timeout: :timer.hours(2),
      schedule: {:interval, :timer.hours(6)}
    }
  end
end
