defmodule Workflows.DataLake.S3Connector do
  @moduledoc """
  S3 operations for data lake ingestion.
  This is a HELPER module - NOT registered as a job.
  """

  require Logger

  def discover_new_files do
    Logger.info("Discovering new files in S3")
    :timer.sleep(1000)

    # Mock file discovery
    [
      %{bucket: "raw-data", key: "events/2024/01/events.json", size: 1024000},
      %{bucket: "raw-data", key: "users/2024/01/users.csv", size: 512000}
    ]
  end

  def ingest_files(validated_files) do
    Logger.info("Ingesting #{length(validated_files)} files to data lake")
    :timer.sleep(3000)

    Enum.map(validated_files, fn file ->
      %{file |
        ingested_at: DateTime.utc_now(),
        data_lake_path: "s3://datalake/#{file.key}",
        status: :ingested
      }
    end)
  end
end
