defmodule ETL.DataLake.Cataloger do
  require Logger

  def update_catalog(ingested_files) do
    Logger.info("Updating data catalog with #{length(ingested_files)} files")
    :timer.sleep(1000)

    %{
      files_cataloged: length(ingested_files),
      catalog_updated_at: DateTime.utc_now(),
      total_size_mb: Enum.sum(Enum.map(ingested_files, & &1.size)) / 1024 / 1024
    }
  end
end
