defmodule ETL.DataLake.DataValidator do
  require Logger

  def validate_files(source_files) do
    Logger.info("Validating #{length(source_files)} files")
    :timer.sleep(1500)

    Enum.filter(source_files, fn file ->
      file.size > 0 and file.size < 10_000_000  # Basic validation
    end)
  end
end
