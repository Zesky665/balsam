# defmodule JokesETL do
#   alias Explorer.DataFrame, as: DF
#   import Enum
#   import Req
#   import Explorer


#   def fetch_data(url) do
#     get!(url).body
#   end

#   def transform_data(raw_data) do
#     transformed = map(raw_data, fn item ->
#       %{
#         id: item["id"],
#         punchline: item["punchline"],
#         setup: item["setup"],
#         type: item["type"]
#       }
#     end)

#     # Create DataFrame from the transformed data
#     jokes = DF.new(transformed)
#     DF.print(jokes)

#     jokes
#   end

#   def export_data(data) do
#     DF.to_csv(data, "jokes.csv")
#   end


#   def main() do
#     url = "https://official-joke-api.appspot.com/jokes/programming/ten"
#     url |> fetch_data |> transform_data |> export_data
#     :ok
#   end
# end


# # Run the main function
# JokesETL.main()

defmodule JokesETL do
  alias Explorer.DataFrame, as: DF
  import Enum
  import Req
  require Logger

  @doc """
  Enhanced version of your original ETL with optional progress tracking.
  This version supports both main() and main(progress_callback) calls.
  """
  def main() do
    main(nil)
  end

  def main(progress_callback) do
    Logger.info("Starting JokesETL process")

    update_progress(progress_callback, 0, 3, "Starting data fetch...")

    url = "https://official-joke-api.appspot.com/jokes/programming/ten"
    raw_data = fetch_data(url)

    update_progress(progress_callback, 1, 3, "Data fetched, starting transformation...")

    transformed_data = transform_data(raw_data)

    update_progress(progress_callback, 2, 3, "Data transformed, exporting...")

    export_data(transformed_data)

    update_progress(progress_callback, 3, 3, "ETL process completed!")

    Logger.info("JokesETL completed successfully")
    :ok
  end

  @doc """
  Long-running version that processes multiple sources.
  This version supports both main_long_running() and main_long_running(progress_callback) calls.
  """
  def main_long_running() do
    main_long_running(nil)
  end

  def main_long_running(progress_callback) do
    Logger.info("Starting long-running JokesETL process")

    # Multiple joke categories to process
    categories = ["programming", "general", "dad", "knock-knock"]
    total_steps = length(categories) + 2  # categories + transform + export

    update_progress(progress_callback, 0, total_steps, "Starting multi-category fetch...")

    # Fetch from multiple sources
    all_data = categories
    |> with_index()
    |> flat_map(fn {category, index} ->
      update_progress(progress_callback, index + 1, total_steps,
        "Fetching #{category} jokes (#{index + 1}/#{length(categories)})")

      url = "https://official-joke-api.appspot.com/jokes/#{category}/ten"

      # Add some delay to simulate long-running processing
      :timer.sleep(1500)

      fetch_data(url)
    end)

    update_progress(progress_callback, total_steps - 1, total_steps,
      "Processing #{length(all_data)} jokes...")

    # Transform all the data
    transformed_data = transform_data(all_data)

    update_progress(progress_callback, total_steps, total_steps, "Exporting final dataset...")

    # Export with timestamp
    timestamp = DateTime.utc_now() |> DateTime.to_iso8601() |> String.replace(":", "-")
    export_data(transformed_data, "jokes_#{timestamp}.csv")

    Logger.info("Long-running JokesETL completed with #{DF.n_rows(transformed_data)} records")
    :ok
  end

  def fetch_data(url) do
    Logger.debug("Fetching data from: #{url}")

    case get(url) do
      {:ok, response} ->
        response.body

      {:error, error} ->
        Logger.warning("Failed to fetch from #{url}: #{inspect(error)}")
        []
    end
  end

  def transform_data(raw_data) when is_list(raw_data) do
    Logger.debug("Transforming #{length(raw_data)} records")

    transformed = map(raw_data, fn item ->
      %{
        id: item["id"],
        punchline: item["punchline"],
        setup: item["setup"],
        type: item["type"],
        processed_at: DateTime.utc_now() |> DateTime.to_iso8601()
      }
    end)

    # Create DataFrame from the transformed data
    jokes = DF.new(transformed)

    if DF.n_rows(jokes) > 0 do
      Logger.debug("Created DataFrame with #{DF.n_rows(jokes)} rows")
      DF.print(jokes, limit: 5)  # Print first 5 rows for verification
    end

    jokes
  end
  def transform_data(_), do: DF.new([])  # Handle non-list input

  def export_data(data, filename \\ "jokes.csv") do
    Logger.info("Exporting data to #{filename}")
    DF.to_csv(data, filename)
    Logger.info("Export completed: #{filename}")
  end

  # Simple progress helper
  defp update_progress(nil, _current, _total, _message), do: :ok
  defp update_progress(callback, current, total, message) when is_function(callback) do
    callback.(current, total, message)
  end
end
