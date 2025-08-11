defmodule ETL.Jokes.JokesETL do
  alias Explorer.DataFrame, as: DF
  import Enum
  import Req

  @moduledoc """
  Simple jokes ETL pipeline.
  Fetches programming jokes from the API and exports to CSV.
  """

  def fetch_data(url) do
    get!(url).body
  end

  def transform_data(raw_data) do
    transformed = map(raw_data, fn item ->
      %{
        id: item["id"],
        punchline: item["punchline"],
        setup: item["setup"],
        type: item["type"]
      }
    end)

    # Create DataFrame from the transformed data
    jokes = DF.new(transformed)
    DF.print(jokes)

    jokes
  end

  def export_data(data) do
    DF.to_csv(data, "data/jokes/jokes.csv")
  end

  def main() do
    main(nil)
  end

  def main(_progress_callback) do
    url = "https://official-joke-api.appspot.com/jokes/programming/ten"
    url |> fetch_data() |> transform_data() |> export_data()
    :ok
  end

  def main_long_running() do
    main_long_running(nil)
  end

  def main_long_running(_progress_callback) do
    categories = ["programming", "general", "dad", "knock-knock"]

    all_data = categories
    |> flat_map(fn category ->
      url = "https://official-joke-api.appspot.com/jokes/#{category}/ten"
      :timer.sleep(1500)  # Be nice to the API
      fetch_data(url)
    end)

    all_data |> transform_data() |> export_data()
    :ok
  end
end
