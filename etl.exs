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

JokesETL.main()
