defmodule ETL.Analytics.UserBehavior.Extractor do
  @moduledoc """
  Extraction logic for user behavior data.
  This is a HELPER module - NOT registered as a job.
  """

  require Logger

  def extract_user_events do
    Logger.info("Extracting user events from multiple sources")

    # Extract from different sources
    web_events = extract_web_events()
    mobile_events = extract_mobile_events()
    api_events = extract_api_events()

    # Combine all events
    web_events ++ mobile_events ++ api_events
  end

  defp extract_web_events do
    # Simulate web analytics extraction
    :timer.sleep(1000)
    Logger.debug("Extracted web events")
    generate_mock_events("web")
  end

  defp extract_mobile_events do
    # Simulate mobile analytics extraction
    :timer.sleep(800)
    Logger.debug("Extracted mobile events")
    generate_mock_events("mobile")
  end

  defp extract_api_events do
    # Simulate API usage extraction
    :timer.sleep(600)
    Logger.debug("Extracted API events")
    generate_mock_events("api")
  end

  defp generate_mock_events(source) do
    1..100
    |> Enum.map(fn i ->
      %{
        user_id: :rand.uniform(1000),
        event_type: Enum.random(["page_view", "click", "purchase", "login"]),
        source: source,
        timestamp: DateTime.utc_now() |> DateTime.add(-:rand.uniform(86400), :second),
        properties: %{session_id: "sess_#{i}", page: "/page_#{:rand.uniform(10)}"}
      }
    end)
  end
end
