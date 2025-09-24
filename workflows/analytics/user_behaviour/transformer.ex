defmodule ETL.Analytics.UserBehavior.Transformer do
  @moduledoc """
  Transformation logic for user behavior data.
  """

  require Logger

  def clean_events(raw_events) do
    Logger.info("Cleaning #{length(raw_events)} raw events")

    raw_events
    |> Enum.filter(&valid_event?/1)
    |> Enum.map(&normalize_event/1)
  end

  def calculate_behavior_metrics(clean_events) do
    Logger.info("Calculating behavioral metrics")

    clean_events
    |> Enum.group_by(& &1.user_id)
    |> Enum.map(fn {user_id, events} ->
      %{
        user_id: user_id,
        total_events: length(events),
        event_types: events |> Enum.map(& &1.event_type) |> Enum.uniq(),
        sources_used: events |> Enum.map(& &1.source) |> Enum.uniq(),
        first_seen: events |> Enum.min_by(& &1.timestamp) |> Map.get(:timestamp),
        last_seen: events |> Enum.max_by(& &1.timestamp) |> Map.get(:timestamp),
        session_count: events |> Enum.map(&get_in(&1, [:properties, :session_id])) |> Enum.uniq() |> length()
      }
    end)
  end

  def generate_user_segments(metrics) do
    Logger.info("Generating user segments")

    Enum.map(metrics, fn user_metric ->
      segment = cond do
        user_metric.total_events > 50 -> "power_user"
        user_metric.total_events > 20 -> "regular_user"
        user_metric.total_events > 5 -> "casual_user"
        true -> "new_user"
      end

      Map.put(user_metric, :segment, segment)
    end)
  end

  defp valid_event?(event) do
    not is_nil(event.user_id) and
    not is_nil(event.event_type) and
    not is_nil(event.timestamp)
  end

  defp normalize_event(event) do
    %{event |
      event_type: String.downcase(event.event_type),
      source: String.downcase(event.source)
    }
  end
end
