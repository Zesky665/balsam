defmodule ETL.Analytics.UserBehavior.Loader do
  @moduledoc """
  Loading logic for user behavior data.
  """

  require Logger

  def load_to_analytics_warehouse(segments, metrics) do
    Logger.info("Loading user segments and metrics to warehouse")

    # Simulate warehouse loading
    :timer.sleep(2000)

    %{
      segments_loaded: length(segments),
      metrics_loaded: length(metrics),
      warehouse_batch_id: "batch_#{System.system_time(:second)}",
      loaded_at: DateTime.utc_now()
    }
  end
end
