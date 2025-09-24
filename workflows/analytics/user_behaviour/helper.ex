defmodule ETL.Analytics.UserBehavior.Helper do
  @moduledoc """
  Helper module for user behavior processing.
  This should NOT be registered as a job.
  """

  def process_events(events) do
    # This is just a helper function
    Enum.count(events)
  end

  def generate_report(data) do
    "Report: #{inspect(data)}"
  end
end
