defmodule ETL.Shared.ProgressTracker do
  @moduledoc """
  Consistent progress tracking across ETL pipelines.
  """

  defstruct [:callback, :total_steps, :current_step, :data]

  def new(callback, total_steps) do
    %__MODULE__{
      callback: callback,
      total_steps: total_steps,
      current_step: 0,
      data: nil
    }
  end

  def update(%__MODULE__{} = tracker, step, message, data \\ nil) do
    if tracker.callback do
      tracker.callback.(step, tracker.total_steps, message)
    end

    %{tracker | current_step: step, data: data}
  end

  def complete(%__MODULE__{} = tracker, message) do
    update(tracker, tracker.total_steps, message, tracker.data)
  end
end
