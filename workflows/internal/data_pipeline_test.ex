# workflows/test/data_pipeline_test.ex
defmodule Workflows.Internal.DataPipelineTest do
  @behaviour Balsam.Workflow

  @moduledoc """
  A multi-node DAG workflow for testing DAG registration.
  Simulates a typical ETL pipeline with extract -> transform -> load pattern.
  """

  @impl true
  def workflow_config do
    %{
      name: "Test Data Pipeline",
      description: "Multi-step ETL pipeline for testing DAG functionality",
      schedule: :manual,
      max_concurrent_nodes: 2,
      max_retries: 3,
      timeout: :timer.minutes(10),
      nodes: %{
        extract_data: %{
          module: __MODULE__,
          function: :extract_step,
          args: [],
          depends_on: []
        },
        validate_data: %{
          module: __MODULE__,
          function: :validate_step,
          args: [],
          depends_on: [:extract_data]
        },
        transform_data: %{
          module: __MODULE__,
          function: :transform_step,
          args: [],
          depends_on: [:validate_data]
        },
        load_data: %{
          module: __MODULE__,
          function: :load_step,
          args: [],
          depends_on: [:transform_data]
        },
        cleanup: %{
          module: __MODULE__,
          function: :cleanup_step,
          args: [],
          depends_on: [:load_data]
        }
      }
    }
  end

  # Individual step functions for the DAG

  def extract_step(progress_callback \\ nil) do
    if progress_callback, do: progress_callback.(1, 1, "Extracting test data")

    :timer.sleep(50)  # Simulate work

    {:ok, %{
      step: :extract,
      records_extracted: 100,
      source: "test_database",
      timestamp: DateTime.utc_now()
    }}
  end

  def validate_step(progress_callback \\ nil) do
    if progress_callback, do: progress_callback.(1, 1, "Validating extracted data")

    :timer.sleep(30)  # Simulate validation

    {:ok, %{
      step: :validate,
      records_validated: 95,
      invalid_records: 5,
      validation_rules_applied: ["not_null", "format_check", "range_check"]
    }}
  end

  def transform_step(progress_callback \\ nil) do
    if progress_callback, do: progress_callback.(1, 1, "Transforming data")

    :timer.sleep(80)  # Simulate transformation

    {:ok, %{
      step: :transform,
      records_transformed: 95,
      transformations_applied: ["normalize", "enrich", "aggregate"],
      output_format: "json"
    }}
  end

  def load_step(progress_callback \\ nil) do
    if progress_callback, do: progress_callback.(1, 1, "Loading data to target")

    :timer.sleep(60)  # Simulate loading

    {:ok, %{
      step: :load,
      records_loaded: 95,
      target: "data_warehouse",
      batch_id: "batch_#{System.system_time(:second)}"
    }}
  end

  def cleanup_step(progress_callback \\ nil) do
    if progress_callback, do: progress_callback.(1, 1, "Cleaning up temporary resources")

    :timer.sleep(20)  # Simulate cleanup

    {:ok, %{
      step: :cleanup,
      temp_files_removed: 3,
      cache_cleared: true,
      pipeline_completed_at: DateTime.utc_now()
    }}
  end
end
