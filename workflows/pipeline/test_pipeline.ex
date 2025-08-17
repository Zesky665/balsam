defmodule ETL.Pipeline.TestPipeline do
  @behaviour Balsam.Workflow

  @impl true
  def workflow_config do
    %{
      name: "Test Multi-Step Pipeline",
      description: "Tests multi-step workflow dependencies",
      schedule: :manual,
      max_concurrent_nodes: 2,
      nodes: %{
        step1: %{
          module: __MODULE__,
          function: :step_one,
          args: [],
          depends_on: []
        },
        step2: %{
          module: __MODULE__,
          function: :step_two,
          args: [],
          depends_on: [:step1]
        },
        step3: %{
          module: __MODULE__,
          function: :step_three,
          args: [],
          depends_on: [:step2]
        }
      }
    }
  end

  # Multi-step workflows don't implement run/1 directly
  # Each step is a separate function

  def step_one(_progress_callback \\ nil) do
    IO.puts("📋 Step 1: Extracting test data...")
    :timer.sleep(1000)
    IO.puts("✅ Step 1 complete")
    {:ok, %{data: ["item1", "item2", "item3"]}}
  end

  def step_two(_progress_callback \\ nil) do
    IO.puts("🔧 Step 2: Transforming data...")
    :timer.sleep(1000)
    IO.puts("✅ Step 2 complete")
    {:ok, %{transformed: true, count: 3}}
  end

  def step_three(_progress_callback \\ nil) do
    IO.puts("💾 Step 3: Loading results...")
    :timer.sleep(1000)
    IO.puts("✅ Step 3 complete - Pipeline finished!")
    {:ok, %{loaded: true, final_result: "success"}}
  end
end
