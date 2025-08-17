defmodule ETL.Analytics.UserBehavior do
  @behaviour Balsam.Workflow

  @impl true
  def workflow_config do
    %{
      name: "User Behavior Analytics Pipeline",
      description: "Extract user data, analyze behavior, generate reports",
      schedule: {:interval, :timer.hours(24)},
      max_concurrent_nodes: 2,
      nodes: %{
        extract_users: %{
          module: __MODULE__,
          function: :extract_users,
          args: [],
          depends_on: []
        },
        extract_events: %{
          module: __MODULE__,
          function: :extract_events,
          args: [],
          depends_on: []
        },
        analyze_behavior: %{
          module: __MODULE__,
          function: :analyze_behavior,
          args: [],
          depends_on: [:extract_users, :extract_events]
        },
        generate_report: %{
          module: __MODULE__,
          function: :generate_report,
          args: [],
          depends_on: [:analyze_behavior]
        }
      }
    }
  end

  def extract_users(_progress_callback \\ nil) do
    IO.puts("📊 Extracting user data...")
    :timer.sleep(1000)
    {:ok, %{users_extracted: 1000}}
  end

  def extract_events(_progress_callback \\ nil) do
    IO.puts("📈 Extracting event data...")
    :timer.sleep(1000)
    {:ok, %{events_extracted: 5000}}
  end

  def analyze_behavior(_progress_callback \\ nil) do
    IO.puts("🧠 Analyzing user behavior...")
    :timer.sleep(2000)
    {:ok, %{insights: ["insight1", "insight2"]}}
  end

  def generate_report(_progress_callback \\ nil) do
    IO.puts("📋 Generating behavior report...")
    :timer.sleep(1000)
    {:ok, %{report_generated: true}}
  end
end
