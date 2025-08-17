defmodule Balsam.StartupLoader do
  use GenServer
  require Logger

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    send(self(), :register_workflows)
    {:ok, %{}}
  end

  @impl true
  def handle_info(:register_workflows, state) do
    Logger.info("Starting workflow registration...")

    try do
      case attempt_registration() do
        :ok ->
          Logger.info("✅ All workflows registered successfully")
        {:error, reason} ->
          Logger.warning("⚠️  Registration failed: #{reason}")
          Process.send_after(self(), :retry, 2000)
      end
    rescue
      error ->
        Logger.error("❌ Registration error: #{inspect(error)}")
        Process.send_after(self(), :retry, 2000)
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(:retry, state) do
    case attempt_registration() do
      :ok ->
        Logger.info("✅ Workflows registered on retry")
      {:error, reason} ->
        Logger.warning("⚠️  Retry failed: #{reason}")
        Logger.info("💡 Manual: Balsam.WorkflowRegistry.register_all()")
    end

    {:noreply, state}
  end

  defp attempt_registration do
    try do
      Balsam.WorkflowRegistry.register_all()
      :ok
    rescue
      error ->
        {:error, inspect(error)}
    end
  end
end
