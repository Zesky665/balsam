import Config

# Use a separate database for tests
config :balsam, Balsam.Repo,
  database: Path.expand("../balsam_test.db", __DIR__),
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 10,  # Increased from 1
  queue_target: 5000,  # Allow longer waits
  queue_interval: 1000,
  ownership_timeout: 60_000,  # 60 seconds
  timeout: 60_000,
  stacktrace: true,
  show_sensitive_data_on_connection_error: true

# Print only warnings and errors during test
config :logger, level: :warning
