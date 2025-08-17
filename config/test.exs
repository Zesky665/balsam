import Config

# Use a separate database for tests
config :balsam, Balsam.Repo,
  database: Path.expand("../balsam_test.db", __DIR__),
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 10,
  queue_target: 5000,
  queue_interval: 1000,
  ownership_timeout: 60_000,
  timeout: 60_000,
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  # Add this line to allow spawned processes to share connections:
  pool_checkout_timeout: 60_000

# Print only warnings and errors during test
config :logger, level: :warning
