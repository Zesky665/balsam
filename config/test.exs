import Config

# Use a separate database for tests
config :balsam, Balsam.Repo,
  database: ":memory:",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 1

# Print only warnings and errors during test
config :logger, level: :warning
