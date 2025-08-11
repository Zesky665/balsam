import Config

config :logger, level: :info

# Configure release runtime
config :balsam, Balsam.Repo,
  database: {:system, "DATABASE_PATH", "/app/data/balsam_prod.db"},
  pool_size: {:system, :integer, "POOL_SIZE", 5}
