import Config

if config_env() == :prod do
  database_path =
    System.get_env("DATABASE_PATH") ||
    "/app/data/balsam_prod.db"

  # Ensure the database directory exists
  database_path |> Path.dirname() |> File.mkdir_p!()

  config :balsam, Balsam.Repo,
    database: database_path,
    pool_size: String.to_integer(System.get_env("POOL_SIZE") || "5")

  # Configure logging level
  log_level =
    System.get_env("LOG_LEVEL", "info")
    |> String.to_existing_atom()

  config :logger, level: log_level
end
