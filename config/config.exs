import Config

# Configure the repository
config :balsam, Balsam.Repo,
  database: Path.expand("../balsam_#{config_env()}.db", Path.dirname(__ENV__.file)),
  pool_size: 5,
  stacktrace: true,
  show_sensitive_data_on_connection_error: true

# Configure logging
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id, :job_id, :job_run_id]

# Configure Ecto
config :balsam, ecto_repos: [Balsam.Repo]

# Import environment specific config
import_config "#{config_env()}.exs"
