# ETL Job Definitions
# This file is loaded by the application to register ETL jobs

import Config

config :balsam, :etl_jobs, [
  %{
    id: :jokes_etl,
    module: ETL.Jokes.JokesETL,
    function: :main,
    args: [],
    schedule: {:interval, :timer.minutes(30)},
    enable_progress: false,
    description: "Fetch programming jokes every 30 minutes"
  },
  %{
    id: :jokes_etl_long,
    module: ETL.Jokes.JokesETL,
    function: :main_long_running,
    args: [],
    schedule: {:interval, :timer.hours(6)},
    enable_progress: true,
    description: "Multi-category joke collection every 6 hours"
  },
  %{
    id: :cleanup_old_data,
    module: Balsam.JobStorage,
    function: :cleanup_old_data,
    args: [90],
    schedule: :manual,
    enable_progress: false,
    description: "Clean up job data older than 90 days"
  }
]
