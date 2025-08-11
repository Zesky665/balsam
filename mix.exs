defmodule Balsam.MixProject do
  use Mix.Project

  def project do
    [
      app: :balsam,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),  # Add this line
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Balsam.Application, []}
    ]
  end

  # Add this function inside the module
  defp elixirc_paths(:test), do: ["lib", "etl", "test/support"]
  defp elixirc_paths(_), do: ["lib", "etl"]

  defp deps do
    [
      # Core ETL dependencies
      {:req, "~> 0.5.0"},
      {:explorer, "~> 0.11.0"},

      # Database dependencies
      {:ecto_sql, "~> 3.12"},
      {:ecto_sqlite3, "~> 0.16"},

      # JSON handling
      {:jason, "~> 1.4"},

      # Development and test dependencies
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      "ecto.setup": ["ecto.create", "ecto.migrate"],
      "ecto.reset": ["ecto.drop", "ecto.setup"],
      test: ["ecto.create --quiet", "ecto.migrate --quiet", "test"]
    ]
  end
end
