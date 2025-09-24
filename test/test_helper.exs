ExUnit.start()

# Start the repo for tests
{:ok, _} = Application.ensure_all_started(:balsam)

# Alternative approach if the above doesn't work:
# {:ok, _} = Balsam.Repo.start_link()

# Set up the database for tests
Ecto.Adapters.SQL.Sandbox.mode(Balsam.Repo, :manual)
