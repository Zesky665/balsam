defmodule BalsamTest do
  use ExUnit.Case
  doctest Balsam

  test "greets the world" do
    assert Balsam.hello() == :world
  end
end
