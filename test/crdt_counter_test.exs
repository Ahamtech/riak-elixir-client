defmodule Riak.CRDT.CounterTest do
  use Riak.Case
  alias Riak.CRDT.Counter

  @moduletag :riak2

  test "create, update and find a counter" do
    key = Riak.Helper.random_key

    Counter.new
      |> Counter.increment
      |> Counter.increment(2)
      |> Riak.CRDT.put({"counters", "bucketcounter"}, key)

    counter = Riak.CRDT.find({"counters", "bucketcounter"}, key)
      |> Counter.value

    assert counter == 3
  end
end
