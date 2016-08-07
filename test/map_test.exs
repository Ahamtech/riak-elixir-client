defmodule Riak.CRDT.MapTest do
  require IEx
  use Riak.Case
  alias Riak.CRDT.Map
  alias Riak.CRDT.Register
  alias Riak.CRDT.Flag
  alias Riak.CRDT.Counter
  alias Riak.CRDT.Set

  @moduletag :riak2

  test "create, update and find a map with other CRDTs" do
    key = Riak.Helper.random_key

    reg_data = "Register data"
    reg = Register.new(reg_data)
    reg_key = "register_key"

    flag = Flag.new |> Flag.enable
    flag_key = "flag_key"

    counter = Counter.new |> Counter.increment
    counter_key = "counter_key"

    set = Set.new |> Set.put("foo")
    set_key = "set_key"

    Map.new
      |> Map.put(reg_key, reg)
      |> Map.put(flag_key, flag)
      |> Map.put(counter_key, counter)
      |> Map.put(set_key, set)
      |> Riak.update("maps", "bucketmap", key)

    map_rec = Riak.find("maps", "bucketmap", key) |> Map.to_record
    map_val = :riakc_map.value(map_rec)

    map_keys = :orddict.fetch_keys(map_val)
    assert {"counter_key", :counter} in map_keys
    assert {"flag_key", :flag} in map_keys
    assert {"register_key", :register} in map_keys
    assert {"set_key", :set} in map_keys

    assert :orddict.size(map_val) == 4

    data = :orddict.to_list(map_val)
    assert {{reg_key, :register}, reg_data} in data
    assert {{flag_key, :flag}, true} in data
    assert {{counter_key, :counter}, 1} in data
    assert {{set_key, :set}, ["foo"]} in data
  end

  test "create, update and find nested maps with new structure" do
    key = Riak.Helper.random_key

    Riak.CRDT.new(%{nested_key: %{flag_key: true}})
    |> Riak.update("maps", "bucketmap", key)

    value_map = Riak.find("maps", "bucketmap", key)
    |> Map.value

    assert length(Elixir.Map.to_list(value_map)) == 1
    nested_map = value_map |> Elixir.Map.get(:nested_key)
    assert Enum.count(nested_map) == 1
    assert nested_map |> Elixir.Map.get(:flag_key) == true

    assert Elixir.Map.has_key?(value_map, :nested_key) == true

    Riak.find("maps", "bucketmap", key)
    |> Map.delete(:nested_key)
    |> Riak.update("maps", "bucketmap", key)

    exists = Riak.find("maps", "bucketmap", key)
    |> Map.has_key?(:nested_key)

    assert exists == false
  end

  test "create, update and find nested maps" do
    key = Riak.Helper.random_key

    flag = Flag.new |> Flag.enable
    flag_key = "flag_key"

    nested = Map.new |> Map.put(flag_key, flag)
    nested_key = "nested_key"

    Map.new
    |> Map.put(nested_key, nested)
    |> Riak.update("maps", "bucketmap", key)

    map = Riak.find("maps", "bucketmap", key)

    value_map = map |> Map.value

    assert Enum.count(value_map) == 1
    assert Elixir.Map.get(value_map, :nested_key) == %{flag_key: true}

    exists = Riak.find("maps", "bucketmap", key)
    |> Map.has_key?({nested_key, :map})

    assert exists == true

    Riak.find("maps", "bucketmap", key) |> Map.delete({nested_key, :map})
    |> Riak.update("maps", "bucketmap", key)

    exists = Riak.find("maps", "bucketmap", key)
    |> Map.has_key?({nested_key, :map})

    assert exists == false

  end

  test "create, update, delete map" do
    key = Riak.Helper.random_key

    Map.new
      |> Map.put("register_key", Register.new("Some Data"))
      |> Riak.update("maps", "users", key)

    reg_data = Riak.find("maps", "users", key)
      |> Map.get(:register_key)

    assert "Some Data" == reg_data

    Riak.delete("maps", "users", key)
    assert Riak.find("maps", "users", key) == nil

  end

  test "map key exists" do
    key = Riak.Helper.random_key

    Map.new
    |> Map.put("register_key", Register.new("Some Data"))
    |> Riak.update("maps", "users", key)

    exists = Riak.find("maps", "users", key)
    |> Map.has_key?({"nothere", :register})

    assert exists == false

    exists = Riak.find("maps", "users", key)
    |> Map.has_key?("register_key")

    assert exists == true

    keys = Riak.find("maps", "users", key)
    |> Map.keys()

    assert keys == [:register_key]
  end
end
