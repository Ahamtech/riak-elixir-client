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
    bucket = {"maps", "bucketmap"}

    Map.new(%{register_key: "Register Data",
              flag_key: true,
              counter_key: 1,
              set_key: ["foo"]})
              |> Riak.CRDT.put(bucket, key)

    map = Riak.CRDT.find(bucket, key)
    assert Map.has_key?(map, :counter_key)
    assert Map.has_key?(map, :flag_key)
    assert Map.has_key?(map, :register_key)
    assert Map.has_key?(map, :set_key)
    assert Enum.count(map) == 4

    data = Map.value(map)
    assert data.register_key == "Register Data"
    assert data.flag_key == true
    assert data.counter_key == 1
    assert data.set_key == ["foo"]
  end

  test "create, update and find nested maps" do
    key = Riak.Helper.random_key
    bucket = {"maps", "bucketmap"}

    Riak.CRDT.new(%{nested_key: %{flag_key: true}})
    |> Riak.CRDT.put(bucket, key)

    map = Riak.CRDT.find(bucket, key)
    flag = Map.get(map, :nested_key) |> Map.get(:flag_key)
    assert Enum.count(map) == 1
    assert Flag.value(flag) == true
    assert Map.has_key?(map, :nested_key) == true

    Riak.CRDT.find(bucket, key)
    |> Map.delete(:nested_key)
    |> Riak.CRDT.update(bucket, key)

    map = Riak.CRDT.find(bucket, key)
    assert Map.has_key?(map, :nested_key) == false
  end

  test "create, update, delete map" do
    key = Riak.Helper.random_key
    bucket = {"maps", "users"}

    Map.new(%{register_key: Register.new("Some Data")})
    |> Riak.CRDT.put(bucket, key)

    reg = Riak.CRDT.find(bucket, key)
    |> Map.get(:register_key)

    assert "Some Data" == Register.value(reg)

    Riak.CRDT.delete(bucket, key)
    assert Riak.CRDT.find(bucket, key) == nil
  end

  test "map key exists" do
    key = Riak.Helper.random_key
    bucket = {"maps", "users"}

    Map.new
    |> Map.put("register_key", "Some Data")
    |> Riak.CRDT.update(bucket, key)

    map = Riak.CRDT.find(bucket, key)
    assert Map.has_key?(map, "nothere") == false
    assert Map.has_key?(map, "register_key") == true
    assert Map.keys(map) == [:register_key]
  end
end
