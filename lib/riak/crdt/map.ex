defmodule Riak.CRDT.Map do
  @moduledoc """
  Encapsulates Riak maps
  """

  @opaque t :: %__MODULE__{
    value: map,
    updates: map,
    removes: MapSet,
    context: binary | nil}
  defstruct value: %{}, updates: %{}, removes: MapSet.new, context: nil

  require Record

  @doc """
  Creates a new `map`
  """
  @spec new :: t
  def new(), do: %Riak.CRDT.Map{}

  def new(%__MODULE__{} = map), do: map
  def new(enumerable) do
    updates = Enum.reduce(enumerable, %{}, fn ({k, v}, a) ->
      Map.put(a, map_key(k), Riak.CRDT.new(v))
    end)
    %Riak.CRDT.Map{
      updates: updates
    }
  end
  def new(enumerable, context) do
    value = Enum.reduce(enumerable, %{}, fn ({k, v}, a) ->
      Map.put(a, map_key(k), Riak.CRDT.new(v))
    end)
    %Riak.CRDT.Map{
      value: value,
      context: context
    }
  end

  def delete(map, key) do
    case get(map, map_key(key)) do
      nil -> map
      val ->
        t = Riak.CRDT.type(val)
        %{map | removes: MapSet.put(map.removes, {record_key(key), t})}
    end
  end

  def drop(map, keys) do
    Enum.reduce(keys, map, fn k,a -> delete(a, k) end)
  end

  def equal?(map1, map2), do: Map.equal?(map1.value, map2.value)

  def fetch(map, key), do: Map.fetch(map.value, map_key(key))

  def fetch!(map, key), do: Map.fetch!(map.value, map_key(key))

  def from_struct(struct) do
    %Riak.CRDT.Map{
      updates: Map.from_struct(struct)
    }
  end
  def from_struct(struct, context) do
    %Riak.CRDT.Map{
      value: Map.from_struct(struct),
      context: context
    }
  end

  def from_record({:map, value, updates, removes, context}) do
    value = Enum.reduce(to_empty(value), %{},
      fn
        ({{k,:map},v},a) ->
          m = :riakc_map.new(v, :undefined)
          Map.put(a, map_key(k), from_record(m))
        ({{k,_},v},a) ->
          Map.put(a, map_key(k), Riak.CRDT.new(v, nil))
      end)
    updates = Enum.reduce(to_empty(updates), %{},
      fn ({{k,_},v},a) ->
        Map.put(a, map_key(k), Riak.CRDT.new(v, context))
      end)
    removes = MapSet.new(removes)
    %Riak.CRDT.Map{
      value: value,
      updates: updates,
      removes: removes,
      context: context
    }
  end

  def to_empty(nil), do: []
  def to_empty(:undefined), do: []
  def to_empty(v), do: v

  def to_nil(nil), do: nil
  def to_nil(:undefined), do: nil
  def to_nil(v), do: v

  def to_undefined(nil), do: :undefined
  def to_undefined(:undefined), do: :undefined
  def to_undefined(v), do: v

  def to_record(map) do
    value = Enum.map(map.value, fn {k, v} ->
      {{record_key(k), Riak.CRDT.type(v.__struct__)},
       v.__struct__.value(v)}
    end)
    updates = Enum.map(map.updates, fn {k, v} ->
      {{record_key(k), Riak.CRDT.type(v.__struct__)},
       v.__struct__.to_record(v)}
    end)
    removes = MapSet.to_list(map.removes)
    {:map, value, updates, removes, to_undefined(map.context)}
  end

  def record_key({k, _}) when is_binary(k), do: k
  def record_key(k) when is_atom(k), do: Atom.to_string(k)
  def record_key(k) when is_binary(k), do: k

  def map_key(k) when is_atom(k), do: k
  def map_key(k) when is_binary(k), do: String.to_atom(k)
  def map_key({k,_}) when is_binary(k), do: String.to_atom(k)

  def get(map, key, default \\ nil) do
    case get_struct(map, key, default) do
      nil -> nil
      v -> v.__struct__.value(v)
    end
  end

  def get_struct(map, key, default \\ nil) do
    case Map.get(map.value, map_key(key), default) do
      nil -> nil
      v -> v
    end
  end

  def has_key?(map, key), do: Map.has_key?(map.value, map_key(key))

  def keys(map), do: Map.keys(map.value)

  def put(map, key, val) do
    %{map | updates: Map.put(map.updates, map_key(key), val)}
  end

  def to_list(map), do: Map.to_list(map.value)

  def update(map, key, initial, fun) do
    key = map_key(key)
    case get(map, key) do
      nil ->
        %{map | updates: Map.update(map.updates, key, initial, fun)}
      val ->
        map = case Map.has_key?(map.updates, key) do
                true ->
                  map
                _ ->
                  put(map, key, val)
              end
        %{map | updates: Map.update(map.updates, key, initial, fun)}
    end
  end

  def update!(map, key, fun) do
    key = map_key(key)
    case get_struct(map, key) do
      nil ->
        %{map | updates: Map.update!(map.updates, key, fun)}
      val ->
        map = case Map.has_key?(map.updates, key) do
                true ->
                  map
                _ ->
                  put(map, key, val)
              end
        %{map | updates: Map.update!(map.updates, key, fun)}
    end
  end

  def values(map) do
    Enum.reduce(map.value, %{}, fn ({k, v}, a) ->
      Map.put(a, k, v.__struct__.value(v))
    end)
  end

  def value(map), do: values(map)

  defimpl Enumerable do
    def count(map), do: Enumerable.Map.count(map.value)
    def member?(map, item), do: Enumerable.Map.member?(map.value, item)
    def reduce(map, acc, fun), do: Enumerable.Map.reduce(map.value, acc, fun)
  end

  defimpl Collectable do
    def into(original) do
      {original, fn
        map, {:cont, {k, v}} -> Riak.CRDT.Map.put(map, k, v)
        map, :done -> map
        _, :halt -> :ok
      end}
    end
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(map, opts) do
      concat ["#Riak.CRDT.Map<", Inspect.Map.inspect(Map.from_struct(map), opts), ">"]
    end
  end
end
