defmodule Riak.CRDT.Map do
  @moduledoc """
  Encapsulates Riak maps
  """
  require Record

  @doc """
  Creates a new `map`
  """
  def new, do: :riakc_map.new
  def new(context) when is_binary(context), do: :riakc_map.new(context)
  def new([{_,_}|_]=values) do
    Enum.reduce(values, new, fn v, a ->
      {k, crdt} = Riak.CRDT.new(v)
      put(a, k, crdt)
    end)
  end
  def new(values) when is_map(values), do: new(Map.to_list(values))
  def new([{_,_}|_]=values, context) when is_binary(context) do
    Enum.map(values, fn {k, v} ->
      {Riak.CRDT.to_crdt_key(k, Riak.CRDT.type(v)), Riak.CRDT.value(Riak.CRDT.new(v, context))}
    end) |> :riakc_map.new(context)
  end
  def new(values, context) when is_map(values) and is_binary(context), do: new(Map.to_list(values), context)

  @doc """
  Get the `map` size
  """
  def size(map) when Record.is_record(map, :map), do: :riakc_map.size(map)
  def size(nil), do: {:error, :nil_object}

  @doc """
  Fetch the value associated to `key` with the `type` on `map`
  """
  def get(map, type, key) when Record.is_record(map, :map) do
    get(map, {key, type})
  end
  def get(nil, _, _), do: {:error, :nil_object}
  def get(map, {key, type}) when Record.is_record(map, :map) do
    :riakc_map.fetch({key, type}, map)
  end
  def get(nil, _), do: {:error, :nil_object}

  @doc """
  Update the `key` on the `map` by passing the function `fun`
  to update the value based on the current value (if exists) as argument
  The type must be :register, :map, :set, :flag or :counter
  """
  def update(map, type, key, fun) when Record.is_record(map, :map) do
    update(map, {key, type}, fun)
  end
  def update(nil, _, _, _), do: {:error, :nil_object}
  def update(map, {key, type}, fun) when Record.is_record(map, :map)
  and is_atom(type)
  and is_binary(key)
  and is_function(fun, 1) do
    :riakc_map.update({key, type}, fun, map)
  end
  def update(nil, _, _), do: {:error, :nil_object}

  @doc """
  Update the `key` on the `map` by passing the `value`
  The value can be any other CRDT
  """
  def put(map, type, key, value) when Record.is_record(map, :map) do
    put(map, {key, type}, value)
  end
  def put(nil, _, _, _), do: {:error, :nil_object}
  def put(map, {key, type}, value) when Record.is_record(map, :map) do
    fun = fn _ -> value end
    :riakc_map.update({key, type}, fun, map)
  end
  def put(map, key, value) when Record.is_record(map, :map)
  and is_binary(key) do
    type = Riak.CRDT.type(value)
    put(map, {key, type}, value)
  end
  def put(nil, _, _), do: {:error, :nil_object}

  @doc """
  Delete a `key` from the `map`
  """
  def delete(map, type, key) when Record.is_record(map, :map) do
    delete(map, {key, type})
  end
  def delete(nil, _, _), do: {:error, :nil_object}
  def delete(map, {key, type}) when Record.is_record(map, :map) do
    :riakc_map.erase({key, type}, map)
  end
  def delete(nil, _), do: {:error, :nil_object}

  @doc """
  Get the original value of the `map`
  """

  def value(map) when Record.is_record(map, :map), do: :riakc_map.value(map)
  def value(nil), do: {:error, :nil_object}

  def into([], %{}=m), do: m
  def into([{{k,:map},v}|rest], %{}=m), do: into(rest, Map.put(m, String.to_atom(k), into(v, %{})))
  def into([{{k,_},v}|rest], %{}=m), do: into(rest, Map.put(m, String.to_atom(k), v))
  def into(map, %{}=m) when Record.is_record(map, :map), do: into(value(map), m)

  def into([], m) when is_list(m), do: Enum.reverse(m)
  def into([{{k,:map},v}|rest], m) when is_list(m), do: into(rest, [{String.to_atom(k), into(v, %{})}|m])
  def into([{{k,_},v}|rest], m) when is_list(m), do: into(rest, [{String.to_atom(k), v}|m])
  def into(map, m) when is_list(m) when Record.is_record(map, :map), do: into(value(map), m)

  def into(_, _), do: {:error, :not_map}

  @doc """
  List all keys of the `map`
  """
  def keys(map) when Record.is_record(map, :map), do: :riakc_map.fetch_keys(map)
  def keys(nil), do: {:error, :nil_object}

  @doc """
  Test if the `key` is contained in the `map`
  """
  def has_key?(map, type, key) when Record.is_record(map, :map) do
    has_key?(map, {key, type})
  end
  def has_key?(nil, _, _), do: {:error, :nil_object}
  def has_key?(map, {key, type}) when Record.is_record(map, :map) do
    :riakc_map.is_key({key, type}, map)
  end
  def has_key?(nil, _), do: {:error, :nil_object}

  @doc """
  Extract the causal context from the `map`
  """
  def context(map) when Record.is_record(map, :map) do
    case map do
      {:map, _, _, _, context} ->
        case context do
          :undefined -> nil
          _ -> context
        end
      _ -> nil
    end
  end
  def context(nil), do: {:error, :nil_object}
end
