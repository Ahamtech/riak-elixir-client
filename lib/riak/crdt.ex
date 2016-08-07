defmodule Riak.CRDT do
  @moduledoc """
  Common CRDT module
  """
  require Record

  @crdts [
    {:set, Riak.CRDT.Set, :is_list},
    {:map, Riak.CRDT.Map, :is_map},
    {:counter, Riak.CRDT.Counter, :is_integer},
    {:register, Riak.CRDT.Register, :is_binary},
    {:flag, Riak.CRDT.Flag, :is_boolean}
  ]

  # Special additional list of values handler for maps
  def type([{_,_}|_]), do: :map

  Enum.each @crdts, fn {t, m, f} ->
    def type(v) when unquote(f)(v), do: unquote(t)
    def type(unquote(m)), do: unquote(t)
  end

  # Special additional list of values handler for maps
  def new({k, [{_,_}|_]=v}), do: {to_crdt_key(k, :map), new(v)}
  def new([{_,_}|_]=v), do: Riak.CRDT.Map.new(v)

  def new([first|_]=v) when not is_binary(first), do: Enum.map(v, fn iv -> new(iv) end)

  Enum.each @crdts, fn {t, m, f} ->
    def new(v) when unquote(f)(v), do: unquote(m).new(v)
    def new({k, v}) when unquote(f)(v),
      do: {to_crdt_key(k, unquote(t)), new(v)}
  end

  def new({k, [{_,_}|_]=v}, c), do: {to_crdt_key(k, :map), new(v, c)}
  def new([{_,_}|_]=v, c), do: Riak.CRDT.Map.new(v, c)

  def new([first|_]=v, c) when not is_binary(first), do: Enum.map(v, fn iv -> new(iv, c) end)

  Enum.each @crdts, fn {t, m, f} ->
    def new(v, c) when unquote(f)(v), do: unquote(m).new(v, c)
    def new({k, v}, c) when unquote(f)(v),
      do: {to_crdt_key(k, unquote(t)), new(v, c)}
  end

  Enum.each @crdts, fn {t, m, f} ->
    def value(v) when unquote(f)(v), do: v
  end

  def into(map, data), do: Riak.CRDT.Map.into(map, data)

  def to_crdt_key({k, t}, t), do: to_crdt_key(k, t)
  def to_crdt_key(k, t), do: {_to_crdt_key(k), t}
  def to_crdt_key({k, t}), do: to_crdt_key(k, t)

  def _to_crdt_key(k) when is_binary(k), do: k
  def _to_crdt_key(k) when is_atom(k), do: Atom.to_string(k)
end
