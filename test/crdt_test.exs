defmodule Riak.CRDTTest do
  use ExUnit.Case
  import Riak.CRDT

  @moduletag :riak2

  test 'type of Register' do
    assert type(Riak.CRDT.Register.new) == :register
    assert type(Riak.CRDT.Register) == :register
    assert type(%Riak.CRDT.Register{}) == :register
  end

  test 'type of Set' do
    assert type(Riak.CRDT.Set.new) == :set
    assert type(Riak.CRDT.Set) == :set
    assert type(%Riak.CRDT.Set{}) == :set
  end

  test 'type of Map' do
    assert type(Riak.CRDT.Map.new) == :map
    assert type(Riak.CRDT.Map) == :map
    assert type(%Riak.CRDT.Map{}) == :map
  end

  test 'type of Flag' do
    assert type(Riak.CRDT.Flag.new) == :flag
    assert type(Riak.CRDT.Flag) == :flag
    assert type(%Riak.CRDT.Flag{}) == :flag
  end

  test 'type of Counter' do
    assert type(Riak.CRDT.Counter.new) == :counter
    assert type(Riak.CRDT.Counter) == :counter
    assert type(%Riak.CRDT.Counter{}) == :counter
  end
end
