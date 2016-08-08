defmodule Riak.Datatype.Flag do
  @moduledoc """
  Encapsulates a boolean datatype inside a CRDT.Map
  """

  @opaque t :: %__MODULE__{
    value: boolean,
    op: :enable | :disable}
  defstruct value: false, op: nil

  require Record

  @doc """
  Creates a new `map`
  """
  @spec new :: t
  def new(), do: %Riak.Datatype.Flag{}

  def new(%__MODULE__{} = flag), do: flag
  def new(true), do: %Riak.Datatype.Flag{op: :enable}
  def new(false), do: %Riak.Datatype.Flag{op: :disable}
  def new(value, _context), do: %Riak.Datatype.Flag{value: value}

  @doc """
  Turns the value to true
  """
  def enable(flag) do
    %{flag | op: :enable}
  end

  @doc """
  Turns the value to false
  """
  def disable(flag) do
    %{flag | op: :disable}
  end

  @doc """
  Extracts current value of `flag`
  """
  def value(flag), do: flag.value

  def from_record({:flag, value, op, _}) do
    %Riak.Datatype.Flag{
      value: value,
      op: to_nil(op)
    }
  end

  def to_record(flag) do
    {:flag, flag.value, to_undefined(flag.op), :undefined}
  end

  def to_op(_), do: :undefined

  def to_nil(nil), do: nil
  def to_nil(:undefined), do: nil
  def to_nil(v), do: v

  def to_undefined(nil), do: :undefined
  def to_undefined(:undefined), do: :undefined
  def to_undefined(v), do: v

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(flag, opts) do
      concat ["#Riak.Datatype.Flag<", Inspect.Map.inspect(Map.from_struct(flag), opts), ">"]
    end
  end
end
