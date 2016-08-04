defmodule Riak.CRDT.Register do
  @moduledoc """
  Encapsulates a binary data to be used on CRDT.Map's
  """
  require Record

  @doc """
  Creates a new register
  """
  def new, do: :riakc_register.new
  def new(context_or_value) when is_binary(context_or_value) do
    # registers cannot exist outside of maps, no context needed
    # case String.printable?(context_or_value) do
    #   true -> new |> set(context_or_value)
    #   _ -> :riakc_register.new(context_or_value)
    # end
    new |> set(context_or_value)
  end
  def new(value, context) when is_binary(value) and is_binary(context), do: :riakc_register.new(value, context)

  @doc """
  Extracts current value of `register`
  """
  def value(register) when Record.is_record(register, :register) do
    :riakc_register.value(register)
  end
  def value(nil), do: {:error, :nil_object}

  @doc """
  Set the `value` on the `register`
  """
  def set(register, value) when Record.is_record(register, :register)
                           and is_binary(value) do
    :riakc_register.set(value, register)
  end
  def set(nil, _), do: {:error, :nil_object}
end
