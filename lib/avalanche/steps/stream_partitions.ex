defmodule Avalanche.Steps.StreamPartitions do
  @moduledoc """
  A custom `Req` pipeline step to stream partitions of data from a statement execution.

  This module implements streaming of partitioned data responses from Snowflake using the SQL API.
  It ensures partitions are retrieved and processed in the correct order.

  See: https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses
  """

  require Logger

  @doc """
  Attach the streaming step to a Req pipeline.

  ## Options

    * `:max_concurrency` - sets the maximum number of tasks to run at the same time.
      Defaults to `System.schedulers_online/0`.

    * `:timeout` - the maximum amount of time to wait (in milliseconds) for each partition.
      Defaults to 2 minutes.
  """
  def attach(%Req.Request{} = request, options \\ []) do
    request
    |> Req.Request.register_options([:max_concurrency, :timeout, :params])
    |> Req.Request.merge_options(options)
    |> Req.Request.append_response_steps(stream_partitions: &stream_partitions/1)
  end

  def stream_partitions(request_response)

  def stream_partitions({request, %{status: 200, body: %{"resultSetMetaData" => metadata} = body} = response}) do
    options = request.options || %{}
    _max_concurrency = Map.get(options, :max_concurrency, System.schedulers_online())
    timeout = Map.fetch!(options, :timeout)

    path = Map.get(body, "statementStatusUrl")
    data = Map.get(body, "data", [])

    row_types = Map.get(metadata, "rowType", [])
    partitions = Map.get(metadata, "partitionInfo", [])

    partition_stream =
      case {path, partitions} do
        {nil, _} ->
          []

        {_, []} ->
          []

        {_, "0"} ->
          []

        {path, [_head | rest]} when is_binary(path) ->
          tasks =
            rest
            |> Stream.with_index(1)
            |> Stream.map(fn {_info, partition} ->
              build_status_request(request, path, partition, row_types)
            end)
            |> Enum.map(fn req ->
              Task.Supervisor.async_nolink(Avalanche.TaskSupervisor, fn ->
                Req.Request.run_request(req)
              end)
            end)

          tasks
          |> Task.yield_many(timeout)
          |> Enum.map(fn {task, result} ->
            case result do
              {:ok, value} ->
                value

              nil ->
                Task.shutdown(task, :brutal_kill)
                error_response("Task timeout")

              {:exit, reason} ->
                error_response(reason)
            end
          end)
          |> Stream.map(&handle_partition_response/1)
      end

    {request, reduce_responses(response, data, partition_stream)}
  end

  def stream_partitions({request, %{status: 200, body: ""} = response}) do
    {request, response}
  end

  def stream_partitions({request, %{status: 200} = response}) do
    {request, response}
  end

  def stream_partitions(request_response), do: request_response

  # Private helpers

  defp build_status_request(%Req.Request{} = request, path, partition, row_types) do
    url = URI.parse(path)
    url = %{url | query: URI.encode_query(partition: partition)}

    request
    |> reset_req_request()
    |> Req.merge(
      method: :get,
      body: "",
      url: url
    )
    |> Req.Request.put_private(:avalanche_row_types, row_types)
  end

  defp reset_req_request(request), do: %{request | current_request_steps: Keyword.keys(request.request_steps)}

  defp handle_partition_response(response) do
    case response do
      {:ok, {_request, %Req.Response{} = response}} ->
        response

      {:ok, {_request, exception}} ->
        error_response(exception)

      {:exit, reason} ->
        error_response(reason)

      other ->
        error_response(other)
    end
  end

  defp error_response(reason) do
    %Req.Response{
      status: 500,
      body: %{
        "message" => "Error retrieving partition",
        "data" => %{},
        "code" => "PARTITION_ERROR",
        "error" => inspect(reason)
      }
    }
  end

  defp reduce_responses(response, data, partition_stream) when is_list(partition_stream) do
    partition_data =
      partition_stream
      |> Stream.filter(&(&1.status == 200))
      |> Stream.flat_map(&Map.get(&1.body, "data", []))

    %{response | body: Map.put(response.body, "data", Stream.concat([data], partition_data))}
  end

  defp reduce_responses(response, _data, _partition_stream), do: response
end
