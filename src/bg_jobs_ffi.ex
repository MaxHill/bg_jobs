defmodule BgJobsFfi do
  require Logger

  def next_run_date(cron_expr, timestamp) do
    # Parse the cron expression (e.g., "*/2" -> ~e[*/2])
    cron = Crontab.CronExpression.Parser.parse!(cron_expr)

    naive_date = NaiveDateTime.from_erl!(timestamp)
    # Logger.info("Parsed NaiveDateTime: #{inspect(naive_date)}")

    # Get the next run date
    next_run = Crontab.Scheduler.get_next_run_date(cron, naive_date)

    # Return result as an ISO8601 string or nil if there is no next run
    case next_run do
      nil -> nil
      {:ok, next} -> 
        NaiveDateTime.to_erl next
    end
  end
end
