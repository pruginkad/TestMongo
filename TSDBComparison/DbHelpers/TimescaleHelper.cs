using Npgsql;
using System;

namespace TSDBComparison
{
  public class TimescaleHelper : PostgresHelper
  {
    public TimescaleHelper() : base("TestTimescale", "Timescale")
    { }

    public override void CheckDatabaseConnection()
    {
      using var conn = GetConnection(true);
      var sql =
        "SELECT default_version, comment FROM pg_available_extensions " +
        "WHERE name = 'timescaledb';";

      using var cmd = new NpgsqlCommand(sql, conn);
      using NpgsqlDataReader rdr = cmd.ExecuteReader();

      if (!rdr.HasRows)
      {
        Console.WriteLine("Missing TimescaleDB extension!");
        conn.Close();
        return;
      }

      while (rdr.Read())
      {
        Console.WriteLine(
          $"TimescaleDB Default Version: {rdr.GetString(0)}\n{rdr.GetString(1)}"
        );
        Console.WriteLine("Connection established!");
      }

      conn.Close();
    }

    public override void CreateTestTable()
    {
      using var conn = GetConnection(true);
      using (var cmd = new NpgsqlCommand($"DROP TABLE IF EXISTS {TestTableName} cascade", conn))
      {
        cmd.ExecuteNonQuery();
        Console.Out.WriteLine("Finished dropping table (if existed)");
      }

      using (
        var cmd = new NpgsqlCommand(
          $"CREATE TABLE {TestTableName} (" +
          $"timestamp TIMESTAMPTZ NOT NULL, " +
          $"object_name TEXT NOT NULL, " +
          $"object_type TEXT NOT NULL, " +
          $"prop_name TEXT NOT NULL, " +
          $"prop_value DOUBLE PRECISION NULL); ",
          conn
        )
      )
      {
        cmd.ExecuteNonQuery();
        Console.Out.WriteLine($"Finished creating the {TestTableName} table");
      }

      using (var cmd = new NpgsqlCommand($"SELECT create_hypertable('{TestTableName}', 'timestamp') ", conn))
      {
        cmd.ExecuteNonQuery();
        Console.Out.WriteLine($"Finished converting the {TestTableName} table to a hypertable");
      }

      conn.Close();
    }
  }
}
