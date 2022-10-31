using Npgsql;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace TSDBComparison
{
  public class PostgresHelper : DbHelper
  {
    public PostgresHelper()
      : base("127.0.0.1", "postgres", "example", "password", 5432, "TestPostgres", "Postgres")
    { }

    public PostgresHelper(string tableName, string dBMSName)
      : base("127.0.0.1", "postgres", "example", "password", 5432, tableName, dBMSName)
    { }

    public override void CheckDatabaseConnection()
    {
      using var conn = GetConnection(true);
      var sql = "SELECT 4;";

      using var cmd = new NpgsqlCommand(sql, conn);
      using NpgsqlDataReader rdr = cmd.ExecuteReader();

      if (rdr.HasRows)
        Console.WriteLine("Connection established!");

      conn.Close();
    }

    public override void CreateTestTable()
    {
      using var conn = GetConnection(false);

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

      conn.Close();
    }

    public override void ClearTestTable()
    {
      using var conn = GetConnection(false);
      using var cmd = new NpgsqlCommand($"DELETE FROM {TestTableName}", conn);
      cmd.ExecuteNonQuery();
      conn.Close();
      Console.Out.WriteLine($"{TestTableName} table is empty.");
    }

    public override void InsertItemsByOne(List<MonitoringItem> items)
    {
      var time1 = DateTime.Now;
      Console.Out.WriteLine($"Insert of {items.Count} started.");
      using var conn = GetConnection(false);

      foreach (var item in items)
      {
        using var cmd = new NpgsqlCommand(
          $"INSERT INTO {TestTableName} (timestamp, object_name, object_type, prop_name, prop_value) " +
          $"VALUES (@timestamp, @object_name, @object_type, @prop_name, @prop_value)",
          conn
        );
        cmd.Parameters.AddWithValue("timestamp", item.Timestamp);
        cmd.Parameters.AddWithValue("object_name", item.ObjectName);
        cmd.Parameters.AddWithValue("object_type", item.ObjectType);
        cmd.Parameters.AddWithValue("prop_name", item.PropName);
        cmd.Parameters.AddWithValue("prop_value", item.PropValue);
        cmd.ExecuteNonQuery();
      }

      conn.Close();
      Console.Out.WriteLine($"Insert of {items.Count} completed for {(DateTime.Now - time1).TotalMilliseconds}.");
    }

    public override void InsertItemsBulk(List<MonitoringItem> items)
    {
      var time1 = DateTime.Now;
      Console.Out.WriteLine($"Bulk insert of {items.Count} started.");
      var values = string.Join(
        ",",
        items.Select(
          item =>
            @$"('{item.Timestamp.ToString("o", CultureInfo.InvariantCulture)}', '{item.ObjectName}', '{item.ObjectType}',
                '{item.PropName}', '{item.PropValue.ToString(CultureInfo.InvariantCulture)}')"
        )
      );

      using var conn = GetConnection(false);
      using var cmd = new NpgsqlCommand(
        @$"INSERT INTO {TestTableName} (timestamp, object_name, object_type, prop_name, prop_value)
             VALUES " + values,
        conn
      );
      cmd.ExecuteNonQuery();

      conn.Close();
      Console.Out.WriteLine($"Bulk insert of {items.Count} completed for {(DateTime.Now - time1).TotalMilliseconds}.");
    }

    public override List<Record> GetForADay(MonitoringItem param)
    {
      var sql = $@"
    SELECT
      date_trunc('hour', timestamp) AS time1,
      AVG(prop_value) AS avg_val,
      MIN(prop_value) AS min_val,
      MAX(prop_value) AS max_val
    FROM {TestTableName}
    WHERE object_name = @object_name AND object_type = @object_type AND prop_name = @prop_name AND
          timestamp >= TIMESTAMPTZ '{param.Timestamp.ToString("yyyy-MM-dd")}' AND
          timestamp < TIMESTAMPTZ '{param.Timestamp.AddDays(1).ToString("yyyy-MM-dd")}'
    GROUP BY time1, prop_name
    ORDER BY time1 ASC;";

      return Execute(param.ObjectName, param.ObjectType, param.PropName, sql);
    }

    public override List<Record> GetForAMonth(MonitoringItem param)
    {
      var month = param.Timestamp.Month;
      var sql = $@"
    SELECT
      date_trunc('day', timestamp) AS time1,
      AVG(prop_value) AS avg_val,
      MIN(prop_value) AS min_val,
      MAX(prop_value) AS max_val
    FROM {TestTableName}
    WHERE object_name = @object_name AND object_type = @object_type AND prop_name = @prop_name AND
          timestamp >= TIMESTAMPTZ '2022-{month}-01' AND
          timestamp < TIMESTAMPTZ '2022-{month + 1}-01'
    GROUP BY time1, prop_name
    ORDER BY time1 ASC;";

      return Execute(param.ObjectName, param.ObjectType, param.PropName, sql);
    }

    public override List<Record> GetForTheLastYear(MonitoringItem param)
    {
      var now = DateTime.Now;
      var sql = $@"
    SELECT
      date_trunc('month', timestamp) AS time1,
      AVG(prop_value) AS avg_val,
      MIN(prop_value) AS min_val,
      MAX(prop_value) AS max_val
    FROM {TestTableName}
    WHERE object_name = @object_name AND object_type = @object_type AND prop_name = @prop_name AND
          timestamp >= TIMESTAMPTZ '{now.AddYears(-1).ToString("yyyy-MM-dd")}' AND
          timestamp < TIMESTAMPTZ '{now.ToString("yyyy-MM-dd")}'
    GROUP BY time1, prop_name
    ORDER BY time1 ASC;";

      return Execute(param.ObjectName, param.ObjectType, param.PropName, sql);
    }

    protected override List<Record> Execute(string objName, string objType, string propName, string sql)
    {
      var result = new List<Record>();
      using var conn = GetConnection(false);
      using var cmd = new NpgsqlCommand(sql, conn);
      cmd.Parameters.AddWithValue("object_name", objName);
      cmd.Parameters.AddWithValue("object_type", objType);
      cmd.Parameters.AddWithValue("prop_name", propName);
      var time1 = DateTime.Now;
      using NpgsqlDataReader rdr = cmd.ExecuteReader();

      while (rdr.Read())
      {
        result.Add(
          new Record
          {
            Timestamp = rdr.GetDateTime(0),
            AvgValue = rdr.GetDouble(1),
            MinValue = rdr.GetDouble(2),
            MaxValue = rdr.GetDouble(3)
          }
        );
      }

      conn.Close();
      Console.Out.WriteLine($"Read completed for {(DateTime.Now - time1).TotalMilliseconds}.");

      return result;
    }

    protected override string GetConnectionString()
    {
      return $@"Server={Host};Username={User};Database={DBname};Port={Port};Password={Password};
SSLMode=Prefer";
    }

    protected  string GetConnectionStringNoDb()
    {
      return $@"Server={Host};Username={User};Port={Port};Password={Password};
SSLMode=Prefer";
    }

    protected NpgsqlConnection GetConnection(bool noDb)
    {
      var time1 = DateTime.Now;
      var connection = new NpgsqlConnection(ConnectionString);
      connection.Open();
      Console.Out.WriteLine($"Connection opened for {(DateTime.Now - time1).TotalMilliseconds}.");
      return connection;
    }
  }
}
