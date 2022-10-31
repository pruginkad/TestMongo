using Npgsql;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace TSDBComparison
{
  public class QuestHelper : DbHelperBase
  {
    public QuestHelper() : base("localhost", "admin", "qdb", "quest", 8812, "TestQuest", "Quest", 1000)
    { }

    public async Task TestWrite(List<MonitoringItem> items)
    {
      Console.WriteLine($"{DBMSName} write is testing.");
      await CheckDatabaseConnection();
      await CreateTestTable();
      await InsertItemsBulkInSteps(items);
      Console.WriteLine($"{DBMSName} write testing completed.");
    }

    public async Task TestRead()
    {
      Console.WriteLine($"{DBMSName} read is testing.");
      await CheckDatabaseConnection();
      var items = await GetForADay(TestParams.ONE_DAY_TEST);
      WriteReadResults(TestParams.ONE_DAY_TEST, items, TestParams.ONE_DAY_TEST.Timestamp.ToString("yyyy-MM-dd"));
      Console.WriteLine("\r\n");

      items = await GetForAMonth(TestParams.ONE_MONTH_TEST);
      WriteReadResults(TestParams.ONE_MONTH_TEST, items, TestParams.ONE_MONTH_TEST.Timestamp.ToString("MMMM"));
      Console.WriteLine("\r\n");
      Console.WriteLine($"{DBMSName} read testing completed.");

      items = await GetForTheLastYear(TestParams.ONE_YEAR_TEST);
      WriteReadResults(TestParams.ONE_YEAR_TEST, items, "last year");
      Console.WriteLine($"{DBMSName} read testing completed.");
    }

    public async Task CheckDatabaseConnection()
    {
      var time1 = DateTime.Now;
      await using var connection = new NpgsqlConnection(ConnectionString);
      await connection.OpenAsync();
      Console.Out.WriteLine($"Connection opened for {(DateTime.Now - time1).TotalMilliseconds}.");
      await using var cmd = new NpgsqlCommand("SELECT 5;", connection);
      await using var reader = await cmd.ExecuteReaderAsync();

      if (reader.HasRows)
      {
        Console.WriteLine("Connection established!");
        connection.Close();
        return;
      }
    }

    public async Task CreateTestTable()
    {
      await using var conn = new NpgsqlConnection(ConnectionString);
      await conn.OpenAsync();

      await using var cmd1 = new NpgsqlCommand($"DROP TABLE IF EXISTS {TestTableName};", conn);
      cmd1.ExecuteNonQuery();

      // TODO: check symbol type!
      await using var cmd2 = new NpgsqlCommand(
        $"CREATE TABLE IF NOT EXISTS {TestTableName}(" +
        $"ts TIMESTAMP, " +
        $"object_name STRING, " +
        $"object_type STRING, " +
        $"prop_name STRING, " +
        $"prop_value DOUBLE) " +
        $"timestamp(ts);",
        conn
      );
      cmd2.ExecuteNonQuery();
      Console.Out.WriteLine($"Finished creating the {TestTableName} table");
    }

    public async Task InsertItems(List<MonitoringItem> items)
    {
      var time1 = DateTime.Now;
      Console.Out.WriteLine($"Insert of {items.Count} started.");
      await using var conn = new NpgsqlConnection(ConnectionString);
      await conn.OpenAsync();

      foreach (var item in items)
      {
        await using var cmd = new NpgsqlCommand(
          $"INSERT INTO {TestTableName} (ts, object_name, object_type, prop_name, prop_value) " +
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

      Console.Out.WriteLine($"Insert of {items.Count} completed for {(DateTime.Now - time1).TotalMilliseconds}.");
    }

    public async Task InsertItemsBulk(List<MonitoringItem> items)
    {
      var time1 = DateTime.Now;
      Console.Out.WriteLine($"Bulk insert of {items.Count} started.");
      await using var conn = new NpgsqlConnection(ConnectionString);
      await conn.OpenAsync();

      var values = string.Join(
        ",\n",
        items.Select(
          item =>
            @$"(to_timestamp('{item.Timestamp.ToString("yyyy-MM-ddTHH:mm:ss")}', 'yyyy-MM-ddTHH:mm:ss'),
                '{item.ObjectName}', '{item.ObjectType}', '{item.PropName}', {item.PropValue.ToString(CultureInfo.InvariantCulture)})"
        )
      );
      
      using var cmd = new NpgsqlCommand(
        @$"INSERT INTO {TestTableName} (ts, object_name, object_type, prop_name, prop_value)
            VALUES " + values,
        conn
      );
      cmd.ExecuteNonQuery();

      Console.Out.WriteLine($"Bulk insert of {items.Count} completed for {(DateTime.Now - time1).TotalMilliseconds}.");
    }

    public async Task<List<Record>> GetForADay(MonitoringItem param)
    {
      var sql = $@"
    SELECT ts, avg(prop_value), max(prop_value), min(prop_value)
    FROM {TestTableName} timestamp(ts)
    WHERE object_name = @object_name AND object_type = @object_type AND prop_name = @prop_name AND
          ts IN '{param.Timestamp.ToString("yyyy-MM-dd")};1d'
    SAMPLE BY 1h";

      return await Execute(param.ObjectName, param.ObjectType, param.PropName, sql);
    }

    public async Task<List<Record>> GetForAMonth(MonitoringItem param)
    {
      var month = param.Timestamp.Month;
      var sql = $@"
    SELECT ts, avg(prop_value), max(prop_value), min(prop_value)
    FROM {TestTableName} timestamp(ts)
    WHERE object_name = @object_name AND object_type = @object_type AND prop_name = @prop_name AND
          ts IN '2022-{(month < 10 ? "0" + month : month.ToString())}-01;1M'
    SAMPLE BY 1d";

      return await Execute(param.ObjectName, param.ObjectType, param.PropName, sql);
    }

    public async Task<List<Record>> GetForTheLastYear(MonitoringItem param)
    {
      var now = DateTime.Now;
      var sql = $@"
    SELECT ts, avg(prop_value), max(prop_value), min(prop_value)
    FROM {TestTableName} timestamp(ts)
    WHERE object_name = @object_name AND object_type = @object_type AND prop_name = @prop_name AND
          ts IN '{now.AddYears(-1).ToString("yyyy-MM-dd")};1y'
    SAMPLE BY 1M";

      return await Execute(param.ObjectName, param.ObjectType, param.PropName, sql);
    }

    public async Task InsertItemsBulkInSteps(List<MonitoringItem> items)
    {
      var k =
        (items.Count / StepForMultipleInserts) +
        (items.Count % StepForMultipleInserts == 0 ? 0 : 1);

      for (var i = 0; i < k; i++)
      {
        var itemsToInsert = items.GetRange(
          i * StepForMultipleInserts,
          Math.Min(StepForMultipleInserts, items.Count - i * StepForMultipleInserts)
        );
        await InsertItemsBulk(itemsToInsert);
      }
    }

    protected override string GetConnectionString()
    {
      return $@"host={Host};port={Port};username={User};password={Password};database={DBname};
ServerCompatibilityMode=NoTypeLoading;";
    }

    protected async Task<List<Record>> Execute(string objName, string objType, string propName, string sql)
    {
      var result = new List<Record>();
      await using var conn = new NpgsqlConnection(ConnectionString);
      await conn.OpenAsync();
      await using var cmd = new NpgsqlCommand(sql, conn);
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
  }
}
