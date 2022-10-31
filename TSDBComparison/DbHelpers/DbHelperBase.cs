using System;
using System.Collections.Generic;

namespace TSDBComparison
{
  public abstract class DbHelperBase
  {
    protected readonly int StepForMultipleInserts = 10000;

    protected readonly string Host;
    protected readonly string User;
    protected readonly string DBname;
    protected readonly string Password;
    protected readonly int Port;
    protected readonly string ConnectionString;
    protected readonly string TestTableName;
    protected readonly string DBMSName;

    public DbHelperBase(
      string host,
      string user,
      string dbName,
      string password,
      int port,
      string tableName,
      string dBMSName,
      int? stepForMultipleInserts = null
    )
    {
      Host = host;
      User = user;
      DBname = dbName;
      Password = password;
      Port = port;
      TestTableName = tableName;
      DBMSName = dBMSName;

      if (stepForMultipleInserts.HasValue)
        StepForMultipleInserts = stepForMultipleInserts.Value;

      ConnectionString = GetConnectionString();
    }

    protected abstract string GetConnectionString();

    protected void WriteReadResults(MonitoringItem param, List<Record> data, string period)
    {
      Console.WriteLine($"Period: {period}, {data.Count} records. " +
        $"Entity= {param.ObjectName} {param.ObjectType}, Property= {param.PropName}.");

      for (var i = 0; i < data.Count; i++)
      {
        Console.WriteLine(
          $"{data[i].Timestamp.ToString("yyyy/MM/dd")}: {data[i].AvgValue}, {data[i].MinValue}, {data[i].MaxValue}"
        );
      }
    }
  }
}
