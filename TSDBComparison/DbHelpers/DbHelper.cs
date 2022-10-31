using System;
using System.Collections.Generic;

namespace TSDBComparison
{
  public abstract class DbHelper : DbHelperBase
  {
    public DbHelper(string host, string user, string dbName, string password, int port, string tableName, string dBMSName)
      : base(host, user, dbName, password, port, tableName, dBMSName) { }

    public void TestWrite(List<MonitoringItem> items)
    {
      Console.WriteLine($"{DBMSName} write is testing.");
      CheckDatabaseConnection();
      CreateTestTable();
      ClearTestTable();
      InsertItemsBulkInSteps(items);
      Console.WriteLine($"{DBMSName} write testing completed.");
    }

    public void TestRead()
    {
      Console.WriteLine($"{DBMSName} read is testing.");
      CheckDatabaseConnection();

      var items = GetForADay(TestParams.ONE_DAY_TEST);
      WriteReadResults(TestParams.ONE_DAY_TEST, items, TestParams.ONE_DAY_TEST.Timestamp.ToString("yyyy-MM-dd"));
      Console.WriteLine("\r\n");

      items = GetForAMonth(TestParams.ONE_MONTH_TEST);
      WriteReadResults(TestParams.ONE_MONTH_TEST, items, TestParams.ONE_MONTH_TEST.Timestamp.ToString("MMMM"));

      items = GetForTheLastYear(TestParams.ONE_YEAR_TEST);
      WriteReadResults(TestParams.ONE_YEAR_TEST, items, "last year");

      Console.WriteLine($"{DBMSName} read testing completed.");
    }

    public void InsertItemsByOneInSteps(List<MonitoringItem> items)
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
        InsertItemsByOne(itemsToInsert);
      }
    }

    public void InsertItemsBulkInSteps(List<MonitoringItem> items)
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
        InsertItemsBulk(itemsToInsert);
      }
    }

    public abstract void CheckDatabaseConnection();

    public abstract void CreateTestTable();

    public abstract void ClearTestTable();

    public abstract void InsertItemsByOne(List<MonitoringItem> items);

    public abstract void InsertItemsBulk(List<MonitoringItem> items);

    public abstract List<Record> GetForADay(MonitoringItem param);

    public abstract List<Record> GetForAMonth(MonitoringItem param);

    public abstract List<Record> GetForTheLastYear(MonitoringItem param);

    protected abstract List<Record> Execute(string objName, string objType, string propName, string sql);
  }
}
