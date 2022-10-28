using DbLayer.Services;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Threading.Tasks;
using TSDBComparison;
using static DbLayer.Services.MongoService;

namespace TestMongo
{
  internal class MongoTest
  {
    static MongoService _service = new MongoService();

    static async  Task RunTestDb(int recordsCount, int objectsCount, CancellationToken token)
    {      
      await _service.DeleteCollection();
      await _service.CreateCollection();

      var items = DataGenerator.GenerateRawDataForOneYear(recordsCount, objectsCount);
      var step = 100000;
      var k = (recordsCount / step) + (recordsCount % step == 0 ? 0 : 1);
      // Insert of 100.000 completed for 00:00:34.
      for (var i = 0; i < k; i++)
      {
        if (token.IsCancellationRequested)
        {
          break;
        }
        var curStep = Math.Min(step, items.Count);
        var t1 = DateTime.Now;
        await _service.InsertItems(items.GetRange(0, curStep));
        var t2 = DateTime.Now;
        items.RemoveRange(0, curStep);

        ConsoleWrite.WriteConsole(
        $"step:{i} ->mongo Insert {curStep} items:{(int)(t2 - t1).TotalMilliseconds}[ms] left:{items.Count},                  ",
              1);
      }
    }

    public static async Task RunTest(CancellationToken token)
    {
      //await RunTestDb(10000000, 5000, token);
      TestAvg();
    }

    static void WriteListToConsole(List<BsonDocument> list)
    {
      var jsonSettings = new JsonWriterSettings()
      {
        Indent = false
      };

      foreach ( var element in list)
      {
        Console.WriteLine(element.ToJson(jsonSettings));
      }
    }
    static void TestAvg()
    {      
      var t1 = DateTime.Now;
      var avg = _service.Agregate(
        DateTime.Today.AddDays(-1),
        DateTime.Today,
        E_GROUPBY.hours);

      var t2 = DateTime.Now;
      WriteListToConsole(avg);
      Console.WriteLine(
        $"mongo agrg for a day, group by hours:{(int)(t2 - t1).TotalMilliseconds}[ms]");
      Console.WriteLine("--------------------------------------------------");

      t1 = DateTime.Now;
      avg = _service.Agregate(
        DateTime.Today.AddDays(-7),
        DateTime.Today,
        E_GROUPBY.days);
      t2 = DateTime.Now;
      WriteListToConsole(avg);
      Console.WriteLine(
        $"mongo agrg for the last week, group by days:{(int)(t2 - t1).TotalMilliseconds}[ms]");
      Console.WriteLine("--------------------------------------------------");

      t1 = DateTime.Now;
      avg = _service.Agregate(
        DateTime.Today.AddMonths(-6),
        DateTime.Today,
        E_GROUPBY.days);
      t2 = DateTime.Now;
      //WriteListToConsole(avg);
      Console.WriteLine(
        $"mongo agrg for some month, group by days:{(int)(t2 - t1).TotalMilliseconds}[ms]");
      Console.WriteLine("--------------------------------------------------");

      t1 = DateTime.Now;
      avg = _service.Agregate(
        DateTime.Today.AddMonths(-6),
        DateTime.Today,
        E_GROUPBY.months);
      t2 = DateTime.Now;
      WriteListToConsole(avg);
      Console.WriteLine(
        $"mongo agrg for a year, group by month:{(int)(t2 - t1).TotalMilliseconds}[ms]");
      Console.WriteLine("--------------------------------------------------");
    }

  }
}
