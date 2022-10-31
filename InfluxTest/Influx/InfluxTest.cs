

using app.Services;
using InfluxDB.Client.Core.Flux.Domain;
using System.Collections;
using System.Collections.Generic;
using TSDBComparison;
using static app.Services.InfluxDBService;

namespace TestInflux
{
  internal class InfluxTest
  {
    static InfluxDBService _service = new InfluxDBService();

    static async Task RunTestDb(int recordsCount, int objectsCount, CancellationToken token)
    {
      await _service.DeleteCollection();
      await _service.CreateCollection();

      var items = DataGenerator.GenerateRawDataForOneYear(recordsCount, objectsCount);
      var step = 10000;
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
        $"step:{i} ->influx Insert {curStep} items:{(int)(t2 - t1).TotalMilliseconds}[ms] left:{items.Count},                  ",
              1);
      }
    }

    public static async Task RunTest(CancellationToken token)
    {
      //await RunTestDb(10000000, 5000, token);
      for (int i = 0; i < 5; i++)
      {
        Console.WriteLine("**********************");
        await TestAvg();
      }
    }

    static async Task TestAvg()
    {
      var t1 = DateTime.Now;
      var avg = await _service.Agregate(
        DateTime.UtcNow.AddDays(-1),
        DateTime.UtcNow,
        E_GROUPBY.hours);

      var t2 = DateTime.Now;
      WriteListToConsole(avg);
      Console.WriteLine(
        $"influx agrg for a day, group by hours:{(int)(t2 - t1).TotalMilliseconds}[ms]");
      Console.WriteLine("--------------------------------------------------");

      t1 = DateTime.Now;
      avg = await _service.Agregate(
        DateTime.UtcNow.AddDays(-7),
        DateTime.UtcNow,
        E_GROUPBY.days);
      t2 = DateTime.Now;
      WriteListToConsole(avg);
      Console.WriteLine(
        $"influx agrg for the last week, group by days:{(int)(t2 - t1).TotalMilliseconds}[ms]");
      Console.WriteLine("--------------------------------------------------");

      t1 = DateTime.Now;
      avg = await _service.Agregate(
        DateTime.UtcNow.AddMonths(-6),
        DateTime.UtcNow,
        E_GROUPBY.days);
      t2 = DateTime.Now;
      //WriteListToConsole(avg);
      Console.WriteLine(
        $"influx agrg for some month, group by days:{(int)(t2 - t1).TotalMilliseconds}[ms]");
      Console.WriteLine("--------------------------------------------------");

      t1 = DateTime.Now;
      avg = await _service.Agregate(
        DateTime.UtcNow.AddMonths(-6),
        DateTime.UtcNow,
        E_GROUPBY.months);
      t2 = DateTime.Now;
      WriteListToConsole(avg);
      Console.WriteLine(
        $"influx agrg for a year, group by month:{(int)(t2 - t1).TotalMilliseconds}[ms]");
      Console.WriteLine("--------------------------------------------------");
    }

    static void WriteListToConsole(List<FluxTable> list)
    {
      return;
      foreach (var element in list)
      {
        foreach(var r in element.Records)
        {
          Console.WriteLine($"{r.Values["_time"].ToString()}->{r.Values["result"].ToString()} = {r.Values["_value"].ToString()} ");
        }        
      }
    }

  }
}
