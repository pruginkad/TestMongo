using DbLayer.Models;
using DbLayer.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestMongo
{
  internal class MongoTest
  {
    public static async Task RunTest(CancellationToken token)
    {
      var service = new RoutService();
      var list = new List<DBRoutLine>();

      for (int i = 0; i < 10000; i++)
      {
        DBRoutLine line = new DBRoutLine();
        list.Add(line);
      }

      Task task = Task.Run(async () => {
        var max_counter = await service.GetMaxCount();

        bool recover = false;

        while (!token.IsCancellationRequested)
        {
          try
          {
            if (!recover)
            {
              foreach (DBRoutLine line in list)
              {
                line.meta.counter = (int)++max_counter;
                line.timestamp = DateTime.UtcNow;
              }
            }

            var t1 = DateTime.Now;

            await service.InsertManyAsync(list);

            var t2 = DateTime.Now;
            max_counter = await service.GetMaxCount();
            var t3 = DateTime.Now;

            Console
            .WriteLine(
              $"mongo:{max_counter / 1000000.0}->Insert:{(int)(t2 - t1).TotalMilliseconds}, GetMax:{(int)(t3 - t2).TotalMilliseconds}");

            recover = false;
          }
          catch (Exception ex)
          {
            recover = true;
            await Task.Delay(3000);
            Console.WriteLine(ex.Message.ToString());
            service = new RoutService();

            max_counter = await service.GetMaxCount();
          }
        }

      }, token);
    }
  }
}
