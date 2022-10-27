using app.Models;
using app.Services;
using Microsoft.Extensions.Configuration;
using NRedisTimeSeries.DataTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestDatabases.Redis
{
  internal class RedisTest
  {
    static public async Task RunTest(CancellationToken token)
    {
      IConfiguration config = new ConfigurationBuilder()
      .AddJsonFile("appsettings.json")
      .AddEnvironmentVariables()
      .Build();

      var service = new RedisService(config);
      var list = new List<(string key, TimeStamp timestamp, double value)>();

      for (int i = 0; i < 10000; i++)
      {
        var line = ("sensor2", DateTime.Now, (double)i);
        list.Add(line);
      }

      var list1 = list.ToArray();

      Task task = Task.Run(async () => {
        var max_counter = await service.GetMaxCount();

        bool recover = false;
        Random random = new Random();

        while (!token.IsCancellationRequested)
        {
          try
          {
            if (!recover)
            {
              for (int i = 0; i < list1.Length; i++)
              {
                list1[i].key = i.ToString();
                list1[i].value = (double)++max_counter;
                list1[i].timestamp = DateTime.UtcNow;
              }
            }

            var t1 = DateTime.Now;

            await service.InsertManyAsync(list1.ToList());

            var t2 = DateTime.Now;
            max_counter = await service.GetMaxCount();
            var t3 = DateTime.Now;

            ConsoleWrite.WriteConsole(
              $"redis:{max_counter / 1000000.0}->Insert:{(int)(t2 - t1).TotalMilliseconds}, GetMax:{(int)(t3 - t2).TotalMilliseconds}                   ",
              4);

            recover = false;
          }
          catch (Exception ex)
          {
            recover = true;
            await Task.Delay(3000);
            Console.WriteLine(ex.Message.ToString());
            service = new RedisService(config);

            max_counter = await service.GetMaxCount();
          }
        }

      }, token);
    }
  }
}
