using app.Models;
using InfluxDB.Client.Api.Domain;
using Microsoft.Extensions.Configuration;
using NRedisTimeSeries.DataTypes;
using NRedisTimeSeries;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NRedisTimeSeries.Commands.Enums;

namespace TestDatabases.Redis
{
  internal class RedisService
  {
    readonly ConnectionMultiplexer redis;
    readonly IDatabase db;
    public RedisService(IConfiguration configuration)
    {
      redis = ConnectionMultiplexer.Connect(
            new ConfigurationOptions
            {
              EndPoints = { "localhost:6379" }
            });

      db = redis.GetDatabase();

      //db.TimeSeriesCreateAsync("sensor1", 600000, new List<TimeSeriesLabel> { new TimeSeriesLabel("id", "sensor-1") });
    }
    public async Task InsertManyAsync(List<(string key, TimeStamp timestamp, double value)> list)
    {
      foreach (var item in list)
      {
        await db.TimeSeriesAddAsync("sensor1", item.timestamp, item.value);
      }      
    }
    public async Task<long> GetMaxCount()
    {
      try
      {
        var result = await db.TimeSeriesGetAsync("sensor1");
        return (long)result.Val;
      }
      catch(Exception ex)
      { }
      return 0;
    }
  }
}
