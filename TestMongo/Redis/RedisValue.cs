using NRedisTimeSeries.DataTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestDatabases.Redis
{
  internal record struct RedisValue(string key, TimeStamp timestamp, double value)
  {
    public static implicit operator (string key, TimeStamp timestamp, double value)(RedisValue value)
    {
      return (value.key, value.timestamp, value.value);
    }

    public static implicit operator RedisValue((string key, TimeStamp timestamp, double value) value)
    {
      return new RedisValue(value.key, value.timestamp, value.value);
    }
  }
}
