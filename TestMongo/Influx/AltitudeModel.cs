using InfluxDB.Client.Core;
using MongoDB.Driver;

namespace app.Models
{
  [Measurement("TestInflux")]
  public class AltitudeModel
  {    
    [Column("some_data", IsTag = true)] public long some_data { get; set; }
    [Column("new_data", IsTag = true)] public long new_data { get; set; } = 111;

    [Column("counter")] public long counter { get; set; }
    [Column("new_counter")] public long new_counter { get; set; }

    [Column(IsTimestamp = true)] public DateTime timestamp { get; set; }
  }
}