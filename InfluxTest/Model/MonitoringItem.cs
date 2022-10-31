using InfluxDB.Client.Core;

namespace TSDBComparison
{
  public class MonitoringItem
  {
    [Column(IsTimestamp = true)] public DateTime Timestamp { get; set; }
    [Column("ObjectName", IsTag = true)] public string ObjectName { get; set; }
    [Column("ObjectType", IsTag = true)] public string ObjectType { get; set; }
    [Column("PropName", IsMeasurement = true)] public string PropName { get; set; }
    [Column("PropValue")] public double PropValue { get; set; }
  }
}
