using System;

namespace TSDBComparison
{
  public class MonitoringItemMeta
  {
    public string ObjectName { get; set; }
    public string ObjectType { get; set; }
    public string PropName { get; set; }
    public double PropValue { get; set; }
  }
  public class MonitoringItem
  {
    public DateTime Timestamp { get; set; }
    public MonitoringItemMeta meta { get; set; }
  }
}
