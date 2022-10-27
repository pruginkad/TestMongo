using System;

namespace TSDBComparison
{
  public class MonitoringItem
  {
    public DateTime Timestamp { get; set; }
    public string ObjectName { get; set; }
    public string ObjectType { get; set; }
    public string PropName { get; set; }
    public double PropValue { get; set; }
  }
}
