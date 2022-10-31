using System;

namespace TSDBComparison
{
  public class Record
  {
    public DateTime Timestamp { get; set; }
    public double AvgValue { get; set; }
    public double MinValue { get; set; }
    public double MaxValue { get; set; }
  }
}
