using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Core;
using InfluxDB.Client.Core.Flux.Domain;
using NodaTime;
using System.Globalization;
using TSDBComparison;

namespace app.Services
{
  public class InfluxDBService
  {
    private readonly string _token;
    private readonly InfluxDBClient _client;
    private readonly string server_addr =
      "http://localhost:8086/";
    //"https://us-east-1-1.aws.cloud2.influxdata.com";
    private readonly string _org =
    //"1d5ad4e69b6b19b5";
    "organization";
    string _bucket = "bucket";
    //"http://localhost:8086";

    public enum E_GROUPBY
    {
      years,
      months,
      days,
      hours
    }

    public InfluxDBService()
    {
      _client = InfluxDBClientFactory.Create(InfluxDBClientOptions.Builder
                .CreateNew()
                .Authenticate("admin", "$Power321".ToCharArray())
                .Bucket(_bucket)
                .Org("organization")
                .LogLevel(LogLevel.None)
                .Url(server_addr)
                .Build());

     // _client = InfluxDBClientFactory.Create(server_addr, _token);
    }
    public async Task DeleteCollection()
    {
      Bucket b = await _client.GetBucketsApi().FindBucketByNameAsync(_bucket);
      if (b!= null)
      {
        await _client.GetBucketsApi().DeleteBucketAsync(b);
      }      
    }

    public async Task CreateCollection()
    {
      var org = await _client.GetOrganizationsApi().FindOrganizationsAsync(1, null, null, _org);

      await _client.GetBucketsApi().CreateBucketAsync(_bucket, org?.First());
    }

    public async Task<T> QueryAsync<T>(Func<QueryApi, Task<T>> action)
    {
      using var client = InfluxDBClientFactory.Create(server_addr, _token);
      var query = client.GetQueryApi();
      return await action(query);
    }


    public async Task InsertItems(List<MonitoringItem> list)
    {     
      var write = _client.GetWriteApiAsync();

      await write
        .WriteMeasurementsAsync(list, WritePrecision.Ns, _bucket, _org);
    }

    public async Task<List<FluxTable>> Agregate(
      DateTime startTime,
      DateTime endTime,
      E_GROUPBY groupBy,
      string ObjectName = "Jason",
      string ObjectType = "Sunshine",
      string PropName = "Coolness"
      )
    {
      var period = "1h";


      if (groupBy == E_GROUPBY.days)
      {
        period = "1d";
      }
      if (groupBy == E_GROUPBY.months)
      {
        period = "1mo";
      }
      if (groupBy == E_GROUPBY.years)
      {
        period="1y";
      }

      var query = _client.GetQueryApi();
      var flux = $"from(bucket: \"{_bucket}\")" +
  $"|> range(start: {startTime.ToString("o", CultureInfo.InvariantCulture)}, stop: {endTime.ToString("o", CultureInfo.InvariantCulture)})" +
  $"|> filter(fn: (r) => r[\"_measurement\"] == \"{PropName}\")" +
  $"|> filter(fn: (r) => r[\"ObjectName\"] == \"{ObjectName}\")" +
  $"|> filter(fn: (r) => r[\"ObjectType\"] == \"{ObjectType}\")";

      var flux_mean = flux+
  $"|> aggregateWindow(every: {period}, fn: mean, createEmpty: false)" +
   $"|> yield(name: \"mean\")" 
  ;

      var flux_min = flux +
        $"|> aggregateWindow(every: {period}, fn: min, createEmpty: false)" +
        $"|> yield(name: \"min\")";


      var flux_max = flux +
        $"|> aggregateWindow(every: {period}, fn: max, createEmpty: false)" +
         $"|> yield(name: \"max\")";
     //flux += $" |> mean()";

      //var flux = "from(bucket:\"bucket\") |> range(start: 0) |> max()";
     var tables_mean = await query.QueryAsync(flux_mean, _org);
      var tables_min = await query.QueryAsync(flux_min, _org);
      var tables_max = await query.QueryAsync(flux_max, _org);
      tables_mean.AddRange(tables_min);
      tables_mean.AddRange(tables_max);
      return tables_mean;
    }
      public async Task<long> GetMaxCount()
    {
      try
      {
        var query = _client.GetQueryApi();
        var flux = "from(bucket:\"bucket\") |> range(start: -1h) |> last()";
        //var flux = "from(bucket:\"bucket\") |> range(start: 0) |> max()";
        var tables = await query.QueryAsync(flux, _org);

        var table = tables.FirstOrDefault();

        if (table == null)
        {
          return 0;
        }

        var record = table.Records.LastOrDefault();

        var count = record.GetValue();

        if (count != null)
        {
          return (long)count;
        }

        return table.Records.Count();
      }
      catch(Exception ex)
      {
        return 0;
      }
    }
  }
}