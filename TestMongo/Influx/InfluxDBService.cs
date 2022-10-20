using System;
using System.Threading.Tasks;
using app.Models;
using DbLayer.Models;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using Microsoft.Extensions.Configuration;

namespace app.Services
{
  public class InfluxDBService
  {
    private readonly string _token;
    private readonly InfluxDBClient _client;

    public InfluxDBService(IConfiguration configuration)
    {
      _token = configuration.GetValue<string>("InfluxDB:Token");
      _client = InfluxDBClientFactory.Create("http://localhost:8086", _token);
    }

    public void Write(Action<WriteApi> action)
    {
      using var client = InfluxDBClientFactory.Create("http://localhost:8086", _token);
      using var write = client.GetWriteApi();
      action(write);
    }

    public async Task<T> QueryAsync<T>(Func<QueryApi, Task<T>> action)
    {
      using var client = InfluxDBClientFactory.Create("http://localhost:8086", _token);
      var query = client.GetQueryApi();
      return await action(query);
    }

    public async Task InsertManyAsync(List<AltitudeModel> list)
    {     
      var write = _client.GetWriteApiAsync();

      await write
        .WriteMeasurementsAsync(list, WritePrecision.Ns, "bucket", "organization");
    }
    public async Task<long> GetMaxCount()
    {
      try
      {
        var query = _client.GetQueryApi();
        var flux = "from(bucket:\"bucket\") |> range(start: 0) |> last()";
        //var flux = "from(bucket:\"bucket\") |> range(start: 0) |> max()";
        var tables = await query.QueryAsync(flux, "organization");

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