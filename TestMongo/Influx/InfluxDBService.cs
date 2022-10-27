using System;
using System.Threading.Tasks;
using app.Models;
using DbLayer.Models;
using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Writes;
using Microsoft.Extensions.Configuration;
using static System.Net.WebRequestMethods;

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
    //"http://localhost:8086";
    public InfluxDBService(IConfiguration configuration)
    {
      _token = configuration.GetValue<string>("InfluxDB:Token");
      _client = InfluxDBClientFactory.Create(server_addr, _token);
    }

    public void Write(Action<WriteApi> action)
    {
      using var client = InfluxDBClientFactory.Create(server_addr, _token);
      using var write = client.GetWriteApi();
      action(write);
    }

    public async Task<T> QueryAsync<T>(Func<QueryApi, Task<T>> action)
    {
      using var client = InfluxDBClientFactory.Create(server_addr, _token);
      var query = client.GetQueryApi();
      return await action(query);
    }

    public async Task InsertManyAsync(List<AltitudeModel> list)
    {     
      var write = _client.GetWriteApiAsync();

      await write
        .WriteMeasurementsAsync(list, WritePrecision.Ns, "bucket", _org);
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