using Microsoft.EntityFrameworkCore;
using PCNWSolrUploadFiles.Controllers;
using PCNWSolrUploadFiles.Data;
using Serilog;
using Serilog.Events;
using Serilog.Filters;

namespace PCNWSolrUploadFiles
{
    public class Program
    {
        public static async Task Main(string[] args)
        {          

            try
            {
                Log.Information("Starting worker service...");

                var host = CreateHostBuilder(args).Build();

                await host.RunAsync();
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "The worker service failed to start.");
            }
            finally
            {
                Log.CloseAndFlush();
            }
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
            .UseWindowsService()
                .UseSerilog((context, services, configuration) => configuration
                .MinimumLevel.Information()
                .MinimumLevel.Override("Microsoft.EntityFrameworkCore", LogEventLevel.Warning) 
                .WriteTo.Console()
                .WriteTo.File("G:\\MyLogs\\SolrPDFUploaderLogs\\log.txt", rollingInterval: RollingInterval.Day)
                .Filter.ByExcluding(Matching.FromSource("Microsoft.EntityFrameworkCore"))
            ) 
                .ConfigureAppConfiguration((context, config) =>
                {
                    var env = context.HostingEnvironment;

                    config.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                          .AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true, reloadOnChange: true)
                          .AddEnvironmentVariables();
                })
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddDbContext<PcnwprojectDbContext>(options =>
                options.UseSqlServer(hostContext.Configuration.GetConnectionString("DefaultConnection")));

                    services.AddTransient<UploadController>();
                    services.AddHostedService<UploadWorkerService>();

                })
            ;
    }
}
