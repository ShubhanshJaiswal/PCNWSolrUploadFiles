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

                // Build the host for the worker service
                var host = CreateHostBuilder(args).Build();

                // Run the worker service
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
                .UseSerilog((context, services, configuration) => configuration
                .MinimumLevel.Information()
                .MinimumLevel.Override("Microsoft.EntityFrameworkCore", LogEventLevel.Warning) // Filter out EF Core logs
                .WriteTo.Console()
                .WriteTo.File("logs/log.txt", rollingInterval: RollingInterval.Day)
                .Filter.ByExcluding(Matching.FromSource("Microsoft.EntityFrameworkCore")) // Exclude EF Core logs
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

                    // Register other services
                    services.AddTransient<UploadController>();
                    services.AddHostedService<UploadWorkerService>();

                });
    }
}
