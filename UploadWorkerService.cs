using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using PCNWSolrUploadFiles.Controllers;
using Microsoft.Extensions.DependencyInjection;

public class UploadWorkerService : BackgroundService
{
    private readonly ILogger<UploadWorkerService> _logger;
    private readonly IServiceProvider _serviceProvider;
    private readonly TimeSpan _scheduleTime = new TimeSpan(2, 0, 0);
    //private readonly TimeSpan _scheduleTime = new TimeSpan(0, 5, 0);

    public UploadWorkerService(ILogger<UploadWorkerService> logger, IServiceProvider serviceProvider)
    {
        _logger = logger;
        _serviceProvider = serviceProvider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Worker started at: {time}", DateTimeOffset.Now);

        bool firstRun = true;

        while (!stoppingToken.IsCancellationRequested)
        {
            TimeSpan delay;

            if (firstRun)
            {
                // Run immediately on first execution
                delay = TimeSpan.Zero;
                firstRun = false;
            }
            else
            {
                var now = DateTime.Now;
                var nextMidnight = DateTime.Today.AddDays(1); 
                delay = nextMidnight - now;
            }

            _logger.LogInformation("Next run scheduled in: {delay}", delay);

            // Wait for the calculated delay
            await Task.Delay(delay, stoppingToken);

            try
            {
                _logger.LogInformation("Starting upload process at: {time}", DateTime.Now);

                using (var scope = _serviceProvider.CreateScope())
                {
                    var uploadController = scope.ServiceProvider.GetRequiredService<UploadController>();
                    await uploadController.UploadAllFiles(); // Your upload process
                }

                _logger.LogInformation("Upload process completed successfully at: {time}", DateTime.Now);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred during the upload process.");
            }
        }
    }

}
