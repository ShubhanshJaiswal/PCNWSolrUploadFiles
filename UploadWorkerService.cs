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
        _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

        while (!stoppingToken.IsCancellationRequested)
        {
            var now = DateTime.Now;
            var nextRunTime = DateTime.Today.Add(_scheduleTime);
            //var nextRunTime = DateTime.Now.Add(_scheduleTime);

            if (now > nextRunTime)
                nextRunTime = nextRunTime.AddDays(1);

            int a = 0;
            var delay = (a > 1) ? nextRunTime - now : TimeSpan.Zero;
            //var delay = nextRunTime - now;
            _logger.LogInformation("Next run scheduled at: {time}", nextRunTime);

            await Task.Delay(delay, stoppingToken);

            try
            {
                _logger.LogInformation("Starting upload process at: {time}",DateTime.Now );

                using (var scope = _serviceProvider.CreateScope())
                {
                    var uploadController = scope.ServiceProvider.GetRequiredService<UploadController>();
                    await uploadController.UploadAllFiles();
                }

                _logger.LogInformation("Upload process completed successfully at: {time}",DateTime.Now);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred during the upload process.");
            }
        }
    }
}
