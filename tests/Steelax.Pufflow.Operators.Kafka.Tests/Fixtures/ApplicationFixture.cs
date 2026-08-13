using JetBrains.Annotations;
using Microsoft.Extensions.Logging;
using Steelax.Pufflow.Operators.Kafka.Tests.Fixtures;
using Testcontainers.Kafka;
using Xunit;

[assembly: AssemblyFixture(typeof(ApplicationFixture))]

namespace Steelax.Pufflow.Operators.Kafka.Tests.Fixtures;

[PublicAPI]
public sealed class ApplicationFixture : IAsyncLifetime
{
    private readonly ILoggerFactory _loggerFactory;
    
    public KafkaContainer KafkaContainer { get; }

    public ApplicationFixture()
    {
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .AddConsole()
                .AddDebug()
                .SetMinimumLevel(LogLevel.Warning); 
        });
        
        KafkaContainer = new KafkaBuilder("confluentinc/cp-kafka:7.4.0")
            .WithKRaft()
            .WithVendor(KafkaVendor.Confluent)
            .WithLogger(_loggerFactory.CreateLogger<KafkaContainer>())
            .Build();
    }

    public async ValueTask InitializeAsync()
    {
        await KafkaContainer.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await KafkaContainer.StopAsync();
            await KafkaContainer.DisposeAsync();
        }
        finally
        {
            _loggerFactory.Dispose();
        }
    }
}
