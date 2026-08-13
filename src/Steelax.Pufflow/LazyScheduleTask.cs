namespace Steelax.Pufflow;

internal sealed class LazyScheduleTask
{
    private Task? _task;
    
    private readonly Func<Task> _handler;
    private readonly TaskCompletionSource _tcs = new();
    private readonly Lock _sync = new();
    
    internal LazyScheduleTask(Func<Task> handler) => _handler = handler;

    public Task ExecuteTask => _tcs.Task;
        
    public void Run()
    {
        lock (_sync)
        {
            _task ??= _handler.Invoke().ContinueWith(static (task, tcs) => ((TaskCompletionSource)tcs!).SetFromTask(task), _tcs);
        }
    }
}