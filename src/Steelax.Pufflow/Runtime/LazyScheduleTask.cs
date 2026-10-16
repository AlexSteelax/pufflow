namespace Steelax.Pufflow.Runtime;

internal sealed class LazyScheduleTask(Func<Task> handler)
{
    private Task? _task;

    private readonly TaskCompletionSource _tcs = new();
    private readonly Lock _sync = new();

    public Task Completion => _tcs.Task;

    public void Run()
    {
        lock (_sync)
        {
            _task ??= Task.Run(handler).ContinueWith(static (task, tcs) => ((TaskCompletionSource)tcs!).SetFromTask(task), _tcs);
        }
    }
}