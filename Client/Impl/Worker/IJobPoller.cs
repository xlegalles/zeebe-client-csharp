#nullable enable
using System.Threading;
using Zeebe.Client.Api.Worker;

namespace Zeebe.Client.Impl.Worker;

internal interface IJobPoller
{
    void StartPolling(IJobClient jobClient, CancellationToken cancellationToken);
}