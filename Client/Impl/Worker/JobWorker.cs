//
//    Copyright (c) 2018 camunda services GmbH (info@camunda.com)
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
#nullable enable
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using GatewayProtocol;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Zeebe.Client.Api.Responses;
using Zeebe.Client.Api.Worker;
using Zeebe.Client.Impl.Commands;

namespace Zeebe.Client.Impl.Worker;

public sealed class JobWorker : IJobWorker
{
    private readonly CancellationTokenSource tokenSource;
    private readonly ILogger<JobWorker>? logger;
    private JobPoller? jobPoller;

    // private int currentJobsActive;
    private volatile bool isRunning;

    internal JobWorker(JobWorkerBuilder builder)
    {
        tokenSource = new CancellationTokenSource();
        tokenSource.Token.Register(() => DelayedCancellationTokenDisposal(builder.PollingInterval.TotalMilliseconds * 2));
        logger = builder.LoggerFactory?.CreateLogger<JobWorker>();

        BuildJobPoller(builder);
    }

    private void BuildJobPoller(JobWorkerBuilder builder)
    {
        var activateJobsRequest = new ActivateJobsRequest
        {
            Type = builder.JobWorkerType ?? throw new ArgumentNullException(nameof(builder.JobWorkerType)),
            Timeout = builder.JobTimeoutInMilliseconds,
            Worker = builder.WorkerName ?? throw new ArgumentNullException(nameof(builder.WorkerName)),
            MaxJobsToActivate = builder.MaxJobsToActivate,
            RequestTimeout = builder.RequestTimeoutInMilliseconds
        };
        activateJobsRequest.FetchVariable.AddRange(builder.JobFetchVariable);
        activateJobsRequest.TenantIds.AddRange(builder.CustomTenantIds);

        var pollerSettings = new JobPollerSettings(
            builder.ThreadCount,
            builder.MaxJobsToActivate,
            builder.MaxJobsToActivate * 0.6,
            builder.AutoCompletionEnabled,
            builder.PollingInterval);

        jobPoller = new JobPoller(
            activateJobsRequest,
            logger,
            builder.Activator,
            builder.JobHandler ?? throw new ArgumentNullException(nameof(builder.JobHandler)),
            pollerSettings);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        tokenSource.Cancel();
        isRunning = false;
    }

    private void DelayedCancellationTokenDisposal(double waitingTime)
    {
        // delay disposing, since poll and handler take some time to close
        Task.Delay(TimeSpan.FromMilliseconds(waitingTime))
            .ContinueWith(_ =>
            {
                logger?.LogError("Dispose source");
                tokenSource.Dispose();
            });
    }

    /// <inheritdoc/>
    public bool IsOpen()
    {
        return isRunning;
    }

    /// <inheritdoc/>
    public bool IsClosed()
    {
        return !isRunning;
    }

    /// <summary>
    /// Opens the configured JobWorker to activate jobs in the given poll interval
    /// and handle with the given handler.
    /// </summary>
    /// <param name="jobClient">The zeebe client to use</param>
    internal void Open(IJobClient jobClient)
    {
        isRunning = true;
        var cancellationToken = tokenSource.Token;
        jobPoller?.StartPolling(jobClient,cancellationToken);
    }
}