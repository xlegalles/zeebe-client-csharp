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

internal record JobPollerSettings(
    int ThreadCount,
    int MaxJobsActive,
    double ThresholdJobsActivation,
    bool AutoCompletion,
    TimeSpan PollInterval);

internal class JobPoller(
    ActivateJobsRequest activateJobsRequest,
    ILogger<JobWorker>? logger,
    JobActivator jobActivator,
    AsyncJobHandler jobHandler,
    JobPollerSettings settings) : IJobPoller
{
    private int currentJobsActive;

    public void StartPolling(IJobClient jobClient, CancellationToken cancellationToken)
    {
        var bufferOptions = CreateBufferOptions(cancellationToken);
        var executionOptions = CreateExecutionOptions(settings.ThreadCount, cancellationToken);

        var input = new BufferBlock<IJob>(bufferOptions);
        var transformer = new TransformBlock<IJob, IJob>(async activatedJob => await HandleActivatedJob(jobClient, activatedJob, cancellationToken),
            executionOptions);
        var output = new ActionBlock<IJob>(_ =>
            {
                Interlocked.Decrement(ref currentJobsActive);
            },
            executionOptions);

        input.LinkTo(transformer);
        transformer.LinkTo(output);

        // Start polling
        Task.Run(async () => await PollJobs(input, cancellationToken),
            cancellationToken).ContinueWith(
            t => logger?.LogError(t.Exception, "Job polling failed"),
            TaskContinuationOptions.OnlyOnFaulted);

        logger?.LogDebug(
            "Job worker ({Worker}) for job type {Type} has been opened",
            activateJobsRequest.Worker,
            activateJobsRequest.Type);
    }

    private ExecutionDataflowBlockOptions CreateExecutionOptions(int threadCount, CancellationToken cancellationToken)
    {
        return new ExecutionDataflowBlockOptions
        {
            MaxDegreeOfParallelism = threadCount,
            CancellationToken = cancellationToken,
            EnsureOrdered = false
        };
    }

    private static DataflowBlockOptions CreateBufferOptions(CancellationToken cancellationToken)
    {
        return new DataflowBlockOptions
        {
            CancellationToken = cancellationToken,
            EnsureOrdered = false
        };
    }

    private async Task PollJobs(ITargetBlock<IJob> input, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var currentJobs = Thread.VolatileRead(ref currentJobsActive);
            if (currentJobs < settings.ThresholdJobsActivation)
            {
                var jobCount = settings.MaxJobsActive - currentJobs;
                activateJobsRequest.MaxJobsToActivate = jobCount;

                try
                {
                    await jobActivator.SendActivateRequest(activateJobsRequest,
                        async jobsResponse => await HandleActivationResponse(input, jobsResponse, jobCount),
                        null,
                        cancellationToken);
                }
                catch (RpcException rpcException)
                {
                    LogRpcException(rpcException);
                    await Task.Delay(settings.PollInterval, cancellationToken);
                }
            }
            else
            {
                await Task.Delay(settings.PollInterval, cancellationToken);
            }
        }
    }

    private async Task HandleActivationResponse(ITargetBlock<IJob> input, IActivateJobsResponse response, int jobCount)
    {
        logger?.LogDebug(
            "Job worker ({Worker}) activated {ActivatedCount} of {RequestCount} successfully",
            activateJobsRequest.Worker,
            response.Jobs.Count,
            jobCount);

        foreach (var job in response.Jobs)
        {
            await input.SendAsync(job);
            Interlocked.Increment(ref currentJobsActive);
        }
    }

    private async Task<IJob> HandleActivatedJob(IJobClient jobClient, IJob activatedJob, CancellationToken cancellationToken)
    {
        var wrappedJobClient = JobClientWrapper.Wrap(jobClient);

        try
        {
            await jobHandler(wrappedJobClient, activatedJob);
            await TryToAutoCompleteJob(wrappedJobClient, activatedJob, cancellationToken);
        }
        catch (Exception exception)
        {
            await FailActivatedJob(wrappedJobClient, activatedJob, cancellationToken, exception);
        }
        finally
        {
            wrappedJobClient.Reset();
        }

        return activatedJob;
    }

    private void LogRpcException(RpcException rpcException)
    {
        LogLevel logLevel;
        switch (rpcException.StatusCode)
        {
            case StatusCode.DeadlineExceeded:
            case StatusCode.Cancelled:
            case StatusCode.ResourceExhausted:
                logLevel = LogLevel.Trace;
                break;
            default:
                logLevel = LogLevel.Error;
                break;
        }

        logger?.Log(logLevel, rpcException, "Unexpected RpcException on polling new jobs");
    }

    private async Task TryToAutoCompleteJob(JobClientWrapper wrappedJobClient, IJob activatedJob, CancellationToken cancellationToken)
    {
        if (!wrappedJobClient.ClientWasUsed && settings.AutoCompletion)
        {
            logger?.LogDebug(
                "Job worker ({Worker}) will auto complete job with key '{Key}'",
                activateJobsRequest.Worker,
                activatedJob.Key);
            await wrappedJobClient.NewCompleteJobCommand(activatedJob)
                .Send(cancellationToken);
        }
    }

    private Task FailActivatedJob(JobClientWrapper wrappedJobClient, IJob activatedJob, CancellationToken cancellationToken, Exception exception)
    {
        const string jobFailMessage =
            "Job worker '{0}' tried to handle job of type '{1}', but exception occured '{2}'";
        var errorMessage = string.Format(jobFailMessage, activatedJob.Worker, activatedJob.Type, exception.Message);
        logger?.LogError(exception, errorMessage);

        return wrappedJobClient.NewFailCommand(activatedJob.Key)
            .Retries(activatedJob.Retries - 1)
            .ErrorMessage(errorMessage)
            .Send(cancellationToken)
            .ContinueWith(
                task =>
                {
                    if (task.IsFaulted)
                    {
                        logger?.LogError(task.Exception, "Problem on failing job occured");
                    }
                }, cancellationToken);
    }
}