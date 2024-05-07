﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Best.HTTP;
using Best.HTTP.Response;
using Unity.VisualScripting;

namespace GRPC.NET
{
    public class GRPCBestHttpHandler : HttpClientHandler
    {
        private static readonly string ContentType = "Content-Type";

        private const int ASYNC_READ_WAIT_MS = 5;

        /**
         * This function is called by gRPC when establishing a new channel to a gRPC server.
         * We are mapping HttpRequestMessage and HttpResponseMessage to its BestHTTP equivalent.
         */
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage grpcRequest, CancellationToken cancellationToken)
        {
            if (grpcRequest.Method != HttpMethod.Post)
                throw new NotSupportedException("gRPC only supports POST method.");

            //
            // Create outgoing HTTP2 request
            //
            HTTPRequest bestRequest = new(grpcRequest.RequestUri, HTTPMethods.Post);

            // Disable internal retry
            bestRequest.RetrySettings.MaxRetries = 0;

            //
            // Prepare outgoing HEADER and HttpContent
            //

            // Copy over all request headers
            foreach (var kv in grpcRequest.Headers)
            {
                foreach (var headerItem in kv.Value) bestRequest.AddHeader(kv.Key, headerItem);
            }

            // Contained in grpcRequest.Content.Headers but we set it hardcoded here
            bestRequest.AddHeader(ContentType, "application/grpc");

            // Create outgoing data stream
            PushPullStream outgoingDataStream = new()
            {
                // BestHTTP does not perform blocking reads. Instead it will expect -1 to be returned if no data is yet
                // available. Each time the internal loop is triggered it will try to read from the stream again to check
                // if there is new data available.
                // This is why we have to trigger Http2Handler on each new DATA package when the stream was flushed.
                NonBlockingRead = true
            };
            bestRequest.UploadSettings.UploadStream = outgoingDataStream;

            // CopyToAsync can replace the underlying Stream of a HttpContent object as long as no write() call
            // was yet initiated/completed on it. This will allow us to provide our own Stream to gRPC on which
            // it then performs its writes on, allowing us to act on these calls.
            grpcRequest.Content.CopyToAsync(outgoingDataStream);

            bestRequest.UploadSettings.OnHeadersSent += _ =>
            {
                grpcRequest.Content.ReadAsStreamAsync().ContinueWith(_ =>
                {
                    outgoingDataStream.Close();
                }, cancellationToken);
            };

            //
            // Prepare HttpResponseMessage mapping incoming HEADER and DATA to forward to gRPC
            //

            TaskCompletionSource<HttpResponseMessage> grpcResponseTask = new();
            PushPullStream incomingDataStream = new();
            HttpResponseMessage grpcResponseMessage = new()
            {
                RequestMessage = grpcRequest,
                // HttpContent wrapper around incoming DATA package stream
                Content = new ServerStreamHttpContent(incomingDataStream)
            };

            // Write incoming headers OR trailing headers
            bool isHeader = true;
            bestRequest.DownloadSettings.OnHeadersReceived += (HTTPRequest _, HTTPResponse response, Dictionary<string, List<string>> headers) =>
            {
                // If we haven't received headers yet and grpc-status is included then its a trailers only response.
                bool trailersOnly = isHeader && headers.Keys.Contains("grpc-status");

                // https://github.com/grpc/grpc-dotnet/blob/master/src/Grpc.Net.Client/Internal/GrpcCall.cs#L311
                foreach (KeyValuePair<string, List<string>> kvp in headers)
                {
                    // Content.Headers is used for content-type and other well known headers, always populate them
                    grpcResponseMessage.Content.Headers.TryAddWithoutValidation(kvp.Key, kvp.Value);

                    // Trailer only responses have all the headers in both, metadata and trailers.
                    // If we add them to Headers, gRPC will take care of that for us. In any other case add them
                    // to the TrailingHeaders.
                    if (isHeader || trailersOnly)
                    {
                        // Add headers
                        grpcResponseMessage.Headers.TryAddWithoutValidation(kvp.Key, kvp.Value);
                    }
                    else
                    {
                        grpcResponseMessage.TrailingHeaders.TryAddWithoutValidation(kvp.Key, kvp.Value);
                    }
                }

                // Copy HTTP status fields
                if (isHeader)
                {
                    grpcResponseMessage.ReasonPhrase = response.Message;
                    grpcResponseMessage.StatusCode = (HttpStatusCode)response.StatusCode;
                    grpcResponseMessage.Version = new Version(response.HTTPVersion.Major, response.HTTPVersion.Minor);
                }

                // If we have a trailers only response, we'll get no data, so let's close the stream now.
                if (trailersOnly)
                {
                    incomingDataStream.Close();
                }

                // Complete Response on first HEADER package (before DATA arrived) to trigger gRPC
                if (!grpcResponseTask.Task.IsCompleted)
                    grpcResponseTask.SetResult(grpcResponseMessage);

                // From now on everything we get are trailers
                isHeader = false;
            };

            // For each incoming DATA package we write data through to gRPC
            bestRequest.DownloadSettings.OnDownloadStarted += (_, _, stream) =>
            {
                try
                {
                    // Copy what we have already available, we'll download the
                    // rest later before completing the response if needed.
                    while (stream.TryTake(out var buffer))
                    {
                        // Make sure that the buffer is released back to the BufferPool.
                        using var b = buffer.AsAutoRelease();
                        incomingDataStream.Write(b.Data, b.Offset, b.Count);
                    }

                    // That's all the data we have, so we're done streaming.
                    if (stream.IsCompleted)
                    {
                        incomingDataStream.Close();
                        return;
                    }

                    // Detatch the stream so we can consume the rest async in a Task.
                    stream.IsDetached = true;
                    Task.Run(async () =>
                    {
                        try
                        {
                            while (!stream.IsCompleted)
                            {
                                if (stream.TryTake(out var buffer))
                                {
                                // Make sure that the buffer is released back to the BufferPool.
                                    using var b = buffer.AsAutoRelease();
                                    incomingDataStream.Write(b.Data, b.Offset, b.Count);
                                }
                                else
                                {
                                // The DownloadContentStream does not block, so we wait a bit.
                                    await Task.Delay(ASYNC_READ_WAIT_MS);
                                }
                            }
                            incomingDataStream.Close();
                        }
                        catch (Exception e)
                        {
                            incomingDataStream.CloseWithException(e);
                        }
                    });
                }
                catch (Exception e)
                {
                    incomingDataStream.CloseWithException(e);
                }
            };

            // When gRPC call is canceled by the application we abort the request
            var cancellationTokenRegistration = cancellationToken.Register(() =>
            {
                bestRequest.Abort();
            });

            bestRequest.Callback += (request, response) =>
            {
                // We might have to handle an error when his callback is called after the request completed
                if (request.State != HTTPRequestStates.Finished)
                {
                    var ex = request.Exception ?? new Exception($"Unknown error while processing grpc req/resp (state={request.State}).");

                    // If the call was aborted instead we set the exception accordingly
                    if (request.State == HTTPRequestStates.Aborted)
                    {
                        ex = new Exception("gRPC call aborted by client.");
                    }

                    // If response IS NOT set we never got a HEADER response (arrives before any DATA)
                    if (!grpcResponseTask.Task.IsCompleted)
                    {
                        grpcResponseTask.SetException(ex);
                    }
                    // If response IS set we instead throw an exception in the blocking read() and write() thread
                    else
                    {
                        incomingDataStream.CloseWithException(ex);
                    }
                }

                // Unregister cancellation token once we are done
                cancellationTokenRegistration.Dispose();
            };

            // Finally send request to initiate transfer
            bestRequest.Send();

            return grpcResponseTask.Task;
        }
    }
}