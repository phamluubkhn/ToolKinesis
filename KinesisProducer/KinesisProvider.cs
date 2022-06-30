using System;
using System.IO;
using System.Text;
using System.Threading;
using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;

namespace KinesisProducer
{
    public class KinesisProvider
    {

        public AmazonKinesisClient CreateAmazonKinesisClient(RegionEndpoint regionEndpoint)
        {
            return new AmazonKinesisClient(regionEndpoint);
        }

        private static string CreateStreamRequest(AmazonKinesisClient amazonKinesisClient, string myStreamName, int myStreamSize)
        {
            string result = "";
            try
            {
                var createStreamRequest = new CreateStreamRequest();
                createStreamRequest.StreamName = myStreamName;
                createStreamRequest.ShardCount = myStreamSize;
                var createStreamReq = createStreamRequest;
                var CreateStreamResponse = amazonKinesisClient.CreateStreamAsync(createStreamReq).Result;
                Console.Error.WriteLine("Created Stream : " + myStreamName);
            }
            catch (AggregateException ae)
            {
                ae.Handle((x) =>
                {
                    if (x is ResourceInUseException)
                    {
                        result = "Producer is not creating stream " + myStreamName +
                        " to put records into as a stream of the same name already exists.";
                        return true;
                    }
                    return false; // Let anything else stop the application.
                });
            }
            WaitForStreamToBecomeAvailable(amazonKinesisClient, myStreamName);
            return result;
        }

        private static string PutRecordRequest(AmazonKinesisClient kinesisClient, string streamName, MemoryStream streamData, string partitionKey, string dataName)
        {
            PutRecordRequest requestRecord = new PutRecordRequest();
            requestRecord.StreamName = streamName;
            requestRecord.Data = streamData;
            requestRecord.PartitionKey = partitionKey;
            var putResultResponse = kinesisClient.PutRecordAsync(requestRecord).Result;
            return $"Successfully putrecord {dataName}:PartitionKey ={partitionKey},  shard ID = {putResultResponse.ShardId}";
        }

        /// <summary>
        /// This method waits a maximum of 10 minutes for the specified stream to become active.
        /// <param name="myStreamName">Name of the stream whose active status is waited upon.</param>
        /// </summary>
        private static void WaitForStreamToBecomeAvailable(AmazonKinesisClient kinesisClient, string myStreamName)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(10);
            while (DateTime.UtcNow < deadline)
            {
                if (IsStreamActive(kinesisClient, myStreamName))
                {
                    return;
                }
                Thread.Sleep(TimeSpan.FromSeconds(20));
            }

            throw new Exception("Stream " + myStreamName + " never went active.");
        }

        public static bool IsStreamActive(AmazonKinesisClient kinesisClient, string myStreamName)
        {
            DescribeStreamRequest describeStreamReq = new DescribeStreamRequest();
            describeStreamReq.StreamName = myStreamName;
            var describeResult = kinesisClient.DescribeStreamAsync(describeStreamReq).Result;
            string streamStatus = describeResult.StreamDescription.StreamStatus;
            if (streamStatus == StreamStatus.ACTIVE)
            {
                return true;
            }
            return false;
        }

        public static ListStreamsResponse GetAllStreams(AmazonKinesisClient kinesisClient)
        {
            return kinesisClient.ListStreamsAsync().Result;
        }
    }
}
