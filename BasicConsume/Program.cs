using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using log4net;

namespace BasicConsume
{
    public class ReceiveMsgs
    {
        private static IModel channel;
        private static IConnection _connection;
        private static string _consumerTag = "";
        private static IBasicConsumer _consumer;
        static UTF8Encoding utfEncoding = new UTF8Encoding();
        public static bool IsConnected => _connection != null && _connection.IsOpen;
        private static CancellationTokenSource _dummyCancellationTokenSource;

        private static string con = $"amqp://guest:guest@localhost:5672";

        private static int counter = 0;
        private static int i = 0;
        private static ulong messageCount = 10000000000;
        private static bool shutdownRaised;
        public static ILog log = log4net.LogManager.GetLogger(typeof(ReceiveMsgs));
        public static void CreateConnection()
        {
            Task connectionTask = null;
            try
            {
                connectionTask = Task.Run(() =>
                {
                    var factory = new ConnectionFactory
                    {
                        Uri = new Uri(con),
                        AutomaticRecoveryEnabled = true,
                        Ssl =
                    {
                        Version = System.Security.Authentication.SslProtocols.Tls12,
                        Enabled = false,
                        AcceptablePolicyErrors =
                            System.Net.Security.SslPolicyErrors.RemoteCertificateChainErrors
                    }
                    };


                    _connection = factory.CreateConnection();
                    _connection.ConnectionShutdown += (sender, args) =>
                    {
                        log.Info($"Lost connection to broker...{args}");
                    };
                    _connection.ConnectionBlocked += (sender, args) =>
                    {
                        log.Info($"Connection Blocked to broker...{args.Reason}");
                    };
                    log.Info($"Created new connection with name = {_connection.ClientProvidedName}");
                });

                connectionTask.Wait(TimeSpan.FromSeconds(10));
            }
            catch (AggregateException ae)
            {
                var brokerUnreachableException = ae.InnerExceptions[0];
                if (brokerUnreachableException is BrokerUnreachableException)
                {
                    throw brokerUnreachableException;
                }
                else
                {
                    log.Info(ae.Message);
                    throw new BrokerUnreachableException(ae);
                }
            }
            catch (Exception e)
            {
                log.Info(e);
            }

            if (!connectionTask.IsCompleted)
            {
                //  throw new TimeoutException();
            }
        }

        private static async void StartListening()
        {
            await Task.Run(() =>
            {
                _consumerTag = channel.BasicConsume("queueName", false, _consumer);
                log.Info($"StartListening completed with {_consumerTag} for {"queueName"}...");
            });
        }
        public static void CreateChannel()
        {
            channel = _connection.CreateModel();
            channel.QueueDeclare("queueName", true, false, false, null);
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += Consumer_Received;
            consumer.Shutdown += Consumer_Shutdown;
            _consumer = consumer;
            StartListening();
        }
        public static void Consumer_Received(object sender, BasicDeliverEventArgs e)
        {
            if (channel.IsClosed)
            {
                shutdownRaised = true;
            }

            if (!shutdownRaised)
            {
                counter++;
                byte[] body = e.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                channel.BasicAck(deliveryTag: e.DeliveryTag, multiple: false);
                log.Info(" Acked "+ message);
            }
        }

        public static async Task UnsubscribeAsync()
        {
            await Task.Run(() =>
            {
                if (!string.IsNullOrEmpty(_consumerTag))
                {
                    try
                    {
                        channel.BasicCancel(_consumerTag);
                        log.Info($"Unsubscribed with {_consumerTag} for {"queueName"}...");
                    }
                    catch (AlreadyClosedException ace)
                    {
                        log.Info(ace.ToString());
                    }
                    catch (OperationInterruptedException oie)
                    {
                        log.Info(oie.ToString());
                    }
                }
                _consumerTag = null;
            });
        }
        public static Task DisconnectAsync()
        {
            if (!IsConnected)
            {
                return Task.CompletedTask;
            }
            var connName = _connection.ClientProvidedName;
            _connection.Close();
            log.Info($"Closed connection for {connName}");
            return Task.CompletedTask;
        }
        public static async void Consumer_Shutdown(object sender, ShutdownEventArgs e)
        {
            try
            {
                Cleanup();
                var mres = new ManualResetEventSlim(false); // state is initially false
                while (!mres.Wait(3000)) // loop until state is true, checking every 3s
                {
                    try
                    {
                        StopNotifyEventsTask(); // to stop publish
                        CreateConnection();  // re create
                        CreateChannel(); // re create 
                                         // messageCount = 10000000;
                        shutdownRaised = false;
                        PublishMsgs();
                        log.Info("Connected!");
                        mres.Set(); // state set to true - breaks out of loop
                    }
                    catch (Exception ex)
                    {
                        log.Info("Connect failed!");
                    }
                }
                //await Task.Run(async () =>
                //{
                //    StopNotifyEventsTask();
                //    await UnsubscribeAsync();
                //    await Task.Delay(10000);
                //    await DisconnectAsync();
                //    await Task.Delay(10000);
                //    CreateConnection();
                //    await Task.Delay(10000);
                //    CreateChannel();
                //    await Task.Delay(10000);
                //    PublishMsgs();
                //});
            }
            catch (TimeoutException ex)
            {
                log.Info(e.ToString());
                throw;
            }
            catch (Exception ex)
            {
                log.Info(e.ToString());
                throw;
            }
        }

        private static void StopNotifyEventsTask()
        {
            log.Info($"Stop Notify Events Task is invoked");
            _dummyCancellationTokenSource?.Cancel();
        }
        public static void PublishMsgs()
        {
            _dummyCancellationTokenSource = new CancellationTokenSource();
            //   var factory = new ConnectionFactory() { HostName = "localhost" };
            //  using (var connection = factory.CreateConnection())
            i = 0;
            var pubChannel = _connection.CreateModel();
            {
                pubChannel.QueueDeclare("queueName", true, false, false, null);
                Task.Run(() =>
                {
                    while (true)
                    {
                        if (!shutdownRaised)
                        {
                            i++;
                            var jsonObject1 = new JObject();
                            jsonObject1.Add("Type", 9);
                            jsonObject1.Add("Message", $"Testing Purpose {i}");
                            var message = new List<JObject>
                            {
                                jsonObject1
                            };
                            var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message));
                            log.Info($"Triggering event>> {body.ToString()}");
                            pubChannel.BasicPublish("", "queueName", null, body);
                            Thread.Sleep(500);
                        }
                    }
                }, _dummyCancellationTokenSource.Token);
            }

        }
        static void Cleanup()
        {
            try
            {
                if (channel != null && channel.IsOpen)
                {
                    channel.Close();
                    channel = null;
                }

                if (_connection != null && _connection.IsOpen)
                {
                    _connection.Close();
                    _connection = null;
                    channel = null;
                }
            }
            catch (IOException ex)
            {
                // Close() may throw an IOException if connection
                // dies - but that's ok (handled by reconnect)
            }
        }
        public static void Main()
        {
            log4net.Config.XmlConfigurator.Configure();
            CreateConnection();
            CreateChannel();
            PublishMsgs();
            log.Info(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }

}
