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
using log4net;

namespace BasicConsume
{
    public class ReceiveMsgs
    {
        private const string _queueName = "queueName";

        private static IConnectionFactory _connectionFactory;
        private static IModel _consumerChannel;
        private static IConnection _consumerConnection;
        private static string _consumerTag = "";
        private static IBasicConsumer _consumer;
        static UTF8Encoding utfEncoding = new UTF8Encoding();
        public static bool IsConnected => _consumerConnection != null && _consumerConnection.IsOpen;

        private static string con = "amqp://guest:guest@localhost:5672";

        private static int counter = 0;
        private static int i = 0;
        private static ulong messageCount = 10000000000;
        private static bool _shutdownRaised;
        private static bool _continuePublishing;
        public static ILog log = log4net.LogManager.GetLogger(typeof(ReceiveMsgs));

        public static void CreateConnectionFactory()
        {
            _connectionFactory = new ConnectionFactory
            {
                Uri = new Uri(con),
                AutomaticRecoveryEnabled = true,
            };
        }

        public static IConnection CreateConnection()
        {
            IConnection conn = null;

            try
            {
                conn  = _connectionFactory.CreateConnection();

                conn.ConnectionShutdown += (sender, args) =>
                {
                    log.Info($"Lost connection to broker...{args}");
                };

                conn.ConnectionBlocked += (sender, args) =>
                {
                    log.Info($"Connection Blocked to broker...{args.Reason}");
                };

                log.Info($"Created new connection with name = {conn.ClientProvidedName}");
            }
            catch (Exception e)
            {
                log.Error(e);
            }

            return conn;
        }

        public static void DeclareQueue(string queueName, IModel channel)
        {
            QueueDeclareOk result = channel.QueueDeclare(queueName, true, false, false, null);
        }

        public static void CreateChannelAndConsume()
        {
            _consumerChannel = _consumerConnection.CreateModel();
            DeclareQueue(_queueName, _consumerChannel);
            var consumer = new EventingBasicConsumer(_consumerChannel);
            consumer.Received += Consumer_Received;
            consumer.Shutdown += Consumer_Shutdown;
            _consumer = consumer;
            _consumerTag = _consumerChannel.BasicConsume("queueName", false, _consumer);
            log.Info($"StartListening completed with {_consumerTag} for {"queueName"}...");
        }

        public static void Consumer_Received(object sender, BasicDeliverEventArgs e)
        {
            if (_consumerChannel.IsClosed)
            {
                _shutdownRaised = true;
            }

            if (!_shutdownRaised)
            {
                counter++;
                byte[] body = e.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                _consumerChannel.BasicAck(deliveryTag: e.DeliveryTag, multiple: false);
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
                        _consumerChannel.BasicCancel(_consumerTag);
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
            var connName = _consumerConnection.ClientProvidedName;
            _consumerConnection.Close();
            log.Info($"Closed connection for {connName}");
            return Task.CompletedTask;
        }

        public static void Consumer_Shutdown(object sender, ShutdownEventArgs e)
        {
            try
            {
                Cleanup();
                var mres = new ManualResetEventSlim(false); // state is initially false
                while (!mres.Wait(3000)) // loop until state is true, checking every 3s
                {
                    try
                    {
                        StopPublishing();
                        _consumerConnection = CreateConnection();
                        CreateChannelAndConsume();
                        _shutdownRaised = false;
                        StartPublishing();
                        log.Info("Connected!");
                        mres.Set();
                    }
                    catch (Exception ex)
                    {
                        log.Error("Connect failed!", ex);
                    }
                }
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

        private static void StopPublishing()
        {
            log.Info($"StopPublishing is invoked");
            _continuePublishing = false;
        }

        public static void StartPublishing()
        {
            Task.Run(() =>
            {
                int publishCount = 0;
                using (IConnection pubConn = _connectionFactory.CreateConnection())
                {
                    using (IModel pubChannel = _consumerConnection.CreateModel())
                    {
                        DeclareQueue(_queueName, pubChannel);
                        while (true == _continuePublishing)
                        {
                            var jsonObject1 = new JObject();
                            jsonObject1.Add("Type", 9);
                            jsonObject1.Add("Message", $"Testing Purpose {i}");
                            var message = new List<JObject>
                            {
                                jsonObject1
                            };
                            var body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(message));

                            if (publishCount % 100 == 0)
                            {
                                log.Info($"publish count: {publishCount}");
                            }

                            pubChannel.BasicPublish("", "queueName", null, body);
                            publishCount++;
                            Thread.Sleep(500);
                        }
                    }
                }
            });
        }

        private static void Cleanup()
        {
            try
            {
                if (_consumerChannel != null && _consumerChannel.IsOpen)
                {
                    _consumerChannel.Close();
                    _consumerChannel = null;
                }

                if (_consumerConnection != null && _consumerConnection.IsOpen)
                {
                    _consumerConnection.Close();
                    _consumerConnection = null;
                    _consumerChannel = null;
                }
            }
            catch (IOException ex)
            {
                log.Error(ex);
            }
        }

        public static void Main()
        {
            log4net.Config.XmlConfigurator.Configure();
            CreateConnectionFactory();
            _consumerConnection = CreateConnection();
            CreateChannelAndConsume();
            PublishMsgs();
            log.Info(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }

}
