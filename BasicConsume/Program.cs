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
        private const string amqp_uri = "amqp://guest:guest@localhost:5672";

        private static IConnectionFactory _connectionFactory;
        private static IModel _consumerChannel;
        private static IConnection _consumerConnection;
        private static string _consumerTag = "";
        private static IBasicConsumer _consumer;
        static UTF8Encoding utfEncoding = new UTF8Encoding();

        private static bool _shutdownRaised;
        private static bool _continuePublishing;
        public static ILog log = log4net.LogManager.GetLogger(typeof(ReceiveMsgs));

        public static void CreateConnectionFactory()
        {
            _connectionFactory = new ConnectionFactory
            {
                Uri = new Uri(amqp_uri),
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
                byte[] body = e.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                _consumerChannel.BasicAck(deliveryTag: e.DeliveryTag, multiple: false);
                log.Info(" Acked "+ message);
            }
        }

        public static void Consumer_Shutdown(object sender, ShutdownEventArgs e)
        {
            try
            {
                log.Warn("Consumer_Shutdown seen");
            }
            catch (Exception ex)
            {
                log.Error("Consumer_Shutdown", ex);
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
                            jsonObject1.Add("Message", $"Testing Purpose {publishCount}");
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

        public static void Main()
        {
            log4net.Config.XmlConfigurator.Configure();
            CreateConnectionFactory();
            _consumerConnection = CreateConnection();
            CreateChannelAndConsume();
            StartPublishing();
            log.Info(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }

}
