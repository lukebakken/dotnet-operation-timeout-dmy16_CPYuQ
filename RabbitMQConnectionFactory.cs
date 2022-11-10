using log4net;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rabbitmq_Ack_App
{
    public abstract class RabbitMQConnectionFactory
    {
        ILog log = log4net.LogManager.GetLogger(typeof(RabbitMQConnectionFactory));

        protected IModel Channel;
        protected IConnection connection;
        protected ConnectionFactory factory;
        public virtual void OnInit()
        {
            try
            {
                var brokerUri = ConfigurationManager.AppSettings["BrokerUri"];
                Console.WriteLine(brokerUri);
                factory = new ConnectionFactory
                {
                    Uri = new Uri(brokerUri),
                    Ssl =
                    {
                        Version = System.Security.Authentication.SslProtocols.Tls12,
                        Enabled = false,
                        AcceptablePolicyErrors =
                            System.Net.Security.SslPolicyErrors.RemoteCertificateChainErrors
                    }
                };
                connection = factory.CreateConnection();
                connection.ConnectionShutdown += Connection_ConnectionShutdown;
                factory.AutomaticRecoveryEnabled = true;
                Channel = connection.CreateModel();

                if (Channel == null)
                {
                    Console.WriteLine("Channel not created");
                }
                else
                {
                    Console.WriteLine("Connection established!");
                    Channel.BasicQos(0, 1, false);
                    Channel.ExchangeDeclare("test-exchange", ExchangeType.Direct, durable: true, autoDelete: false);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            Console.WriteLine("Perss enter to stop application");
            Console.ReadLine();
        }

        private void Connection_ConnectionShutdown(object sender, ShutdownEventArgs e)
        {
            try
            {
                log.Info("Connection shutdown occured");
                if (Channel != null)
                {
                    log.Info("Connection shutdown occured - Channel close and dispose initated");
                    Channel.Close();
                    Channel.Dispose();
                    log.Info("Connection shutdown occured - Channel close and dispose completed");

                }
                connection = factory.CreateConnection();
                factory.AutomaticRecoveryEnabled = true;
                connection.ConnectionShutdown += Connection_ConnectionShutdown;
                Channel = connection.CreateModel();
                log.Info("Connection & channel recreated");
            }
            catch (Exception ex)
            {
                log.Error("Exception on recreating Connection : " + ex.Message + " : " + ex.StackTrace);
            }
        }
    }
}
