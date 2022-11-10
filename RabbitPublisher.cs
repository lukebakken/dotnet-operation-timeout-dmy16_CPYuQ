using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using RabbitMQ.Client;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;
using System.Diagnostics;
using log4net;

namespace Rabbitmq_Ack_App
{
    public class RabbitPublisher:RabbitMQConnectionFactory
    {
        ILog log = log4net.LogManager.GetLogger(typeof(RabbitPublisher));

        public override void OnInit()
        {
            base.OnInit();

            Channel.BasicAcks += Channel_BasicAcks;
            Channel.ConfirmSelect();
        }
        private void Channel_BasicAcks(object sender, BasicAckEventArgs args)
        {
           log.Info($"{args.DeliveryTag} is delivered.");
        }

        public void Publish(string message)
        {
            if (string.IsNullOrEmpty(message))
                return;

            if (Channel == null)
            {
                Console.WriteLine("channel == null");
                log.Info("channel == null");
                throw new InvalidOperationException("channel is null on publish");
            }

            byte[] msgBytes = Encoding.UTF8.GetBytes(message);

            try
            {
                IBasicProperties messageProperties = Channel.CreateBasicProperties();
                messageProperties.Persistent = true;

                Channel.BasicPublish("test-exchange", "test-ack-routing", messageProperties, msgBytes);
                Channel.WaitForConfirms();
            }
            catch (Exception ex)
            {
                log.Error($"failed to publish message to ExchangeName ", ex);
            }
        }
    }
}
