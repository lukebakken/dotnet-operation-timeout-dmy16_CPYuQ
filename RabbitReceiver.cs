using log4net;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rabbitmq_Ack_App
{
    public class RabbitReceiver : RabbitMQConnectionFactory
    {
        ILog log = log4net.LogManager.GetLogger(typeof(RabbitReceiver));

        public override void OnInit()
        {
            base.OnInit();

            //Channel.BasicQos(0, 1, false);

            //QueueName = Channel.QueueDeclare(QueueName, true, false, false, null).QueueName;
            //Channel.QueueBind(QueueName, ExchangeName, RoutingKey);
            var QueueName = Channel.QueueDeclare("test-ack", true, false, false, null).QueueName;
            Channel.QueueBind(QueueName, "test-exchange", "test-ack-routing");
        }

        public MQMessage Receive()
        {
            if (Channel == null)
            {
                log.Info("channel == null");
                throw new InvalidOperationException("channel is null on receive");
            }

            try
            {
                BasicGetResult result = Channel.BasicGet("test-ack", false);
                if (result != null)
                {
                    return new MQMessage()
                    {
                        Content = Encoding.UTF8.GetString(result.Body.ToArray()),
                        DeliveryTag = result.DeliveryTag,
                    };
                }
            }
            catch (Exception ex)
            {
                log.Error($"failed to receive message from queue", ex);
                if (Channel != null)
                {
                    //log.Error($"Channel Closed Reason : " + Channel.CloseReason.ReplyText);
                    if ((connection.IsOpen) && (Channel.IsClosed) && (Channel.CloseReason.ReplyCode == 406))
                    {
                        log.Error($"Channel Closed Reason : " + Channel.CloseReason.ReplyText);
                        Channel.Dispose();
                        Channel = connection.CreateModel();
                        log.Error($"Channel created");
                    }
                    else if (connection != null)
                    {
                        if (!connection.IsOpen)
                        {
                            log.Error($"Connection Closed Reason : " + connection.CloseReason.ReplyText);
                            connection.Dispose();
                            log.Error($"Connection Disposed");
                        }

                    }
                    else
                    {
                        log.Error($"Channel not created");
                    }
                }
                log.Error($"Channel or Connection is null", ex);
            }

            return null;
        }

        public void Ack(ulong deliveryTag)
        {
            Channel.BasicAck(deliveryTag, false);
        }
    }
}
