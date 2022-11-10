using log4net;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace Rabbitmq_Ack_App
{
    public partial class Form1 : Form
    {
        RabbitPublisher rmqpublisher;
        RabbitReceiver rmqreceiver;
        RabbitReceiverTestQueue rmqtestreceiver;
        RabbitPublisherTestQueue rmqtestpublisher;
        int counter = 0;
        Thread threadMessagePublish;
        Thread threadMessageReceive;
        Thread threadMessageTestPublish;
        Thread threadMessageTestReceive;

        ILog log = log4net.LogManager.GetLogger(typeof(Form1));

        public Form1()
        {
            InitializeComponent();
            log4net.Config.XmlConfigurator.Configure();
            rmqpublisher = new RabbitPublisher();
            rmqpublisher.OnInit();
            rmqreceiver = new RabbitReceiver();
            rmqreceiver.OnInit();
            rmqtestreceiver = new RabbitReceiverTestQueue();
            rmqtestreceiver.OnInit();
            rmqtestpublisher = new RabbitPublisherTestQueue();
            rmqtestpublisher.OnInit();
            threadMessagePublish = new Thread(PublishMessage);
            threadMessageReceive = new Thread(ReceiveMessage);
            threadMessageTestPublish = new Thread(PublishTestMessage);
            threadMessageTestReceive = new Thread(ReceiveTestMessage);
        }

        private void button1_Click(object sender, EventArgs e)
        {
            threadMessagePublish.Start();
            threadMessageTestPublish.Start();
            button1.Enabled = false;
        }
        private void PublishMessage()
        {
            while (true)
            {
                string message = "Message Published:" + counter++;
                rmqpublisher.Publish(message);
                Thread.Sleep(1000);
            }
        }

        private void PublishTestMessage()
        {
            while (true)
            {
                string message = "Message Test Published:" + counter++;
                rmqtestpublisher.Publish(message);
                Thread.Sleep(1000);
            }
        }

        private void button2_Click(object sender, EventArgs e)
        {
            threadMessageReceive.Start();
            threadMessageTestReceive.Start();
            button2.Enabled = false;
        }

        private void ReceiveMessage()
        {
            MQMessage result;
            try
            {
                while (true)
                {
                    result = rmqreceiver.Receive();
                    if (result != null)
                    {
                        log.Info("Received from Queue with Ack:" + result.Content);

                        rmqreceiver.Ack(result.DeliveryTag);
                        log.Info("Received from Queue with Ack:" + result.Content + " Delivery Tag: " + result.DeliveryTag);

                    }
                }
            }
            catch(Exception ex)
            {
                log.Error("Receive Error : " + ex.Message + ex.StackTrace);
            }
        }

        private void ReceiveTestMessage()
        {
            MQMessage result;
            try
            {
                while (true)
                {
                    result = rmqtestreceiver.Receive();
                    if (result != null)
                    {
                        log.Info("Received from Queue with Ack:" + result.Content);

                        rmqtestreceiver.Ack(result.DeliveryTag);
                        log.Info("Received from Queue with Ack:" + result.Content + " Delivery Tag: " + result.DeliveryTag);
                    }
                }
            }
            catch (Exception ex)
            {
                log.Error("Receive Error : " + ex.Message + ex.StackTrace);
            }
        }
    }
}
