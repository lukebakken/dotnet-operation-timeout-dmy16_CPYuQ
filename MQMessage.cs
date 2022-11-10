using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rabbitmq_Ack_App
{
    public class MQMessage
    {
        public string Content { get; set; }

        public ulong DeliveryTag { get; set; }
    }
}
