using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FREBProcessor;
using System.Threading;

namespace FREBProcessor
{
    class Program
    {
        static void Main(string[] args)
        {
            Task.Factory.StartNew(() => Sender());

            FRTReciever newReciever = new FRTReciever(@"c:\temp");
            newReciever.Start();
            newReciever.Wait();
        }

        private static void Sender()
        {
            FRTCollector frebCollector = new FRTCollector(FRTCollector.MonitoringSource.AzureBlobStorage, "freblogs");
            frebCollector.Start();
        }
    }
}
