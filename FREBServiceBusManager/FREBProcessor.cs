using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.Web.Administration;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Net;
using Microsoft.WindowsAzure.Storage.Analytics;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System.Diagnostics.Contracts;

namespace FREBProcessor
{
    ///<summary>
    ////
    //   Code for collecting the files from the local file system, routing events into and out of Azure then back to disk
    ///
    /// - Detects the FREB log direcory from various sources
    /// - Processes the file as an event then streams the data in the Azure Service bus
    /// - Monitors the Event hub for events and when there is one it writes it to the disk
    ///
    ///</summary>

    //  The delegate used across he solution for raising events
    public delegate void ProcessStreamHandler(object sender, FRTEventArg e);
    public delegate void ProcessEventHandler(object sender, EventHubDataArgs e);

    // Interface for the monitors to present a consistent interface to the client
    public interface ISourceMonitor : IDisposable
    {
        void Start();
        void Stop();
        void Wait();
        event ProcessStreamHandler ProcessEvents;
    }

    // Event args for the event handlers
    sealed public class EventHubDataArgs : EventArgs
    {
        public bool IsEmpty { get { return (HubEventData == null || HubEventData.Count() == 0); } }
        public IEnumerable<EventHubData> HubEventData { get; set; }

        public EventHubDataArgs(IEnumerable<EventHubData> events)
        {
            HubEventData = events;
        }
    }
    sealed public class FRTEventArg : EventArgs
    {
        public bool IsEmpty { get { return (Data == null || Data.Count() == 0); } }
        public Dictionary<string, string> Data { get; set; }

        public FRTEventArg(Dictionary<string, string> frebData)
        {
            Data = frebData;
        }
    }

    // The base class for handling event data 
    [Serializable]
    public class EventHubData
    {
        // This is per machine and can possibly have multiple callers
        private static long _rowKey = 0;

        // JSON can handle these data types
        // I found that these members have to be accessible when the event is deserialized
        // or they will not be populated
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }

        public string Owner { get; set; }
        public string Eventname { get; set; }
        public string Eventdata { get; set; }
        public string Eventtype { get; set; }
        public string Eventprovider { get; set; }
        public Guid EventproviderId { get; set; }
        public DateTime Eventtimestamp { get; set; }
        public DateTime Senttimestamp { get; set; }
        public Dictionary<string, object> Properties { get; set; }
        public EventHubData() { }
        public EventHubData(string ownername, string eventprovider, string eventtype, string eventname, string eventData, DateTime eventtime, Guid providerId, string partition, int maxlen, int minlen)
        {
            // assign the incoming data
            Owner = ownername;
            EventproviderId = providerId;
            PartitionKey = partition;
            Eventprovider = eventprovider;
            Eventtype = eventtype;
            Eventname = eventname;
            Eventtimestamp = eventtime;
            Eventdata = eventData;

            // set the event information
            Senttimestamp = DateTime.Now.ToUniversalTime();
            Interlocked.Exchange(ref _rowKey, Senttimestamp.Ticks);
            RowKey = _rowKey.ToString();
        }

        // Static methods for the serializtion of Event data 
        static public EventHubData DeserializeEventData(EventData eventData)
        {
            // Create the event data aand serialize the class to a Jason object
            return JsonConvert.DeserializeObject<EventHubData>(Encoding.UTF8.GetString(eventData.GetBytes()));
        }
        static public EventData SerializeEventData(string ownername, string eventprovider, string eventtype, string eventname, string eventData, DateTime eventtime, Guid providerId, string partition, int maxlen, int minlen)
        {
            EventHubData newData = new EventHubData(ownername, eventprovider, eventtype, eventname, eventData, eventtime, providerId, partition, maxlen, minlen);
            return SerializeEventData<EventHubData>(newData, partition, maxlen, minlen);
        }
        static public EventData SerializeEventData<T>(T value, string partition, int maxlen, int minlen)
        {
            // Create the event data aand serialize the class to a Jason object
            var serializedString = JsonConvert.SerializeObject(value);

            // Create an  EventData from the Jason object
            EventData data = new EventData(Encoding.UTF8.GetBytes(serializedString))
            {
                PartitionKey = partition
            };

            // If the serialized data is too small return
            if (data.SerializedSizeInBytes > minlen &&
                data.SerializedSizeInBytes <= maxlen)
            {
                return data;
            }

            //  256 Kb per message, use a batch mechanism for the larger message
            if (data.SerializedSizeInBytes > maxlen)
            {
                throw (new NotImplementedException("The event data is too large to be processed, consider batching the event data"));
            }
            return null;
        }
    }
    sealed public class FREBHubData : EventHubData
    {
        #region Private
        private static long _rowKey;
        private static string _partitionReference;
        private static string _providername;
        private static string _ownername;
        private static Guid _providerId;
        #endregion

        // This is the maximum and minimum sizes that we want to handle for a FREB event
        new public static string Owner { get { return _ownername; } }
        public const int MaximumSize = 25000;
        public const int MinimumSize = 20000;
        public const int BlockSize = 100000;

        // Constructors
        static FREBHubData()
        {
            _ownername = "FREBProcessing";
            _providerId = new Guid("ed544058-2bf2-42b4-b9a0-fd2782fe44ae");
            _providername = Path.GetFileName(System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName);
            _partitionReference = string.Format("{0}_{1}", Environment.MachineName, _providername);
        }
        public FREBHubData(string filename, string eventdata, string eventtype = "SystemEvent")
        {
            if (string.IsNullOrEmpty(filename) || string.IsNullOrEmpty(eventdata))
            {
                // Handle in your code
                return;
            }
            base.Eventdata = eventdata;
            Initialize(filename, eventtype);
        }
        private void Initialize(string filename, string eventtype = "SystemEvent")
        {
            // assign the provider info
            base.Eventprovider = _providername;
            base.EventproviderId = _providerId;

            // assign the incoming data
            base.Owner = _ownername;
            base.Eventtype = eventtype;
            base.Eventname = filename;
            base.Eventtimestamp = GetEventTime();

            // set the event information
            base.PartitionKey = string.Format("{0}_{1}", eventtype, _partitionReference);
            base.Senttimestamp = DateTime.Now.ToUniversalTime();
            Interlocked.Exchange(ref _rowKey, Senttimestamp.Ticks);
            base.RowKey = _rowKey.ToString();
        }
        private DateTime GetEventTime()
        {
            if (base.Eventdata != null && base.Eventdata.Length > 1000)
            {
                using (StringReader reader = new StringReader(base.Eventdata))
                { 
                    string line = string.Empty;
                    while (!string.IsNullOrEmpty((line = reader.ReadLine())))
                    {
                        if (line.Contains("<TimeCreated SystemTime="))
                        {
                            // You should improve on this
                            string[] evtTime = line.Split(new string[] { "<TimeCreated SystemTime=", "/>", " ", "  ","   ","    ","     ","      " }, StringSplitOptions.RemoveEmptyEntries);
                            if (evtTime != null && evtTime.Length > 0)
                            {
                                DateTime ret;
                                if (DateTime.TryParse(evtTime[0].Trim(new char [] { '\'', '\"'}), out ret))
                                {
                                    return ret;
                                }
                            }
                        }
                    }
                }
            }
            return DateTime.Now;
        }

        // Event transformation into event data for this object 
        public static EventData SerializeFREBHubData(string name, string stringdata, string eventtype, string partition)
        {
            // Create the event data aand serialize the object
            FREBHubData obj = new FREBHubData(name, stringdata, eventtype);
            return EventHubData.SerializeEventData(obj, partition, MaximumSize, MinimumSize);
        }
    }        
    
    // The Sender and Reciever code
    sealed public class FRTReciever : IDisposable
    {
        /// <summary>
        /// This class is designed to monitor the specidied event hub using the Azure Event Processor Host 
        /// </summary>
        
        #region Private
        private bool _disposed;
        private CancellationTokenSource _tokenSource;
        private ConcurrentDictionary<string, string> _eventQueue;
        private EventProcessorHost _eventProcessorHost;
        private AutoResetEvent _readReady;
        #endregion

        #region Public properties
        public string ArchivePath { get; internal set; }
        #endregion

        #region Constuctor
        public FRTReciever(string archpath)
        {
            if (!Directory.Exists(archpath))
            {
                // Handle error
                Trace.WriteLine("FRTReciever::FRTReciever construtor failed because the archive path does not exist");
                return;
            }

            ArchivePath = archpath;

            // Cancel token and completed bool
            _tokenSource = new CancellationTokenSource();

            // Allocate the event stream queue
            _eventQueue = new ConcurrentDictionary<string, string>();

            // Allocate event
            _readReady = new AutoResetEvent(false);

            ///////////////////////////////////////////////////////////////
            // Endpoint=sb://[namespace].servicebus.windows.net/;SharedAccessKeyName=Manage;SharedAccessKey=[key]
            ///////////////////////////////////////////////////////////////
            // NOTE: You will get an AV if these locals are not populated.  You will have to handle this in some way.
            // NOTE: Below is not the Microsoft naming convention for app settings
            ///////////////////////////////////////////////////////////////
            var connectionString = ConfigurationManager.AppSettings["Microsoft.ServiceBus.ConnectionString"];
            var eventHubConnectionString = ConfigurationManager.AppSettings["Microsoft.EventHub.Reciever.ConnectionString"];
            var groupname = ConfigurationManager.AppSettings["Microsoft.EventHub.Reciever.ConsumerGroupName"];
            var hubname = ConfigurationManager.AppSettings["Microsoft.EventHub.Reciever.Name"];

            // The NamespaceManager class is used to create Event Hubs
            var namespacemanager = NamespaceManager.CreateFromConnectionString(connectionString);
            var eventHubDescription = namespacemanager.CreateEventHubIfNotExists(hubname);
            var connectionStringBuilder = new ServiceBusConnectionStringBuilder(connectionString);
            connectionStringBuilder.EntityPath = eventHubDescription.Path;

            // Host processor
            string storageConnectionString = string.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}", connectionStringBuilder.SharedAccessKeyName, connectionStringBuilder.SharedAccessKey);
            StreamEventProcessor.ProcessEvents += StreamEventProcessor_ProcessEvents;

            // Allocate and set up the Event processing host
            _eventProcessorHost = new EventProcessorHost(Dns.GetHostName(), eventHubDescription.Path, groupname, eventHubConnectionString, storageConnectionString);
            _eventProcessorHost.PartitionManagerOptions = new PartitionManagerOptions()
            {
                MaxReceiveClients = 1,
                LeaseInterval = new TimeSpan(0, 1, 0),
                RenewInterval = new TimeSpan(0, 0, 20),
                AcquireInterval = new TimeSpan(0, 0, 20)
            };
        }
        #endregion

        #region Destructor
        ~FRTReciever()
        {
            this.Dispose(false);
        }
        #endregion
        
        #region Dispose pattern
        public void Dispose()
        {
            if (_disposed)
            { return; }

            this.Dispose(true);
            GC.SuppressFinalize(this);

            // This has been disposed
            this._disposed = true;
        }
        private void Dispose(bool disposing)
        {
            if (_disposed)
            { return; }

            if (this._eventProcessorHost != null)
            {
                this._eventProcessorHost.UnregisterEventProcessorAsync();
                this._eventProcessorHost = null;
            }

            if (this._tokenSource != null)
            {
                if (!this._tokenSource.IsCancellationRequested)
                {
                    this._tokenSource.Cancel();
                }
                this._tokenSource.Dispose();
                this._tokenSource = null;
            }

            if (this._eventQueue != null)
            {
                this._eventQueue.Clear();
                this._eventQueue = null;
            }

            if (_readReady != null)
            {
                this._readReady.Close();
                this._readReady.Dispose();
                this._readReady = null;
            }

            // This has been disposed
            _disposed = true;
        }
        #endregion 

        #region The StreamEventProcessor event handler and default options error handler
        void StreamEventProcessor_ProcessEvents(object sender, EventHubDataArgs e)
        {
            if (!e.IsEmpty && !_tokenSource.Token.IsCancellationRequested)
            {
                // Add the wait event to the event list and reset the waiter to wait
                foreach (EventHubData info in e.HubEventData)
                {
                    ////////////////////////////////////////////////////////////////////////
                    // If there is no name or no data then we have nothing to write to disk
                    ////////////////////////////////////////////////////////////////////////
                    // Hanhdle in your code!
                    if (!string.IsNullOrEmpty(info.Eventdata) && !string.IsNullOrEmpty(info.Eventname))
                    {
                        if (!_eventQueue.ContainsKey(info.Eventname))
                        {
                            _eventQueue.TryAdd(info.Eventname, info.Eventdata);
                        }
                    }
                }
                if (_eventQueue.Count > 0)
                {
                    _readReady.Set();
                }
            }
        }
        void DefaultOptions_ExceptionReceived(object sender, ExceptionReceivedEventArgs e)
        {
            Trace.WriteLine(string.Format("FRTReciever::DefaultOptions_ExceptionReceived Error: {0}", e.Exception.Message));
        }
        #endregion

        #region External methods for managing the runtime
        public void Wait()
        {
            _tokenSource.Token.WaitHandle.WaitOne();
        }
        public void Stop()
        {
            _readReady.Set();
            _tokenSource.Cancel();
        }
        public void Start()
        {
            // Start the reciever of the events
            Task.Factory.StartNew(() => EventPump(), _tokenSource.Token);

            // Start the writer of the events to disk
            Task.Factory.StartNew(() => WriteEvent(), _tokenSource.Token);
        }
        #endregion

        #region The event pump for recieving event data from the Event host and writing it to disk
        private async void EventPump()
        {
            var epo = new EventProcessorOptions
            {
                MaxBatchSize = 100,
                PrefetchCount = 100,
                ReceiveTimeOut = TimeSpan.FromSeconds(30),
                InitialOffsetProvider = (partitionId) => DateTime.UtcNow
            };
            epo.ExceptionReceived += DefaultOptions_ExceptionReceived;

            while (!_tokenSource.Token.IsCancellationRequested)
            {
                await _eventProcessorHost.RegisterEventProcessorAsync<StreamEventProcessor>(epo);
            }
        }
        #endregion

        #region The event writer for transforming event data to FREB files on the local disk
        private void WriteEvent()
        {
            while (!_tokenSource.Token.IsCancellationRequested)
            {
                _readReady.WaitOne();
                if (_eventQueue.Count == 0)
                {
                    return;
                }

                string result;
                List<string> fileList = _eventQueue.Keys.ToList();
                foreach (string fileName in fileList)
                {
                    try
                    {
                        // If the file exists or there is no data then this is removed from the queue
                        if (File.Exists(string.Format(@"{0}\{1}", ArchivePath, fileName)) || string.IsNullOrEmpty(_eventQueue[fileName]))
                        {
                            _eventQueue.TryRemove(fileName, out result);
                        }
                        else
                        {
                            File.WriteAllText(string.Format(@"{0}\{1}", ArchivePath, fileName), _eventQueue[fileName]);
                            _eventQueue.TryRemove(fileName, out result);
                        }
                    }
                    catch (Exception e)
                    {
                        // Handle error
                        Trace.WriteLine(string.Format("FRTReciever::WriteEvent Error: {0}", e.Message));
                    }                    
                }
            }
        }
        #endregion
    }
    sealed public class FRTCollector : IDisposable
    {
        #region Private constants
        private const string _FrebFilter = "fr*.xml";
        private const int _maxEventDataSize = 262000;
        #endregion

        #region Private properties
        private Boolean _disposed;
        private Boolean _initialized;
        private Boolean _running;
        private string _userName;
        private string _password;
        private string _sourceName;
        private MonitoringSource _sourceType;
        private CancellationTokenSource _TokenSource;
        private StringBuilder _stringBuilder;
        private FileMonitor _FileMonitor;
        private BlobStorageMonitor _AzureMonitor;
        private FTPStorageMonitor _FTPMonitor;
        private EventHubClient _hubClient;
        #endregion

        #region Public properties
        public string SourceName { get { return _sourceName; } }
        public MonitoringSource SourceType { get { return _sourceType; } }
        public enum MonitoringSource { None = 0, WebApp, FTP, FileSystem, AzureBlobStorage };
        #endregion

        #region Destructor
        ~FRTCollector()
        {
            this.Dispose(false);
        }
        #endregion

        #region Dispose pattern
        public void Dispose()
        {
            if (_disposed)
            { return; }

            this.Dispose(true);
            GC.SuppressFinalize(this);

            // This has been disposed
            this._disposed = true;
        }
        private void Dispose(bool disposing)
        {
            if (_disposed)
            { return; }

            if (this._TokenSource != null)
            {
                if (!this._TokenSource.IsCancellationRequested)
                {
                    this._TokenSource.Cancel();
                }
                this._TokenSource.Dispose();
                this._TokenSource = null;
            }

            if (this._FileMonitor != null)
            {
                this._FileMonitor.Dispose();
                this._FileMonitor = null;
            }

            if (this._FTPMonitor != null)
            {
                this._FTPMonitor.Dispose();
                this._FTPMonitor = null;
            }

            if (this._AzureMonitor != null)
            {
                this._AzureMonitor.Dispose();
                this._AzureMonitor = null;
            }
            // This has been disposed
            _disposed = true;
        }
        #endregion

        #region Constructor
        /// <summary>        
        /// The constructor code broken out into several calls to different Initialize method for supporting multiple constructors behavior
        /// <para>
        /// 1) File system client requires a file location 
        /// </para>
        /// <para>
        /// 2) FTP client requires, user name and password 
        /// </para>
        /// <para>
        /// 3) Azure storage client requires a container 
        /// </para>
        /// <para>
        /// 4) Web client will attempt to detect the FREB location or you can use a Source name for non default web applications
        /// </para>
        /// NOTE: If there is an entry in the app config or web config then it will not over write what is set in the constructor
        /// </summary>
        public FRTCollector(MonitoringSource sourceType, string sourcename = "", string username = "", string password = "")
        {
            // Allocate
            _TokenSource = new CancellationTokenSource();
            _stringBuilder = new StringBuilder(FREBHubData.MaximumSize);

            // Set properties and initialize we are not checking them here because they are conditional 
            _initialized = false;
            _sourceType = sourceType;
            _sourceName = sourcename;
            _userName = username;
            _password = password;

            // Initalize the Hub client
            bool sucess = false;
            if (InitializeHubClient())
            {
                // Set up the system for monitoring
                switch (sourceType)
                {
                    case MonitoringSource.WebApp:
                    case MonitoringSource.FileSystem:
                        sucess = InitializeFileMonitoring();
                        break;
                    case MonitoringSource.FTP:
                        sucess = InitializeFTPMonitoring();
                        break;
                    case MonitoringSource.AzureBlobStorage:
                        sucess = InitializeAzureMonitoring();
                        break;
                    default:
                        break;
                }

                // We could not construct the object properly
                if (!sucess)
                {
                    // This is fatal, it is better to faile early and throw an Exception than to construct an object that is not functional
                    // https://msdn.microsoft.com/en-us/library/aa269568(v=vs.60).aspx
                    throw(new Exception("Unable to continue, please check your paramters and check the tracing output for further troubleshooting"));
                }
            }
        }
        #endregion

        #region Initialization methods

        // Initalize the Hub client
        private bool InitializeHubClient()
        {
            if (_initialized)
            {
                Trace.WriteLine("FRTCollector::InitializeHubClient The hub client has already been initalized");
                return false;
            }

            // No matter what we ahave initialized
            _initialized = true;
            _running = false;

            string azurenamespace = string.Empty;
            string hubname = string.Empty;

            if (SourceType == MonitoringSource.WebApp)
            {
                System.Configuration.Configuration rootWebConfig = System.Web.Configuration.WebConfigurationManager.OpenWebConfiguration(null);
                if (rootWebConfig != null && rootWebConfig.AppSettings.Settings.Count > 0)
                {
                    foreach (string key in rootWebConfig.AppSettings.Settings.AllKeys)
                    {
                        ///////////////////////////////////////////////////////////////
                        // NOTE: Below is not the Microsoft naming convention for app settings
                        ///////////////////////////////////////////////////////////////
                        switch (key)
                        {
                            case "Microsoft.IIS.FRTLogfileLocation":
                                _sourceName = string.IsNullOrEmpty(rootWebConfig.AppSettings.Settings["Microsoft.IIS.FRTLogfileLocation"].Value) ? _sourceName : rootWebConfig.AppSettings.Settings["Microsoft.IIS.FRTLogfileLocation"].Value;
                                break;
                            case "Microsoft.ServiceBus.Namespace":
                                azurenamespace = rootWebConfig.AppSettings.Settings["Microsoft.EventHub.Sender.Namespace"].Value;
                                break;
                            case "Microsoft.EventHub.Sender.Name":
                                hubname = rootWebConfig.AppSettings.Settings["Microsoft.EventHub.Sender.Name"].Value;
                                break;
                            default:
                                break;
                        }
                    }
                }
                else
                { 
                    // Handle error
                    Trace.WriteLine("FRTCollector::InitializeHubClient failed to initalize");
                    return false;
                }
            }
            else
            {
                // If the source location is in the app config over wrtie 
                _sourceName = string.IsNullOrEmpty(ConfigurationManager.AppSettings["Microsoft.IIS.FRTLogfileLocation"]) ? _sourceName : ConfigurationManager.AppSettings["Microsoft.IIS.FRTLogfileLocation"];
                azurenamespace = ConfigurationManager.AppSettings["Microsoft.ServiceBus.ConnectionString"];
                hubname = ConfigurationManager.AppSettings["Microsoft.EventHub.Sender.Name"];
            }

            // The NamespaceManager class is used to create Event Hubs
            var namespacemanager = NamespaceManager.CreateFromConnectionString(azurenamespace);

            // Use the CreateEventHubIfNotExists methods to avoid exceptions
            var description = namespacemanager.CreateEventHubIfNotExists(hubname);

            // Create the EventHubClient client
            if (description.Status == EntityStatus.Active)
            {
                if ((_hubClient = EventHubClient.Create(hubname)) != null)
                {
                    return true;
                }
            }

            // Failed condition
            Trace.WriteLine("FRTCollector::InitializeHubClient The hub client was not initalized");
            return false;
        }

        // Initialize the monitors
        private bool InitializeFTPMonitoring()
        {
            ///////////////////////////////////////////////////////////////
            // Set up the FTP monitoring
            ///////////////////////////////////////////////////////////////

            // Only do this once and should never happen more than once
            if (_FTPMonitor != null)
            {
                Trace.WriteLine("FRTCollector::InitializeFTPMonitoring The FTP monitor is already configured");
                return false;
            }

            _FTPMonitor = new FTPStorageMonitor(SourceName, _userName, _password);
            _FTPMonitor.ProcessEvents += OnCreated;
            return true;
        }
        private bool InitializeFileMonitoring()
        {
            ///////////////////////////////////////////////////////////////
            // Set up the file monitoring
            ///////////////////////////////////////////////////////////////

            // Only do this once and should never happen more than once
            if (_FileMonitor != null)
            {
                Trace.WriteLine("FRTCollector::InitializeFileMonitoring The File watcher is already configured");
                return false;
            }

            // if this is not set then use the Server manager
            string dirFullPath = _sourceName;
            if (string.IsNullOrEmpty(dirFullPath) && _sourceType == MonitoringSource.WebApp)
            {
                using (ServerManager manager = new ServerManager())
                {
                    if (manager != null && !Directory.Exists(dirFullPath))
                    {
                        dirFullPath = Environment.ExpandEnvironmentVariables(manager.SiteDefaults.TraceFailedRequestsLogging.Directory);
                        if (!Directory.Exists(dirFullPath))
                        {
                            dirFullPath = Environment.ExpandEnvironmentVariables(manager.SiteDefaults.LogFile.Directory);
                            // This will be finally checked again below
                        }
                    }
                }
            }

            _FileMonitor = new FileMonitor(dirFullPath, _FrebFilter);
            _sourceName = dirFullPath;
            _FileMonitor.ProcessEvents += OnCreated;
            return true;
        }
        private bool InitializeAzureMonitoring()
        {
            ///////////////////////////////////////////////////////////////
            // Set up the Azure blob monitoring
            ///////////////////////////////////////////////////////////////
            
            // Only do this once and should never happen more than once
            if (_AzureMonitor != null)
            {
                Trace.WriteLine("FRTCollector::InitializeAzureMonitoring The Azure storage monitor is already configured");
                return false;
            }

            _AzureMonitor = new BlobStorageMonitor(SourceName);
            _AzureMonitor.ProcessEvents += OnCreated;
            return true;
        }
        #endregion

        #region The Event send handler
        private void OnCreated(object sender, FRTEventArg e)
        {
            if (!e.IsEmpty)            
            {
                foreach (string fileName in e.Data.Keys)
                {
                    if (!string.IsNullOrEmpty(fileName) &&
                        !string.IsNullOrEmpty(e.Data[fileName]))
                    {
                        // Get the data
                        EventData data = FREBHubData.SerializeFREBHubData(fileName, e.Data[fileName], FREBHubData.Owner, "0");
                        if (data != null)
                        {
                            SendAsync(data);
                        }
                    }
                }
            }
        }
        #endregion

        #region External methods for managing the object in a program
        public void Wait()
        {
            _TokenSource.Token.WaitHandle.WaitOne();
        }
        public void Stop()
        {
            // NOTE: We do not support a restart

            // Indicate to the clients processes that we are done
            _running = false;
            _TokenSource.Cancel();

            if (_FileMonitor != null)
            {
                _FileMonitor.Stop();
            }

            if (_FTPMonitor != null)
            {
                _FTPMonitor.Stop();
            }

            if (_AzureMonitor != null)
            {
                _AzureMonitor.Stop();
            }

            if (_hubClient != null && !_hubClient.IsClosed)
            {
                _hubClient.CloseAsync();
            }
        }
        public void Start()
        {
            // If these are null then something did not go right in the constructor
            if (_running || _TokenSource.IsCancellationRequested)
            {
                Trace.WriteLine("FRTCollector::Run is already runnning or is shutting down");
                return;
            }
            _running = true;

            if ((SourceType == MonitoringSource.FileSystem || SourceType == MonitoringSource.WebApp) &&
                _FileMonitor != null)
            {
                // Enable the eventing for monitoring the directory
                _FileMonitor.Start();
            }
            else if (SourceType == MonitoringSource.FTP && _FTPMonitor != null)
            {
                _FTPMonitor.Start();
            }
            else if (SourceType == MonitoringSource.AzureBlobStorage && _AzureMonitor != null)
            {
                _AzureMonitor.Start();
            }
        }
        #endregion

        #region Async task for managing event hub sends
        private void SendAsync(EventData data)
        {
            try
            {
                _hubClient.SendAsync(data);
            }            
            catch (TimeoutException too)
            {
                // We will allow other exception to go by
                Trace.WriteLine(string.Format("FRTCollector::SendAsync Error: Timeout, will try this message again. Message {0}:", too.Message));
            }
        }
        #endregion
    }

    // Source monitors
    public class FileMonitor : ISourceMonitor, IDisposable
    {
        #region Private properties
        private bool _disposed;
        private bool _running;
        private const int _pollingTime = 20000;
        private FileSystemWatcher _fileSystemWatcher;
        private CancellationTokenSource _TokenSource;
        private StringBuilder _stringBuilder;
        #endregion

        #region Public properties
        public string Filter { get; set; }
        public string DirectoryPath { get; set; }
        public int PollingTime { get { return _pollingTime; } }
        #endregion

        // Event handler
        public event ProcessStreamHandler ProcessEvents;
        protected virtual void RaiseEvent(FRTEventArg e)
        {
            if (ProcessEvents != null)
            {
                ProcessEvents(this, e);
            }
        }

        #region Destructor
        ~FileMonitor()
        {
            this.Dispose(false);
        }
        #endregion

        #region Dispose pattern
        public void Dispose()
        {
            if (_disposed)
            { return; }

            this.Dispose(true);
            GC.SuppressFinalize(this);

            // This has been disposed
            this._disposed = true;
        }
        private void Dispose(bool disposing)
        {
            if (_disposed)
            { return; }

            if (this._TokenSource != null)
            {
                if (!this._TokenSource.IsCancellationRequested)
                {
                    this._TokenSource.Cancel();
                }
                this._TokenSource.Dispose();
                this._TokenSource = null;
            }

            if (this._fileSystemWatcher != null)
            {
                _fileSystemWatcher.Dispose();
                _fileSystemWatcher = null;
            }

            // This has been disposed
            _disposed = true;
        }
        #endregion

        #region Constructors
        public FileMonitor()
        {
            Initialize();
        }
        public FileMonitor(string dirpath, string filter = "None")
        {
            Filter = filter;
            DirectoryPath = dirpath;
            Initialize();
        }
        #endregion

        #region Initalize method for supporting different constructor calls
        private void Initialize()
        {
            if (string.IsNullOrEmpty(DirectoryPath) || !Directory.Exists(DirectoryPath))
            {
                // This is fatal, it is better to faile early and throw an Exception than to construct an object that is not functional
                // https://msdn.microsoft.com/en-us/library/aa269568(v=vs.60).aspx
                Trace.WriteLine("FileMonitor::Initialize Failed because the directory value is invlid, check the location or the value for validity");
                throw (new ArgumentException("The directory location is not valid"));
            }

            // Set up monitoring and allocate the object
            _fileSystemWatcher = (string.IsNullOrEmpty(Filter) || Filter == "None") ? new FileSystemWatcher(DirectoryPath) : new FileSystemWatcher(DirectoryPath, Filter);
            _fileSystemWatcher.IncludeSubdirectories = true;

            // We want to monitor for changes
            _fileSystemWatcher.NotifyFilter = NotifyFilters.LastAccess | NotifyFilters.LastWrite
            | NotifyFilters.FileName | NotifyFilters.DirectoryName;

            // Add event handlers.
            _fileSystemWatcher.Created += EventProcessor;

            // Do not start monitoring at this time
            _fileSystemWatcher.EnableRaisingEvents = false;

            // Allocate
            _stringBuilder = new StringBuilder();
        }
        #endregion

        #region External controls
        public void Wait()
        {
            _TokenSource.Token.WaitHandle.WaitOne();
        }
        public void Stop()
        {
            _TokenSource.Cancel();
            _running = _fileSystemWatcher.EnableRaisingEvents = false;
        }
        public void Start()
        {
            if (_running)
            {
                Trace.WriteLine("FileMonitor::Start is already runnning");
                return;
            }
            _running = _fileSystemWatcher.EnableRaisingEvents = true;
        }
        #endregion

        #region Process the incoming data
        private void EventProcessor(object sender, FileSystemEventArgs e)
        {
            // NOTE: Since we registered for multiple notifications logic should be added here to see if we processed this file already
            // Check to see if this file exists
            if (!File.Exists(e.FullPath))
            {
                // Handle this in your code
                Trace.WriteLine("FileMonitor::EventProcessor -> File does not exist");
                return;
            }

            using (StreamReader fs = GetFileStream(e.FullPath))
            {
                // Get the data
                _stringBuilder.Clear();
                _stringBuilder.Append(fs.ReadToEnd());

                Dictionary<string, string> newData = new Dictionary<string, string>();
                newData.Add(Path.GetFileName(e.FullPath), _stringBuilder.ToString());

                RaiseEvent(new FRTEventArg(newData));            
            }
        }
        #endregion

        #region Utilities
        private StreamReader GetFileStream(string fileName)
        {
            int iMaxAttempts = 20; // totally random number
            int iAttempts = 0;

            StreamReader retStream = null;
            while (retStream == null && iAttempts < iMaxAttempts)
            {
                try
                {
                    retStream = new StreamReader(fileName);
                    iAttempts++;
                }
                catch { }
                Thread.Sleep(100);
            }

            // Try one last time or Throw() the exception
            if (retStream == null)
            {
                retStream = new StreamReader(fileName);
            }
            return retStream;
        }
        #endregion
    }
    public class BlobStorageMonitor : ISourceMonitor, IDisposable
    {
        #region Private constants
        private const int _pollingTime = 20000;
        #endregion

        #region Private properties
        private bool _disposed;
        private bool _running;
        private string _connection;
        private CloudBlobContainer _blobContainer;
        private CloudBlobClient _blobClient;
        private List<string> _FRTList;
        private CancellationTokenSource _TokenSource;
        #endregion

        #region Public properties
        public string ContainerName;
        public int PollingTime { get { return _pollingTime; } }
        #endregion

        // Event handler
        public event ProcessStreamHandler ProcessEvents;
        protected virtual void RaiseEvent(FRTEventArg e)
        {
            if (ProcessEvents != null)
            {
                ProcessEvents(this, e);
            }
        }

        #region Destructor
        ~BlobStorageMonitor()
        {
            this.Dispose(false);
        }
        #endregion

        #region Dispose pattern
        public void Dispose()
        {
            if (_disposed)
            { return; }

            this.Dispose(true);
            GC.SuppressFinalize(this);

            // This has been disposed
            this._disposed = true;
        }
        private void Dispose(bool disposing)
        {
            if (_disposed)
            { return; }

            if (this._TokenSource != null)
            {
                if (!this._TokenSource.IsCancellationRequested)
                {
                    this._TokenSource.Cancel();
                }
                this._TokenSource.Dispose();
                this._TokenSource = null;
            }

            if (_blobContainer != null)
            {
                _blobContainer = null;
            }

            // This has been disposed
            _disposed = true;
        }
        #endregion

        #region Constructors
        public BlobStorageMonitor()
        {
            Initialize();
        }
        public BlobStorageMonitor(string containerRef, string connection = "")
        {
            ContainerName = containerRef;
            _connection = connection;
            Initialize();
        }
        #endregion

        #region Initalize method for supporting different constructor calls
        private void Initialize()
        {
            if (string.IsNullOrEmpty(_connection))
            {
                // DefaultEndpointsProtocol=https;AccountName=STORAGE_ACCOUNT_NAME;AccountKey=PRIMARY_ACCESS_KEY
                _connection = ConfigurationManager.AppSettings["AzureWebJobsDashboard"];
            }
            if (string.IsNullOrEmpty(ContainerName))
            {
                ContainerName = ConfigurationManager.AppSettings["Microsoft.Storage.Blob.ContainerName"];
            }

            if (string.IsNullOrEmpty(_connection))
            {
                // This is fatal, it is better to faile early and throw an Exception than to construct an object that is not functional
                // https://msdn.microsoft.com/en-us/library/aa269568(v=vs.60).aspx
                Trace.WriteLine("StorageMonitor::Initialize Failed because the AzureWebJobsDashboard connection string is invalid from the app config");
                throw (new ArgumentException("The AzureWebJobsDashboard name was not defined, try adding it ot the app config"));
            }

            if (string.IsNullOrEmpty(ContainerName))
            {

                if (string.IsNullOrEmpty(ContainerName))
                {
                    // This is fatal, it is better to faile early and throw an Exception than to construct an object that is not functional
                    // https://msdn.microsoft.com/en-us/library/aa269568(v=vs.60).aspx
                    Trace.WriteLine("StorageMonitor::Initialize The Blob storage container name was not defined, try adding it ot the app config");
                    throw (new ArgumentException("The Blob storage container name was not defined, try adding it ot the app config"));
                }
            }

            // lower case the container name
            ContainerName = ContainerName.ToLower();

            // Create the storage account, container and client objects
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(_connection);
            _blobClient = storageAccount.CreateCloudBlobClient();

            // Set up a retry time
            _blobClient.DefaultRequestOptions.RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(3), 3);

            // Get a blob container for later use
            _blobContainer = _blobClient.GetContainerReference(ContainerName);

            // We ill create the blob if it is not there
            _blobContainer.CreateIfNotExists();

            // Allocate
            _TokenSource = new CancellationTokenSource();
            _FRTList = new List<string>();
        }
        #endregion

        #region External controls
        public void Wait()
        {
            _TokenSource.Token.WaitHandle.WaitOne();
        }
        public void Stop()
        {
            _TokenSource.Cancel();
        }
        public void Start()
        {
            if (_running)
            {
                Trace.WriteLine("StorageMonitor::Start is already runnning");
                return;
            }
            Task.Factory.StartNew(() => EventProcessor(), _TokenSource.Token);
        }
        #endregion

        #region Working methods
        private string DownloadBlob(string blockReference)
        {
            CloudBlob blob = _blobContainer.GetBlobReference(blockReference);
            if (BlobExists(blob))
            {
                MemoryStream blobStream = new MemoryStream();
                blob.DownloadToStream(blobStream);

                StringBuilder sb = new StringBuilder();
                if (blobStream != null && blobStream.CanRead)
                {
                    // I have found that this is not always ready to read
                    if (blobStream.Position > 0)
                    {
                        blobStream.Seek(0, SeekOrigin.Begin);
                    }
                    using (StreamReader sr = new StreamReader(blobStream))
                    {
                        sb.Append(sr.ReadToEnd());
                    }
                }

                // Return results
                return sb.ToString();
            }
            return string.Empty;
        }
        private List<string> UpdateEventsList()
        {
            // NOTE: This implements polling, the Azure SDK has an API for this now, I did not put that implementation into this sample 
            List<string> retList = new List<string>();
            try
            {
                // This has an can fail, it will return a 404, I think it is due to no blocks avalible
                IEnumerable<IListBlobItem> newList = _blobContainer.ListBlobs();
                if (newList != null && newList.Count() > 0)
                {
                    foreach (IListBlobItem li in newList)
                    {
                        string storageRef = li.StorageUri.PrimaryUri.AbsoluteUri.ToString()
                            .Replace(li.Parent.Uri.AbsoluteUri.ToString(), "")
                            .Trim('/');
                        if (!_FRTList.Contains(storageRef))
                        {
                            _FRTList.Add(storageRef);
                            retList.Add(storageRef);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Trace.WriteLine(string.Format("StorageMonitor::UpdateEventsList Error: {0}", e.Message));
            }
            return retList;
        }
        private void EventProcessor()
        {
            _running = true;
            StringBuilder sb = new StringBuilder();
            while (!_TokenSource.Token.WaitHandle.WaitOne(_pollingTime))
            {
                List<string> blockRef = UpdateEventsList();
                if (blockRef.Count > 0)
                {
                    Dictionary<string, string> eventListing = new Dictionary<string, string>();
                    foreach (string bRef in blockRef)
                    {
                        if (bRef.Contains(".xml"))
                        {
                            sb.Clear();
                            sb.Append(DownloadBlob(bRef));
                            if (sb.Length > 0)
                            {
                                eventListing.Add(bRef, sb.ToString());
                                sb.Clear();
                            }
                        }
                    }
                    if (eventListing.Count > 0)
                    {
                        RaiseEvent(new FRTEventArg(eventListing));
                    }
                }
            }
            _running = false;
        }
        public void UploadData(string name, string path)
        {
            // This was for testing, I left it in the code base for future testing
            try
            {
                using (var fileStream = System.IO.File.OpenRead(path))
                {
                    CloudBlockBlob blockBlob = _blobContainer.GetBlockBlobReference(name);
                    blockBlob.UploadFromStream(fileStream);
                }
            }
            catch (Exception ee)
            {
                Trace.WriteLine(string.Format("StorageMonitor::UploadFileToBlob Error: {0}", ee.Message));
            }
        }
        #endregion

        #region Utilities
        private FileStream GetFileStream(string fileName)
        {
            int iMaxAttempts = 20;
            int iAttempts = 0;

            FileStream retStream = null;
            while (retStream == null && iAttempts < iMaxAttempts)
            {
                try
                {
                    retStream = File.OpenRead(fileName);
                    iAttempts++;
                }
                catch { }
                Thread.Sleep(100);
            }

            // Try one last time or Throw() the exception
            if (retStream == null)
            {
                retStream = File.OpenRead(fileName);
            }
            return retStream;
        }
        private bool BlobExists(CloudBlob blob)
        {
            if (blob != null)
            {
                try
                {
                    blob.FetchAttributes();
                    return true;
                }
                catch
                { }
            }
            return false;
        }
        #endregion
    }
    public class FTPStorageMonitor : ISourceMonitor, IDisposable
    {
        #region Private constant properties
        private const int _pollingTime = 20000;
        #endregion

        #region Private properties
        private string _FTPDirectory;
        private string _FTPPassword;
        private string _FTPUser;
        private bool _disposed;
        private bool _running;
        private Uri _FTPSserverUri;
        private Dictionary<int, string> _FRTList;
        private CancellationTokenSource _TokenSource;
        private FtpWebRequest _PollingRequest;
        private WebClient _DownloadRequest;
        private StringBuilder _sbListUpdate;
        #endregion

        #region Public properties
        public int PollingTime { get { return _pollingTime; } }
        #endregion

        // Event handler
        public event ProcessStreamHandler ProcessEvents;
        protected virtual void RaiseEvent(FRTEventArg e)
        {
            if (ProcessEvents != null)
            {
                ProcessEvents(this, e);
            }
        }

        #region Destructor
        ~FTPStorageMonitor()
        {
            this.Dispose(false);
        }
        #endregion

        #region Dispose pattern
        public void Dispose()
        {
            if (_disposed)
            { return; }

            this.Dispose(true);
            GC.SuppressFinalize(this);

            // This has been disposed
            this._disposed = true;
        }
        private void Dispose(bool disposing)
        {
            if (_disposed)
            { return; }

            if (this._TokenSource != null)
            {
                if (!this._TokenSource.IsCancellationRequested)
                {
                    this._TokenSource.Cancel();
                }
                this._TokenSource.Dispose();
                this._TokenSource = null;
            }

            if (_DownloadRequest != null)
            {
                _DownloadRequest.Dispose();
                _DownloadRequest = null;
            }

            if (_PollingRequest != null)
            {
                _PollingRequest.Abort();
                _PollingRequest = null;
            }

            // This has been disposed
            _disposed = true;
        }
        #endregion

        #region Constructor
        public FTPStorageMonitor(string ftpDir = "", string username = "", string password = "")
        {
            if (string.IsNullOrEmpty(password))
            {
                password = ConfigurationManager.AppSettings["FTPStorageMonitor.FTP.Password"];
            }
            if (string.IsNullOrEmpty(username))
            {
                username = ConfigurationManager.AppSettings["FTPStorageMonitor.FTP.Username"];
            }
            if (string.IsNullOrEmpty(ftpDir))
            {
                ftpDir = ConfigurationManager.AppSettings["FTPStorageMonitor.FTP.Directory"];
            }

            // Validate user input
            if (string.IsNullOrEmpty(ftpDir) ||
               string.IsNullOrEmpty(username) ||
                string.IsNullOrEmpty(password))
            {
                // This is fatal, it is better to faile early and throw an Exception than to construct an object that is not functional
                // https://msdn.microsoft.com/en-us/library/aa269568(v=vs.60).aspx
                Trace.WriteLine("FTPStorageMonitor::FTPStorageMonitor Invalid parameters");
                throw (new ArgumentException("One or more of the input paramters are invalid"));
            }

            _FTPSserverUri = new Uri(ftpDir);
            if (_FTPSserverUri.Scheme != Uri.UriSchemeFtp)
            {
                // This is fatal, it is better to faile early and throw an Exception than to construct an object that is not functional
                // https://msdn.microsoft.com/en-us/library/aa269568(v=vs.60).aspx
                Trace.WriteLine("FTPStorageMonitor::FTPStorageMonitor Invalid FTP Uri parameter");
                throw (new ArgumentException("The FTP Uri paramters is not valid"));
            }

            try
            {
                _PollingRequest = (FtpWebRequest)WebRequest.Create(_FTPDirectory);         
                _DownloadRequest = new WebClient(); 
            }
            catch (Exception e)
            {
                // This is fatal, it is better to faile early and throw an Exception than to construct an object that is not functional
                // https://msdn.microsoft.com/en-us/library/aa269568(v=vs.60).aspx
                Trace.WriteLine(string.Format("FTPStorageMonitor::FTPStorageMonitor Unable to create WebRequest or WebClient(s), error: {0}", e.Message));
                throw;            
            }

            // Assign
            _FTPPassword = password;
            _FTPUser = username;
            _FTPDirectory = ftpDir;

            // Allocate
            _sbListUpdate = new StringBuilder();
            _FRTList = new Dictionary<int, string>();
            _TokenSource = new CancellationTokenSource();
        }
        #endregion

        #region External controls
        public void Wait()
        {
            _TokenSource.Token.WaitHandle.WaitOne();
        }
        public void Stop()
        {
            _TokenSource.Cancel();
        }
        public void Start()
        {
            if (_running)
            {
                Trace.WriteLine("FTPMonitor::Start is already runnning");
                return;
            }
            Task.Factory.StartNew(() => EventProcessor(), _TokenSource.Token);
        }
        #endregion

        #region Working memebrs
        private void EventProcessor()
        {
            // Process events
            _running = true;
            StringBuilder sb = new StringBuilder();
            while (!_TokenSource.Token.WaitHandle.WaitOne(_pollingTime))
            {
                List<string> files = UpdateEventsList();
                if (files.Count > 0)
                {
                    Dictionary<string, string> eventListing = new Dictionary<string, string>();
                    foreach (string filename in files)
                    {
                        sb.Clear();
                        sb.Append(Download(filename));
                        if (sb.Length > 0)
                        {
                            eventListing.Add(filename, sb.ToString());
                            sb.Clear();
                        }
                    }
                    RaiseEvent(new FRTEventArg(eventListing));
                }
            }
            sb.Clear();
            _running = false;
        }
        private string Download(string filename)
        {
            try
            {
                byte[] newFileData = _DownloadRequest.DownloadData(string.Format("{0}/{1}", _FTPSserverUri.ToString(), filename));
                return System.Text.Encoding.UTF8.GetString(newFileData);
            }
            catch (WebException e)
            {
                // Handle error
                Trace.WriteLine("FTPManager::Download Error: {0}", e.Message);
            }
            return string.Empty;
        }
        private List<string> UpdateEventsList()
        {
            List<string> retList = new List<string>();

            // Get the object used to communicate with the server.            
            if (_PollingRequest != null)
            {
                _PollingRequest.Method = WebRequestMethods.Ftp.ListDirectoryDetails;
                _PollingRequest.Credentials = new NetworkCredential(_FTPUser, _FTPPassword);

                // You should do a better chack of the response before using it
                FtpWebResponse response = (FtpWebResponse)_PollingRequest.GetResponse();
                if (response != null)
                {
                    using (StreamReader reader = new StreamReader(response.GetResponseStream()))
                    {
                        _sbListUpdate.Clear();
                        _sbListUpdate.Append(reader.ReadToEnd());
                        if (_sbListUpdate.Length > 0)
                        {
                            // This is to only get the names for down loading later
                            // WARNING: Parsing an FTP response is more art than science.  IT is fragile and can break anytime the FTP server decides to change its format
                            string[] newblocks = _sbListUpdate.ToString().Split(new string[] { " ", "\r\n", "               " }, StringSplitOptions.RemoveEmptyEntries);
                            if (newblocks != null)
                            {
                                for (int i = 0; i < newblocks.Length; i++)
                                {
                                    if (!string.IsNullOrEmpty(newblocks[i]) &&               // Is this readable 
                                        newblocks[i].Contains(".xml") &&                    // Is the a FREB file
                                        !_FRTList.Keys.Contains(newblocks[i].GetHashCode()))// Have we stored it already
                                    {
                                        _FRTList.Add(newblocks[i].GetHashCode(), newblocks[i]);
                                        retList.Add(newblocks[i]);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return retList;
        }
        #endregion
    } 

    // The implementation of IEventProcessor for processing hub events
    public class StreamEventProcessor : IEventProcessor
    {
        #region Private properties
        private PartitionContext _partitionContext;
        private Stopwatch _checkpointStopWatch;
        #endregion

        // Event handler
        public static event ProcessEventHandler ProcessEvents;
        protected virtual void RaiseEvent(EventHubDataArgs e)
        {
            if (ProcessEvents != null)
            {
                ProcessEvents(this, e);
            }
        }

        #region The IEventProcessor interface implementation
        public Task OpenAsync(PartitionContext context)
        {
            // Initializes the Event Hub processor instance. This method is called before any event data is passed 
            // to this processor instance. Ownership information for the partition on which this processor instance works. 
            // Any attempt to call CheckpointAsync will fail during the Open operation.
            _partitionContext = context;
            _checkpointStopWatch = new Stopwatch();
            _checkpointStopWatch.Start();

            return Task.FromResult<object>(null);
        }
        public async Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> events)
        {
            try
            {
                List<EventHubData> newData = new List<EventHubData>();
                foreach (EventData eventData in events)
                {
                    newData.Add(DeserializeEventData(eventData));
                }
                RaiseEvent(new EventHubDataArgs(newData));

                //Call checkpoint every 5 minutes, so that worker can resume processing from the 5 minutes back if it restarts.
                if (this._checkpointStopWatch.Elapsed > TimeSpan.FromMinutes(5))
                {
                    await context.CheckpointAsync();
                    this._checkpointStopWatch.Restart();
                }
            }
            catch (Exception exp)
            {
                // Handle in your code
                Console.WriteLine("Error in processing: " + exp.Message);
            }
        }
        public async Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            if (reason == CloseReason.Shutdown)
            {
                await context.CheckpointAsync();
            }
        }
        #endregion

        #region Method for deserializng the EventData into our EventHubData for further processing
        public  EventHubData DeserializeEventData(EventData eventData)
        {
            return EventHubData.DeserializeEventData(eventData);
        }
        #endregion
    }
}
