using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.IO;
using Newtonsoft.Json;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System.Configuration;

namespace SBSendQueueConApp
{
  class Program
  {
    static NamespaceManager _namespaceManager;
    static List<BrokeredMessage> _messageList;

    static void Main(string[] args)
    {
      List<Category> categories = GetCategories();
      _messageList = GenerateMessages(categories);
      Console.WriteLine($"MessageList.Count -> {_messageList.Count()}");
      CollectSBDetails();
      CreateQueue(_messageList.Count);
      Console.ReadKey(true);
    }

    private static void CreateQueue(int messageCount)
    {
      QueueDescription catsQueue = null;
      if (!_namespaceManager.QueueExists("categoryqueue"))
      {
        catsQueue = _namespaceManager.CreateQueue("categoryqueue");
      }

      MessagingFactory factory = MessagingFactory.Create(_namespaceManager.Address, _namespaceManager.Settings.TokenProvider);

      QueueClient catsQueueClient = factory.CreateQueueClient("categoryqueue");
      Console.WriteLine("Sending messages to the queueu.....");
      for (int i = 0; i < messageCount; i++)
      {
        var cat = _messageList[i];
        cat.Label = cat.Properties[(i + 1).ToString()].ToString();
        //Console.WriteLine(cat.GetType());
        catsQueueClient.Send(cat);
        Console.WriteLine($"Message ID: {cat.MessageId},\nMessage Sent Cat: {cat.Label}");
      }
      Console.WriteLine("All Message Sent");
    }

    private static void CollectSBDetails()
    {
      _namespaceManager = NamespaceManager.CreateFromConnectionString(ConfigurationManager.AppSettings["Microsoft.ServiceBus.ConnectionString"].ToString());
    }
    private static List<BrokeredMessage> GenerateMessages(List<Category> categories)
    {
      List<BrokeredMessage> result = new List<BrokeredMessage>();
      foreach (var item in categories)
      {
        //Console.WriteLine($"{item.CategoryID} -> {item}");
        BrokeredMessage message = new BrokeredMessage();
        message.Properties.Add(item.CategoryID.ToString(), item.ToString());
        result.Add(message);
        //Console.WriteLine(result[ctr++]);
      }
      return result;
    }

    private static List<Category> GetCategories()
    {
      var data = File.ReadAllText(@"../../Categories.json");
      var categories = JsonConvert.DeserializeObject<List<Category>>(data);
      return categories;
    }
  }
}
