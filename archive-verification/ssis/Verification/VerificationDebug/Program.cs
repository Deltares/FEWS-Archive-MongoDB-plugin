using System;
using System.IO;
using System.Web.Script.Serialization;
using System.Linq;
using System.Net.Mail;
using System.Threading;
using Microsoft.AnalysisServices;
using Database = Microsoft.AnalysisServices.Tabular.Database;
using Server = Microsoft.AnalysisServices.Tabular.Server;

namespace ST_589451501c0a4de7b0e22ca56d1c813b
{
	public class Program
	{
		private const int MaxRetries = 5;

		/// <summary>
		/// Main
		/// </summary>
		public static void Main()
		{
			try
			{
				dynamic settings = new JavaScriptSerializer().Deserialize<dynamic>(File.ReadAllText(Path.Combine("\\\\knxdwfewsmc1\\archive\\Verification", "Settings.json")));
				string bimPath = settings["bimPath"];
				string tabularConnectionString = settings["tabularConnectionString"];

				Directory.GetFiles(bimPath, "*.bim").ToList().ForEach(bimFile => Deploy(tabularConnectionString, bimFile));

				//Dts.TaskResult = (int)DTSExecResult.Success;
			}
			catch (Exception)
			{
				//Dts.TaskResult = (int)DTSExecResult.Failure;
			}
		}

		/// <summary>
		/// Deploy
		/// </summary>
		/// <param name="tabularConnectionString"></param>
		/// <param name="bimFile"></param>
		/// <returns></returns>
		private static void Deploy(string tabularConnectionString, string bimFile)
		{
			for (int retry = 1; retry <= MaxRetries; retry++)
			{
				Server server = new Server();
				try
				{
					server.Connect(tabularConnectionString);
					var databaseName = Path.GetFileNameWithoutExtension(bimFile);
					var bim = File.ReadAllText(bimFile);

					dynamic cube = new JavaScriptSerializer().Deserialize<dynamic>(bim);
					var database = server.Databases.FindByName(databaseName);

					if (!UpdateCapable(cube, database))
						server.Execute($"{{\"delete\": {{\"object\": {{\"database\": \"{databaseName}\"}}}}}}");

					var result = server.Execute($"{{\"createOrReplace\": {{\"object\": {{\"database\": \"{databaseName}\"}}, \"database\": {bim}}}}}");
					if (result.ContainsErrors)
						throw new Exception($"{bimFile}\n{tabularConnectionString}\n\n{string.Join("\n", result.Cast<XmlaResult>().SelectMany(r => r.Messages.Cast<XmlaMessage>()).Select(m => m.Description))}");

					//bool fireAgain = true;
					//Dts.Events.FireInformation(0, MethodBase.GetCurrentMethod().Name, $"{MethodBase.GetCurrentMethod().Name}:{bimFile} -> {tabularConnectionString}", string.Empty, 0, ref fireAgain);
					break;
				}
				catch (Exception ex)
				{
					//Dts.Events.FireWarning(0, $"WARNING - {MethodBase.GetCurrentMethod().Name} ({Environment.MachineName.ToUpper()}:{Dts.Variables["System::PackageName"].Value})", $"{ex}\n\n{bimFile} -> {tabularConnectionString}", string.Empty, 0);
					if (retry >= MaxRetries)
					{
						//Dts.Events.FireError(0, $"ERROR - {MethodBase.GetCurrentMethod().Name} ({Environment.MachineName.ToUpper()}:{Dts.Variables["System::PackageName"].Value})", $"{ex}", string.Empty, 0);
						//SendMail($"ERROR - {MethodBase.GetCurrentMethod().Name} ({Environment.MachineName.ToUpper()}:{Dts.Variables["System::PackageName"].Value})", $"{ex}");
						throw;
					}
					Thread.Sleep(1000 * 30);
				}
				finally
				{
					server.Disconnect();
				}
			}
		}

		/// <summary>
		/// UpdateCapable
		/// </summary>
		/// <param name="cube"></param>
		/// <param name="database"></param>
		/// <returns></returns>
		private static bool UpdateCapable(dynamic cube, Database database)
		{
			var compatibilityLevel = (int)cube["compatibilityLevel"];
			var clientCompatibilityLevel = cube["model"]["annotations"][0]["value"];

			var existingCompatibilityLevel = database?.CompatibilityLevel;
			var existingClientCompatibilityLevel = database?.Model.Annotations.Find("ClientCompatibilityLevel").Value;

			return database == null || compatibilityLevel == existingCompatibilityLevel && clientCompatibilityLevel == existingClientCompatibilityLevel;
		}

		/// <summary>
		/// SendMail
		/// </summary>
		/// <param name="subject"></param>
		/// <param name="message"></param>
		private static void SendMail(string subject, string message)
		{
			dynamic settings = new JavaScriptSerializer().Deserialize<dynamic>(File.ReadAllText(Path.Combine("\\\\knxdwfewsmc1\\archive\\Verification", "Settings.json")));
			try
			{
				string smtpConnectionString = settings["smtpConnectionString"];
				var smtp = smtpConnectionString.Split(';').Where(s => s.Contains("=")).ToDictionary(s => s.Split('=')[0], s => s.Split('=')[1]);
				var smtpServer = smtp["SmtpServer"];
				SmtpClient smtpClient = new SmtpClient(smtpServer);
				smtpClient.Send(settings["fromEmailAddress"], settings["toEmailAddresses"], subject, message);
			}
			catch (Exception e)
			{
				//Dts.Events.FireError(0, $"ERROR - {MethodBase.GetCurrentMethod().Name} ({Environment.MachineName.ToUpper()}:{Dts.Variables["System::PackageName"].Value})", $"{e}", string.Empty, 0);
			}
		}
	}
}