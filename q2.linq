<Query Kind="Program">
  <NuGetReference>MySql.Data</NuGetReference>
  <NuGetReference>YAXLib</NuGetReference>
  <Namespace>MySql.Data</Namespace>
  <Namespace>MySql.Data.MySqlClient</Namespace>
  <Namespace>System</Namespace>
  <Namespace>System.Collections.Generic</Namespace>
  <Namespace>System.Data.SqlClient</Namespace>
  <Namespace>System.Diagnostics</Namespace>
  <Namespace>System.IO</Namespace>
  <Namespace>YAXLib</Namespace>
</Query>

public class SelectWithInsert
{
	public string SelectStatement { get; set; }
	public string InsertStatement { get; set; }
}
public class ConnectionInfo
{
	// ReSharper disable once InconsistentNaming
	public string ConnectionStringToMySql { get; set; }
	public string ConnectionStringToSqlserver { get; set; }
	public List<SelectWithInsert> SelectAndInsertStatements { get; set; }
	public List<string> DeleteStatements { get; set; }
}

public class Program
{
	public static void Main(string[] args)
	{
		var serializer = new YAXSerializer(typeof(ConnectionInfo));
		CreateSampleConfigurationFile(serializer);
		var connectionInfo = serializer.Deserialize(File.ReadAllText("ConnectionInfo.txt")) as ConnectionInfo;
		var st = "Errors: \n";
		if (connectionInfo == null)
		{
			CreateErrorFile("Configuration Error");
			throw new InvalidOperationException("Configuration Error");
		}

		var connectionRead = new SqlConnection { ConnectionString = connectionInfo.ConnectionStringToSqlserver };
		var connectionWrite = new MySqlConnection { ConnectionString = connectionInfo.ConnectionStringToMySql };

		try
		{
			connectionRead.Open();
		}
		catch (Exception e)
		{
			st += "Can't Open Sqlserver. Error: " + e;
			throw;
		}
		try
		{
			connectionWrite.Open();
		}
		catch (Exception e)
		{
			st += "Can't Open MSSql. Error: " + e;
			throw;
		}
		
		
		foreach (var deleteQuery in connectionInfo.DeleteStatements)
		{
			var deleteCommand = new MySqlCommand(deleteQuery, connectionWrite);
			try { deleteCommand.ExecuteNonQuery(); }
			catch (Exception e)
			{
				st += "Can't execute delete statement: " + deleteCommand.CommandText + ". Error text: \n" + e;
				//                    throw;
			}
			deleteCommand.Dispose();
		}

		foreach (var selectWithInsert in connectionInfo.SelectAndInsertStatements)
		{
			var selectStatement = selectWithInsert.SelectStatement;
			var selectCommand = new SqlCommand(selectStatement, connectionRead);
			selectCommand.CommandTimeout = 360;
			SqlDataReader data;
			try
			{
				data = selectCommand.ExecuteReader();
			}
			catch (Exception e)
			{
				st += "Can't execute select statement: " + selectCommand.CommandText + ". Error text: \n" + e;
				throw;
			}
			var dataList = new List<List<string>>();

			while (data.Read())
			{
				var i = 0;
				var list = new List<string>();
				while (true)
				{
					try
					{
						var val = data.GetValue(i).ToString();
						list.Add(val);
						i++;
					}
					catch (Exception)
					{
						dataList.Add(list);
						break;
					}
				}
			}
			data.Close();
			selectCommand.Dispose();


			foreach (var list in dataList)
			{
				var insertStatement = selectWithInsert.InsertStatement + "(";
				var first = true;
				foreach (var value in list)
				{
					if (!first)
						insertStatement += ",";
					else
						first = false;
					if (value == "")
						insertStatement = insertStatement + "null";
					else
					{
						var val = value;
						if (value.Length > 15 && value[value.Length - 1] == 'M')
							try
							{
								val = ConvertSqlDateToMySqlDate(value);
							}
							catch (Exception e)
							{
								st += "Cant ConvertSqlDateToMySqlDate: " + value + ". Error text: \n" + e;
							    throw(e);
							}
						insertStatement = insertStatement + "'" + val + "'";
					}
				}

				insertStatement += ")";
				var insertCommand = new MySqlCommand(insertStatement, connectionWrite);
				try
				{
					insertCommand.ExecuteNonQuery();
					insertCommand.Dispose();
				}
				catch (Exception e)
				{
					st += "Cant execute command : " + insertCommand.CommandText + ". Error text: \n" + e;
//					throw(e);
				}
//				return;
			}

		}
		if (st.Length < 10)
			st += "No Errors for today. ";
		CreateErrorFile(st);
		connectionWrite.Close();
		connectionRead.Close();

	}

	private static void CreateErrorFile(string text)
	{
		File.WriteAllText("ErrorFile.txt", text);
	}

	private static void CreateSampleConfigurationFile(YAXSerializer serializer)
	{
		File.WriteAllText("sampleConfigFile.txt", serializer.Serialize(new ConnectionInfo
		{
			ConnectionStringToSqlserver = "ConnectionStringToSqlserver",
			ConnectionStringToMySql = "ConnectionStringToMSSql",
			SelectAndInsertStatements = new List<SelectWithInsert>
				{
					new SelectWithInsert {SelectStatement = "select statement 1", InsertStatement = "insert statement 1"},
					new SelectWithInsert {SelectStatement = "select statement 2", InsertStatement = "insert statement 2"}
				},
			DeleteStatements = new List<string> { "delete statement 1", "delete statement 2" }
		}));
	}

	private static string ConvertSqlDateToMySqlDate(string value)
	{
		var val = value;
		var monthString = val.Remove(val.IndexOf('/'));
		var month = int.Parse(monthString);
		val = value;
		val = val.Substring(val.IndexOf('/') + 1);
		var dayString = val.Remove(val.IndexOf('/'));
		var day = int.Parse(dayString);
		val = value;
		val = val.Substring(val.IndexOf('/') + 1);
		val = val.Substring(val.IndexOf('/') + 1);
		var yearString = val.Remove(val.IndexOf(' '));
		var year = int.Parse(yearString);
		val = value;
		val = val.Remove(0, val.IndexOf(' ') + 1);
		var v = val;
		var hourString = v.Remove(v.IndexOf(':'));
		var hour = int.Parse(hourString);
		if (value[value.Length - 2] == 'P' && hour < 12)
			hour = hour + 12;
		v = val;
		v = v.Substring(v.IndexOf(':') + 1);
		var minuteString = v.Remove(v.IndexOf(':'));
		var minute = int.Parse(minuteString);
		v = val;
		v = v.Substring(v.IndexOf(':') + 1);
		v = v.Substring(v.IndexOf(':') + 1);
		var secondString = v.Remove(v.IndexOf(' '));
		var second = int.Parse(secondString);
		val = year + "/" + month + "/" + day + " " + hour + ":" + minute + ":" + second;
		return val;
	}
}