using System;
using System.Data;
using System.Linq;
using MockData;

namespace MockDataTest
{
	public class ResultSetTests
	{
		public static void Main(string[] args)
		{
			var rst = new ResultSetTests();
			rst.X();
			Console.WriteLine("<ENTER> to exit");
			Console.ReadLine();
		}
		
		// TODO: convert to tests
		public void X()
		{
			var cars = new []
			{
				new Car { Make = "Ford", Model = "Pinto", RV = BitConverter.GetBytes(0xDEADBEEF) },
				new Car { Make = "Chevy", Model = "Nova" }
			};

			var	makers = new []
			{
				new Maker { Name = "Ford" },
				new Maker { Name = "Chevy" }
			};
			
			var results = new ResultSet();
			var carFields = new [] { "Make", "Model", "RV" };
			var makerFields = new [] { "Name" };
			results.DefineSchema(0, makers.FirstOrDefault(), makerFields);
			results.DefineSchema(1, cars.FirstOrDefault(), carFields);
			
			results.AddRecords(0, makers, makerFields);
			results.AddRecords(1, cars, carFields);
			
			results.AddRecord(0, new object[]{ "Saturn" });
			results.AddRecord(1, new object[]{ "Saturn", "Ion", 17 });
			
			foreach(DataColumn column in results.GetSchemaTable().Columns)
			{
				Console.WriteLine("Column Name: '{0}', Type: '{1}'", column.ColumnName, column.DataType.Name);
			}
			while(results.Read())
			{
				Console.WriteLine("Maker, Name: '{0}'"
				                  , results.GetString(0));
			}
			if(results.NextResult())
			{
				foreach(DataColumn column in results.GetSchemaTable().Columns)
				{
					Console.WriteLine("Column Name: '{0}', Type: '{1}'", column.ColumnName, column.DataType.Name);
				}
				while(results.Read())
				{
					Console.WriteLine("Car, Make: '{0}', Model: '{1}', TimeStamp: {2}"
					                  , results.GetString(0), results.GetString(1), results.GetTimestamp(2));
				}
			}
		}
	}
}
