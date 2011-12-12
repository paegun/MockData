using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace MockData
{	
	public class ResultSet : IDataReader
	{
		private class FieldDefinition
		{
			public Type Type { get; set; }
			public string Name { get; set; }
		}

		private FieldDefinition[] Fields { get; set; }
		private List<object[]> Records { get; set; }
		private ResultSet Next;

		private Dictionary<string, int> FieldOrdinals;
		private int CurrentRecord = -1;
		private bool _isClosed = false;

		public void DefineSchema(int resultSetOrdinal, object value, string[] orderedFieldNames)
		{
			if(resultSetOrdinal < 0) throw new ArgumentOutOfRangeException("resultSetOrdinal");
			var type = value.GetType();
			var members = orderedFieldNames;
			var properties = type.GetProperties()
				.ToDictionary(it => it.Name, it => it.PropertyType);
			var fields = type.GetFields()
				.ToDictionary(it => it.Name, it => it.FieldType);
			
			var resultSet = this;			
			while(--resultSetOrdinal >= 0)
			{
				if (resultSet.Next == null)
				{
					resultSet.Next = new ResultSet();
				}
				resultSet = resultSet.Next;
			}
			
			var fieldDefinitions = new List<FieldDefinition>();
			foreach(var member in members)
			{
				var memberName = member;
				Type memberType;
				if(!properties.TryGetValue(memberName, out memberType))
				{
					if(!fields.TryGetValue(memberName, out memberType))
					{
					}
				}
				if (memberType != default(Type))
				{
					fieldDefinitions.Add(new FieldDefinition{ Name = memberName, Type = memberType });
				}
			}
			resultSet.Fields = fieldDefinitions.ToArray();
		}

		public void AddRecords(int resultSetOrdinal, object[] values, string[] orderedFieldNames)
		{
			if(resultSetOrdinal < 0) throw new ArgumentOutOfRangeException("resultSetOrdinal");
			var type = values.FirstOrDefault().GetType();
			var members = orderedFieldNames;
			var properties = type.GetProperties()
				.ToDictionary(it => it.Name, it => it);
			var fields = type.GetFields()
				.ToDictionary(it => it.Name, it => it);

			var resultSet = this;			
			while(--resultSetOrdinal >= 0)
			{
				if (resultSet.Next == null)
				{
					resultSet.Next = new ResultSet();
				}
				resultSet = resultSet.Next;
			}
			
			var valuesLength = fields.Count + properties.Count;
			foreach(var value in values)
			{
				var recordValueIndex = -1;
				var recordValues = new object[valuesLength];
				foreach(var member in members)
				{
					var memberName = member;
					PropertyInfo propertyInfo;
					FieldInfo fieldInfo;
					if(properties.TryGetValue(memberName, out propertyInfo))
					{
						recordValues[++recordValueIndex] = propertyInfo.GetValue(value, null);
					}
					else if(fields.TryGetValue(memberName, out fieldInfo))
					{
						recordValues[++recordValueIndex] = fieldInfo.GetValue(value);
					}
				}
				if (null == resultSet.Records) resultSet.Records = new List<object[]>();
				resultSet.Records.Add(recordValues);
			}
		}

		public void AddRecord(int resultSetOrdinal, object[] values)
		{
			if(resultSetOrdinal < 0) throw new ArgumentOutOfRangeException("resultSetOrdinal");

			var resultSet = this;			
			while(--resultSetOrdinal >= 0)
			{
				if (resultSet.Next == null)
				{
					resultSet.Next = new ResultSet();
				}
				resultSet = resultSet.Next;
			}
			
			if(values.Length != resultSet.FieldCount)
			{
				throw new ArgumentException("Record values count must match the defined schema FieldCount.", "values");
			}
			
			var recordValues = values;
			if (null == resultSet.Records) resultSet.Records = new List<object[]>();
			resultSet.Records.Add(recordValues);
		}
		
		#region IDataReader
		
		public int GetOrdinal(string fieldName)
		{
			if(FieldOrdinals == null)
			{
				var fieldOrdinals = new Dictionary<string, int>();
				var i = -1;
				foreach(var field in Fields)
				{
					fieldOrdinals[field.Name] = ++i;
				}
				FieldOrdinals = fieldOrdinals;
			}
			return FieldOrdinals[fieldName];
		}
		public object this[string fieldName]
		{ 
			get 
			{
				return this[GetOrdinal(fieldName)];
			}
		}
		
		public object this[int fieldOrdinal]
		{ 
			get 
			{
				return Records[CurrentRecord][fieldOrdinal];
			}
		}
		
		public int FieldCount
		{
			get { return (null == Fields) ? -1 : Fields.Length; }
		}
		
		public bool IsDBNull(int fieldOrdinal)
		{
			var fieldValue = this[fieldOrdinal];
			return (fieldValue == null);
		}
		
		public IDataReader GetData(int fieldOrdinal)
		{
			throw new NotImplementedException("Nested DataReader is not supported");
		}
		
		public DateTime GetDateTime(int fieldOrdinal)
		{
			return (DateTime)this[fieldOrdinal];
		}

		public Decimal GetDecimal(int fieldOrdinal)
		{
			return (Decimal)this[fieldOrdinal];
		}
		
		public String GetString(int fieldOrdinal)
		{
			return (string)this[fieldOrdinal];
		}

		public Double GetDouble(int fieldOrdinal)
		{
			return (Double)this[fieldOrdinal];
		}

		public float GetFloat(int fieldOrdinal)
		{
			return (float)this[fieldOrdinal];
		}

		public Int64 GetInt64(int fieldOrdinal)
		{
			return (Int64)this[fieldOrdinal];
		}

		public Int32 GetInt32(int fieldOrdinal)
		{
			return (Int32)this[fieldOrdinal];
		}
		
		public Int16 GetInt16(int fieldOrdinal)
		{
			return (Int16)this[fieldOrdinal];
		}

		public Guid GetGuid(int fieldOrdinal)
		{
			return (Guid)this[fieldOrdinal];
		}
		
		public long GetChars(int fieldOrdinal, long fieldoffset, char[] buffer, int bufferoffset, int length)
		{
			var s = GetString(fieldOrdinal);
			var chars = s.Select(c => c).Skip((int)fieldoffset).Take(length).ToArray();
			var j = 0;
			var read = Math.Min(chars.Length, Math.Min(length, buffer.Length));
			for(var i = bufferoffset; i < read; ++i,++j)
			{
				buffer[i] = chars[j];
			}
			return read;
		}
		
		public long GetBytes(int fieldOrdinal, long fieldoffset, byte[] buffer, int bufferoffset, int length)
		{
			var bytes = this[fieldOrdinal] as byte[];
			if (null == bytes) return 0;
			var bufferLength = buffer.Length;
			var j = 0;
			var read = Math.Min(bytes.Length, Math.Min(length, buffer.Length));
			for(var i = bufferoffset; i < read; ++i,++j)
			{
				buffer[i] = bytes[j];
			}
			return read;
		}
		
		public Char GetChar(int fieldOrdinal)
		{
			return (Char)this[fieldOrdinal];
		}

		public Byte GetByte(int fieldOrdinal)
		{
			return (Byte)this[fieldOrdinal];
		}

		public Boolean GetBoolean(int fieldOrdinal)
		{
			return (Boolean)this[fieldOrdinal];
		}
	
		public int GetValues(object[] values)
		{
			values = Records[CurrentRecord];
			return FieldCount;
		}
		
		public object GetValue(int fieldOrdinal)
		{
			return this[fieldOrdinal];
		}
		
		public Type GetFieldType(int fieldOrdinal)
		{
			return Fields[fieldOrdinal].Type;
		}
		
		public string GetDataTypeName(int fieldOrdinal)
		{
			return Fields[fieldOrdinal].Type.Name;
		}

		public string GetName(int fieldOrdinal)
		{
			return Fields[fieldOrdinal].Name;
		}
		
		public void Dispose() { /*nop*/ }
		
		public int RecordsAffected
		{
			get { return (null == Records) ? -1 : Records.Count - 1; }
		}
		
		public bool IsClosed
		{
			get { return _isClosed; }
		}
		
		public int Depth
		{
			get { throw new NotImplementedException("Nested DataReader is not supported"); }
		}
		
		public bool Read()
		{
			++CurrentRecord;
			return (CurrentRecord <= RecordsAffected);
		}
		
		public bool NextResult()
		{
			if (null == Next) return false;
			Fields = Next.Fields;
			Records = Next.Records;
			Next = Next.Next;
			CurrentRecord = -1;
			return true;
		}
		
		public DataTable GetSchemaTable()
		{
			var dataTable = new DataTable();
			var dataColumns =
				(from field in Fields
				 select new DataColumn(field.Name, field.Type)
				).ToArray();
			dataTable.Columns.AddRange(dataColumns);
			return dataTable;
		}
		
		public void Close()
		{
			_isClosed = true;
		}
		
		#endregion IDataReader
	}
}