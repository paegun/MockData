using System;
using System.Data;

namespace MockData
{
	public static class DataReaderExtensions
	{
		public static long GetTimestamp(this IDataReader reader, int fieldOrdinal)
		{
			var rowversion = new byte[sizeof(long)];
			reader.GetBytes(fieldOrdinal, 0, rowversion, 0, rowversion.Length);
			var timestamp = BitConverter.ToInt64(rowversion, 0);
			return timestamp;
		}
	}
}
