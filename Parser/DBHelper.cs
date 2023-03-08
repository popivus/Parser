using System;
using System.Data;
using System.Data.SqlClient;

namespace Parser
{
    static class DBHelper
    {
        public static string connectionString = @"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=Lesegais;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False";
        private static SqlConnection connection;
        private static SqlCommand cmd;
        private static DataSet dataSet;
        private static SqlDataAdapter dataAdapter;
        public static string CmdScalar(string command)
        {
            connection = new SqlConnection(connectionString);
            try
            {
                connection.Open();
                cmd = new SqlCommand(command, connection);
                cmd.ExecuteNonQuery();
                if (!command.StartsWith("INSERT"))
                {
                    if (cmd.ExecuteScalar() != null) return cmd.ExecuteScalar().ToString();
                    else return null;
                }
                else return null;
            }
            catch (Exception ex)
            {
                Log.Add(ex.Message);
                return null;
            }
            finally
            {
                connection.Close();
            }
        }

        
        public static DataSet FillDataSet(string command)
        {
            dataSet = new DataSet();
            connection = new SqlConnection(connectionString);
            try
            {
                connection.Open();
                dataAdapter = new SqlDataAdapter(command, connection);
                dataAdapter.Fill(dataSet);
                return dataSet;
            }
            catch (Exception ex)
            {
                Log.Add(ex.Message);
                return null;
            }
            finally
            {
                connection.Close();
            }
        }
    }
}
