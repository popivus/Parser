using Newtonsoft.Json;
using Parser.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;

namespace Parser
{
    internal class Program
    {
        private static string URL = "https://www.lesegais.ru/open-area/deal";
        private static int delayMilliseconds = 600000;
        private static int delayMillisecondsError = 60000;
        static void Main(string[] args)
        {
            while (true)
            {
                int dealsCount = GetDealsCount();
                WoodDeal[] woodDeals = GetWoodDeals(dealsCount);
                if (woodDeals.Length == 0)
                {
                    Thread.Sleep(delayMillisecondsError);
                    continue;
                }

                string query = "";
                List<WoodDeal> currentWoodDeals = GetDealsFromDB();
                foreach (WoodDeal deal in woodDeals)
                {
                    if (currentWoodDeals.Exists(d => d.dealNumber == deal.dealNumber))
                        query += $"UPDATE [dbo].[WoodDeal] SET [SellerName] = '{deal.sellerName}', [SellerInn] = '{deal.sellerInn}', [BuyerName] = '{deal.buyerName}', [BuyerInn] = '{deal.buyerInn}', [WoodVolumeBuyer] = {deal.woodVolumeBuyer.ToString().Replace(',','.')}, [WoodVolumeSeller] = {deal.woodVolumeSeller.ToString().Replace(',', '.')}, [DealDate] = '{deal.dealDate}', [__typename] = '{deal.__typename}' WHERE [DealNumber] = '{deal.dealNumber}'\n";
                    else
                        query += $"INSERT INTO [dbo].[WoodDeal] ([DealNumber], [SellerName], [SellerInn], [BuyerName], [BuyerInn], [WoodVolumeBuyer], [WoodVolumeSeller], [DealDate], [__typename]) VALUES ('{deal.dealNumber}', '{deal.sellerName.Replace('\'','"')}', '{deal.sellerInn}', '{deal.buyerName.Replace('\'', '"')}', '{deal.buyerInn}', {deal.woodVolumeBuyer.ToString().Replace(',', '.')},  {deal.woodVolumeSeller.ToString().Replace(',', '.')}, '{deal.dealDate}', '{deal.__typename}')\n";

                    if (query.Split('\n').Length >= 1000)
                    {
                        DBHelper.CmdScalar(query);
                        query = "";

                        Console.WriteLine("=====Part inserted=====");
                    }
                }

                DBHelper.CmdScalar(query);
                Console.WriteLine("=====Done=====");
                Thread.Sleep(delayMilliseconds);
            }
        }

        private static List<WoodDeal> GetDealsFromDB()
        {
            DataTable table = DBHelper.FillDataSet("SELECT * FROM [dbo].[WoodDeal]").Tables[0];
            List<WoodDeal> woodDeals = new List<WoodDeal>();
            foreach (DataRow row in table.Rows)
            {
                WoodDeal woodDeal = new WoodDeal()
                {
                    dealNumber = row.ItemArray[0].ToString(),
                    sellerName = row.ItemArray[1].ToString(),
                    sellerInn = row.ItemArray[2].ToString(),
                    buyerName = row.ItemArray[3].ToString(),
                    buyerInn = row.ItemArray[4].ToString(),
                    woodVolumeBuyer = float.Parse(row.ItemArray[5].ToString()),
                    woodVolumeSeller = float.Parse(row.ItemArray[6].ToString()),
                    dealDate = row.ItemArray[7].ToString(),
                    __typename = row.ItemArray[8].ToString()
                };
                woodDeals.Add(woodDeal);
            }
            return woodDeals;
        }

        private static int GetDealsCount()
        {
            string body = "{\"query\":\"query SearchReportWoodDealCount($size: Int!, $number: Int!, $filter: Filter, $orders: [Order!]) {\\n  searchReportWoodDeal(filter: $filter, pageable: {number: $number, size: $size}, orders: $orders) {\\n    total\\n    number\\n    size\\n    overallBuyerVolume\\n    overallSellerVolume\\n    __typename\\n  }\\n}\\n\",\"variables\":{\"size\":20,\"number\":0,\"filter\":null},\"operationName\":\"SearchReportWoodDealCount\"}";

            Rootobject rootobject = GetData(body);
            if (rootobject.data == null)
                return 0;

            return rootobject.data.searchReportWoodDeal.total;
        }

        private static WoodDeal[] GetWoodDeals(int count)
        {
            string body = "{\"query\":\"query SearchReportWoodDeal($size: Int!, $number: Int!, $filter: Filter, $orders: [Order!]) {\\n  searchReportWoodDeal(filter: $filter, pageable: {number: $number, size: $size}, orders: $orders) {\\n    content {\\n      sellerName\\n      sellerInn\\n      buyerName\\n      buyerInn\\n      woodVolumeBuyer\\n      woodVolumeSeller\\n      dealDate\\n      dealNumber\\n      __typename\\n    }\\n    __typename\\n  }\\n}\\n\",\"variables\":{\"size\": " + count.ToString() + ",\"number\":0,\"filter\":null,\"orders\":null},\"operationName\":\"SearchReportWoodDeal\"}";

            Rootobject rootobject = GetData(body);
            if (rootobject.data == null)
                return null;

            return rootobject.data.searchReportWoodDeal.content;
        }

        private static Rootobject GetData(string body)
        {
            try
            {
                var web = (HttpWebRequest)WebRequest.Create("https://www.lesegais.ru/open-area/graphql");
                web.Method = "POST";
                web.Accept = "*/*";
                web.UserAgent = "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0)";
                web.ContentType = "application/json";
                web.Headers.Add(HttpRequestHeader.AcceptLanguage, "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7");
                web.Headers.Add("sec-ch-ua", "\"Chromium\";v=\"110\", \"Not A(Brand\";v=\"24\", \"Google Chrome\";v=\"110\"");
                web.Headers.Add("sec-ch-ua-mobile", "?0");
                web.Headers.Add("sec-ch-ua-platform", "\"Windows\"");
                web.Headers.Add("Sec-Fetch-Dest", "empty");
                web.Headers.Add("Sec-Fetch-Mode", "cors");
                web.Headers.Add("Sec-Fetch-Site", "same-origin");
                web.Credentials = CredentialCache.DefaultCredentials;
                web.Referer = URL;
                
                byte[] dataBody = Encoding.Default.GetBytes(body);

                using (var stream = web.GetRequestStream())
                {
                    stream.Write(dataBody, 0, dataBody.Length);
                    HttpWebResponse response = (HttpWebResponse)web.GetResponse();
                    StreamReader reader = new StreamReader(response.GetResponseStream());
                    string responseString = reader.ReadToEnd();
                    var rootobject = JsonConvert.DeserializeObject<Rootobject>(responseString);
                    return rootobject;
                }
            }
            catch (Exception err)
            {
                Console.WriteLine(err.Message);
                return null;
            }
        }
    }
}
