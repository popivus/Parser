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

        private static int dealsPerRequest = 20;
        private static int requestDelayMilliseconds = 200;
        static void Main(string[] args)
        {
            while (true)
            {
                int dealsCount = GetDealsCount();
                int iterations = dealsCount / dealsPerRequest;
                List<WoodDeal> currentWoodDeals = GetDealsFromDB();

                Log.Add("=====Start parsing=====");
                Log.Add($"All deals count: {dealsCount}");
                Log.Add($"Iterations: {iterations}");

                for (int i = 0; i <= iterations; i++)
                {
                    int dealStartNumber = i * dealsPerRequest;
                    int dealsCountNow = dealsPerRequest;

                    if (i == iterations)
                        dealsCountNow = dealsCount - dealStartNumber;

                    WoodDeal[] woodDeals = GetWoodDeals(dealsCountNow, i);
                    if (woodDeals.Length == 0)
                    {
                        Thread.Sleep(delayMillisecondsError);
                        continue;
                    }

                    string query = "";
                    foreach (WoodDeal deal in woodDeals)
                    {
                        if (string.IsNullOrWhiteSpace(deal.dealNumber) ||
                            string.IsNullOrWhiteSpace(deal.sellerName) ||
                            string.IsNullOrWhiteSpace(deal.buyerName) ||
                            string.IsNullOrWhiteSpace(deal.dealDate))
                            continue;

                        if (deal.buyerInn.Length != 12 &&
                            deal.buyerInn.Length != 10 &&
                            deal.buyerInn.Length != 0)
                            continue;

                        if (deal.sellerInn.Length != 12 &&
                            deal.sellerInn.Length != 10 &&
                            deal.sellerInn.Length != 0)
                            continue;

                        foreach (char c in deal.buyerInn)
                            if (!char.IsDigit(c)) continue;

                        foreach (char c in deal.sellerInn)
                            if (!char.IsDigit(c)) continue;

                        if (!DateTime.TryParse(deal.dealDate, out DateTime testDateTime))
                            continue;

                        var current = currentWoodDeals.FirstOrDefault(d => d.dealNumber == deal.dealNumber);
                        if (current == null)
                        {
                            query += $"INSERT INTO [dbo].[WoodDeal] ([DealNumber], [SellerName], [SellerInn], [BuyerName], [BuyerInn], [WoodVolumeBuyer], [WoodVolumeSeller], [DealDate], [__typename]) VALUES (N'{deal.dealNumber}', N'{deal.sellerName.Replace('\'', '"')}', N'{deal.sellerInn}', N'{deal.buyerName.Replace('\'', '"')}', N'{deal.buyerInn}', {deal.woodVolumeBuyer.ToString().Replace(',', '.')},  {deal.woodVolumeSeller.ToString().Replace(',', '.')}, N'{deal.dealDate}', N'{deal.__typename}')\n";
                        }
                        else
                        {
                            if (current.sellerName != deal.sellerName ||
                                current.sellerInn != deal.sellerInn ||
                                current.buyerName != deal.buyerName ||
                                current.buyerInn != deal.buyerInn ||
                                current.woodVolumeBuyer != deal.woodVolumeBuyer ||
                                current.woodVolumeSeller != deal.woodVolumeSeller ||
                                current.dealDate != deal.dealDate ||
                                current.__typename != deal.__typename)
                                query += $"UPDATE [dbo].[WoodDeal] SET [SellerName] = '{deal.sellerName}', [SellerInn] = N'{deal.sellerInn}', [BuyerName] = N'{deal.buyerName}', [BuyerInn] = N'{deal.buyerInn}', [WoodVolumeBuyer] = {deal.woodVolumeBuyer.ToString().Replace(',', '.')}, [WoodVolumeSeller] = {deal.woodVolumeSeller.ToString().Replace(',', '.')}, [DealDate] = N'{deal.dealDate}', [__typename] = N'{deal.__typename}' WHERE [DealNumber] = N'{deal.dealNumber}'\n";
                        }

                    }

                    if (query != "") 
                        DBHelper.CmdScalar(query);

                    Log.Add($"=====Part {i + 1} inserted=====");
                    Thread.Sleep(requestDelayMilliseconds);
                }
                Log.Add("=====Done=====");
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

        private static WoodDeal[] GetWoodDeals(int count, int startNumber)
        {
            string body = "{\"query\":\"query SearchReportWoodDeal($size: Int!, $number: Int!, $filter: Filter, $orders: [Order!]) {\\n  searchReportWoodDeal(filter: $filter, pageable: {number: $number, size: $size}, orders: $orders) {\\n    content {\\n      sellerName\\n      sellerInn\\n      buyerName\\n      buyerInn\\n      woodVolumeBuyer\\n      woodVolumeSeller\\n      dealDate\\n      dealNumber\\n      __typename\\n    }\\n    __typename\\n  }\\n}\\n\",\"variables\":{\"size\": " + count.ToString() + ",\"number\":" + startNumber.ToString() + ",\"filter\":null,\"orders\":null},\"operationName\":\"SearchReportWoodDeal\"}";

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
                Log.Add(err.Message);
                return null;
            }
        }
    }
}
