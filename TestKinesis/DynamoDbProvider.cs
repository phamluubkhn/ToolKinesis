using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace TestKinesis
{
    public static class DdbIntro
    {
        public static void test()
        {

            string json = @"{'﻿customer_id': '808080A2', 'address': 'Tổ 23, Phú Đô, NTL HN', 'tax_code': '9090909095', 'customer_name': 'Vũ Thị C', 'email': 'Update mail sai định dạng', 'priority': '2', 'buyer_legal_name': 'Vũ Thị C'}";
            string jso2 = @"{'customer_id': '808080A2', 'address': 'Tổ 23, Phú Đô, NTL HN', 'tax_code': '9090909095', 'customer_name': 'Vũ Thị C', 'email': 'Update mail sai định dạng', 'priority': '2', 'buyer_legal_name': 'Vũ Thị C'}";
            STAGING_CUSTOMER customer = JsonConvert.DeserializeObject<STAGING_CUSTOMER>(json);
            STAGING_CUSTOMER custome2 = JsonConvert.DeserializeObject<STAGING_CUSTOMER>(jso2);
            //Chi tiết về Record
            //https://docs.aws.amazon.com/lambda/latest/dg/with-ddb.html
            Amazon.DynamoDBv2.Model.Record kinesisStreamRecord = JsonConvert.DeserializeObject<Amazon.DynamoDBv2.Model.Record>(json, new MyDateTimeConverter());
            string xxx = kinesisStreamRecord.EventName;
            //Record dyanmoRecordData = JsonConvert.DeserializeObject<Record> (json, new MyDateTimeConverter());
            if (kinesisStreamRecord != null)
            {
                TRANSACTION_DATA xxxx =  BuildCustomerObject<TRANSACTION_DATA>(kinesisStreamRecord);
            }
        }

        private static T BuildCustomerObject<T>(Record dyanmoRecordData)
        {
            







            object xx = Activator.CreateInstance(typeof(T));
            Amazon.DynamoDBv2.Model.StreamRecord streamRecord = dyanmoRecordData.Dynamodb;
            Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue> streamRecordData = streamRecord.NewImage;
            foreach (KeyValuePair<string, Amazon.DynamoDBv2.Model.AttributeValue> entry in streamRecordData)
            {
                foreach (PropertyInfo propertyInfo in xx.GetType().GetProperties())
                {

                    if (propertyInfo.Name.Equals(entry.Key, StringComparison.OrdinalIgnoreCase))
                    {
                        string rawValue = GetStreamRecordAttributeValue(entry.Value);
                        if (!string.IsNullOrEmpty(rawValue))
                        {
                            KinesisDataFieldAttribute  a = (KinesisDataFieldAttribute)propertyInfo.GetCustomAttribute(typeof(KinesisDataFieldAttribute), false);
                            switch (a.DataFieldType)
                            {
                                case KinesisDataFieldType.StringType:
                                    propertyInfo.SetValue(xx, rawValue);
                                    break;
                                case KinesisDataFieldType.IntegerType:
                                    propertyInfo.SetValue(xx, int.Parse(rawValue));
                                    break;
                                case KinesisDataFieldType.DecimalType:
                                    propertyInfo.SetValue(xx, decimal.Parse(rawValue));
                                    break;
                                case KinesisDataFieldType.DatetimeType:
                                    propertyInfo.SetValue(xx, DateTime.Parse(rawValue, null, System.Globalization.DateTimeStyles.RoundtripKind));
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                }
            }




            


            return (T)Convert.ChangeType(xx, typeof(T));
        }


        /// <summary>
        /// Vào đây để xem mô tả chi tiết các kiểu dữ liệu
        /// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DotNetSDKHighLevel.html
        /// </summary>
        /// <param name="attributeValue"></param>
        /// <returns></returns>
        private static string GetStreamRecordAttributeValue(AttributeValue attributeValue)
        {
            string result = "";

            foreach (PropertyInfo propertyInfo in attributeValue.GetType().GetProperties())
            {
                object objResult = null;
                switch (propertyInfo.Name)
                {
                    //All number types: N (number type)
                    //bool: N (number type). 0 represents false and 1 represents true.
                    case "N":
                    //All string types
                    //DateTime : The DateTime values are stored as ISO-8601 formatted strings.
                    case "S":
                        objResult = attributeValue.GetType().GetProperty(propertyInfo.Name).GetValue(attributeValue, null);
                        break;
                    default:
                        break;
                }
                
                if (objResult != null)
                {
                    result = objResult.ToString();
                }
                if (!string.IsNullOrEmpty(result))
                {
                    break;
                }
            }
            return result;
        }



        public static async Task<JArray> ReadJsonMovieFile_async(string jsonMovieFilePath)
        {
            StreamReader sr = null;
            JsonTextReader jtr = null;
            JArray movieArray = null;

            Console.WriteLine("  -- Reading the movies data from a JSON file...");

            try
            {
                sr = new StreamReader(jsonMovieFilePath);
                jtr = new JsonTextReader(sr);
                movieArray = (JArray)await JToken.ReadFromAsync(jtr);
            }
            catch (Exception ex)
            {
                Console.WriteLine("     ERROR: could not read the file!\n          Reason: {0}.", ex.Message);
            }
            finally
            {
                jtr?.Close();
                sr?.Close();
            }

            return movieArray;
        }
    }

    
}
