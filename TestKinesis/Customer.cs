using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestKinesis
{
    //public class STAGING_CUSTOMER
    //{
    //    [KinesisDataField(KinesisDataFieldType.StringType)]
    //    public string CUSTOMER_ID { get; set; }

    //    [KinesisDataField(KinesisDataFieldType.StringType)]
    //    public string CUSTOMER_NAME { get; set; }

    //    [KinesisDataField(KinesisDataFieldType.StringType)]
    //    public string ADDRESS { get; set; }

    //    [KinesisDataField(KinesisDataFieldType.StringType)]
    //    public string EMAIL { get; set; }
    //    [KinesisDataField(KinesisDataFieldType.StringType)]
    //    public string TAX_CODE { get; set; }

    //    [KinesisDataField(KinesisDataFieldType.StringType)]
    //    public string BUYER_LEGAL_NAME { get; set; }

    //    [KinesisDataField(KinesisDataFieldType.IntegerType)]
    //    public int PRIORITY { get; set; }
    //}

    public class STAGING_CUSTOMER
    {
        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string CUSTOMER_ID { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string CUSTOMER_NAME { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string ADDRESS { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string EMAIL { get; set; }
        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string TAX_CODE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string BUYER_LEGAL_NAME { get; set; }

        [KinesisDataField(KinesisDataFieldType.IntegerType)]
        public int? PRIORITY { get; set; }
    }

    public class CUSTOMER_BANK_ACCOUNT
    {

        [KinesisDataField(KinesisDataFieldType.DatetimeType)]
        public DateTime? ACCOUNT_CLOSED_DATE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string ACCOUNT_NUMBER { get; set; }

        public string CUSTOMER_CODE { get; set; }
        public DateTime? DS_PARTITION_DATE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string CATEGORY { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string CURRENCY { get; set; }

        [KinesisDataField(KinesisDataFieldType.IntegerType)]
        public int STATUS { get; set; }
    }

    public class COMPANY
    {
        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string COMPANY_CODE { get; set; }

        public DateTime DS_PARTITION_DATE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string NAME { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string MNEMONIC { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string TAX_CODE { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string EMAIL { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string PHONE_NUMBER { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string FAX { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string LEGAL_REPRESENT { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string NAME_ADDRESS { get; set; }
        public string NAME_LEAD_COM { get; set; }

        [KinesisDataField(KinesisDataFieldType.StringType)]
        public string STATUS { get; set; }

        [KinesisDataField(KinesisDataFieldType.DatetimeType)]
        public DateTime LST_UPDATE_DATE { get; set; }
    }


    public class KinesisStreamRecord
    {
        public string AwsRegion { get; set; }
        public string EventID { get; set; }
        public string EventName { get; set; }
        public string EventSource { get; set; }
        public string RecordFormat { get; set; }
        public string TableName { get; set; }
        public string UserIdentity { get; set; }

        public Amazon.DynamoDBv2.Model.StreamRecord Dynamodb { get; set; }
    }


    public class MyDateTimeConverter : Newtonsoft.Json.JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(DateTime);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var t = long.Parse(reader.Value.ToString());
            return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(t);
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }
    }


    public class KinesisRecordPropertiesConst
    {
        public const string StringTypeFields = ";ENTRY_ID;ENTRY_TYPE;BUYER_CODE;BRANCH_CODE;COMPANY_CODE;BUYER_BANK_ACCOUNT;CURRENCY_CODE;PAYMENT_METHOD_NAME;INV_TYPE_CODE;INV_NOTE;TRANS_NO;ITEM_NAME;UNIT_NAME;VAT_CATEGORY_PERCENTAGE;IS_SOURCE;MODULE;ACCOUNT_CO_CODE;PALCAT;TRANSACTION_TYPE;REVERT_FLAG;TRANSACTION_CODE;ORIGIN_TRANS_REF;";
        public const string DatetimeTypeFields = "TRANFER_DATE;PROCESS_DATE;CREATION_DATE";
        public const string IntegerTypeFields = "";
        public const string DecimalTypeFields = ";EXCHANGE_RATE;QUANTITY";
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class KinesisDataFieldAttribute : Attribute
    {
        private KinesisDataFieldType dataFieldType;

        public KinesisDataFieldAttribute(KinesisDataFieldType dataFieldType)
        {
            this.dataFieldType = dataFieldType;

        }

        public virtual KinesisDataFieldType DataFieldType
        {
            get { return dataFieldType; }
        }
    }


    public enum KinesisDataFieldType
    {
        StringType = 1,
        IntegerType = 2,
        DatetimeType = 3,
        DecimalType = 4,
    }
}
