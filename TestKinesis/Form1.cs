using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Runtime;
using Newtonsoft.Json;

namespace TestKinesis
{
    public partial class Form1 : Form
    {
        private static string customerDataText = "";
        private static StringBuilder logPanelText = new StringBuilder();
        private static AmazonKinesisClient kinesisClient;

        public Form1()
        {
            InitializeComponent();
            AppendLog("Đang khởi tạo KinesisClient");
            kinesisClient = KinesisProvider.CreateAmazonKinesisClient(RegionEndpoint.APSoutheast1);
            AppendLog($"Khởi tạo KinesisClient thành công. Region {RegionEndpoint.APSoutheast1}");
            ViewAllStream();
            //DdbIntro.test();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog = new OpenFileDialog();
            openFileDialog.CheckFileExists = false;
            openFileDialog.AddExtension = true;
            openFileDialog.Multiselect = false;
            openFileDialog.Filter = "JSON files (*.json)|*.json";

            if (openFileDialog.ShowDialog() == System.Windows.Forms.DialogResult.OK)
            {
                customerDataText = File.ReadAllText(openFileDialog.FileName);
                AppendLog("Bắt đầu đẩy dữ liệu vào stream");
                ///List<KinesisStreamRecord> customers = JsonConvert.DeserializeObject<List<KinesisStreamRecord>>(customerDataText, new MyDateTimeConverter());
                List<STAGING_CUSTOMER> customers = JsonConvert.DeserializeObject<List<STAGING_CUSTOMER>>(customerDataText);
                foreach (var item in customers)
                {
                    string rawData = JsonConvert.SerializeObject(item);
                    string rawV2 = rawData.Replace("0001-01-01T00:00:00", "1648539247112");
                    MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes(rawV2));
                    string result = KinesisProvider.PutRecordRequest(kinesisClient, "tcb-finance-eiv-data-sit-staging_customer", stream, "1", "");
                }
                AppendLog("Done");
            }
        }

        private void button3_Click(object sender, EventArgs e)
        {
            ViewAllStream();
        }

        private void ViewAllStream()
        {
            AppendLog("Danh sách Stream:");
            ListStreamsResponse listStreamsResponse = KinesisProvider.GetAllStreams(kinesisClient);
            if (listStreamsResponse != null && listStreamsResponse.StreamNames.Count > 0)
            {
                foreach (var item in listStreamsResponse.StreamNames)
                {
                    AppendLog(item);
                }
            }
            else
            {
                AppendLog("Không có stream trong danh sách");
            }
        }

        private void AppendLog(string textLog)
        {
            logPanelText.AppendLine(textLog);
            panel1.Controls.Clear();
            Label lbl = new Label();
            lbl.AutoSize = true;
            lbl.Text = logPanelText.ToString();
            panel1.Controls.Add(lbl);
            panel1.AutoScrollPosition = new Point(0, panel1.DisplayRectangle.Height);
        }

        private void button4_Click(object sender, EventArgs e)
        {
            AppendLog("Bắt đầu tạo dữ liệu test");
            string raw = @"{'AwsRegion':'ap-southeast-1','EventID':'6246ae91a08a5f8fa22a8b82','EventName':'INSERT','EventSource':'aws:dynamodb','RecordFormat':'application/json','TableName':'tcb-finance-eiv-data-sit-company','UserIdentity':null,'Dynamodb':{'ApproximateCreationDateTime':'1648539247112','Keys':{'customer_id':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':'21333875','NS':[],'NULL':false,'S':null,'SS':[]}},'NewImage':{'ENTRY_ID':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'624abccde0955dd3e891b524','SS':[]},'ENTRY_TYPE':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'0','SS':[]},'BUYER_CODE':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'Lila Cotton','SS':[]},'BRANCH_CODE':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'Sherman Griffin','SS':[]},'COMPANY_CODE':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'Melendez Mclaughlin','SS':[]},'BUYER_BANK_ACCOUNT':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'Brandy Floyd','SS':[]},'CURRENCY_CODE':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'EUR','SS':[]},'PAYMENT_METHOD_NAME':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'Walker Contreras','SS':[]},'INV_TYPE_CODE':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'Janet Mcfadden','SS':[]},'INV_NOTE':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'Marva Ross','SS':[]},'TRANS_NO':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'Jordan Mcdowell','SS':[]},'EXCHANGE_RATE':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':'75.888098','NS':[],'NULL':false,'S':null,'SS':[]},'ITEM_NAME':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'Morrow Zimmerman','SS':[]},'UNIT_NAME':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'Reva Barlow','SS':[]},'QUANTITY':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':'27.049526','NS':[],'NULL':false,'S':null,'SS':[]},'UNIT_PRICE':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':'16.710789','NS':[],'NULL':false,'S':null,'SS':[]},'VAT_CATEGORY_PERCENTAGE':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'Gibbs Hicks','SS':[]},'VAT_AMOUNT':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':'47.946023','NS':[],'NULL':false,'S':null,'SS':[]},'TOTAL_AMOUNT_WITHOUT_VAT':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':'-20.774538','NS':[],'NULL':false,'S':null,'SS':[]},'TOTAL_AMOUNT':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':'62.081369','NS':[],'NULL':false,'S':null,'SS':[]},'IS_SOURCE':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'0','SS':[]},'MODULE':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'Deann Meadows','SS':[]},'ACCOUNT_CO_CODE':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'Mendoza Key','SS':[]},'PRIORITY':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':'0','NS':[],'NULL':false,'S':null,'SS':[]},'PALCAT':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'Solomon Fuentes','SS':[]},'AMOUNT_LCY':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':'-27.91599','NS':[],'NULL':false,'S':null,'SS':[]},'PRODCAT':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':'0','NS':[],'NULL':false,'S':null,'SS':[]},'TRANSACTION_TYPE':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'Freida Hickman','SS':[]},'REVERT_FLAG':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'Pam Salas','SS':[]},'TRANSACTION_CODE':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'Magdalena Moon','SS':[]},'ORIGIN_TRANS_REF':{'B':null,'BOOL':false,'IsBOOLSet':false,'BS':[],'L':[],'IsLSet':false,'M':{},'IsMSet':false,'N':null,'NS':[],'NULL':false,'S':'Randi Mccall','SS':[]}},'OldImage':{},'SequenceNumber':null,'SizeBytes':175,'StreamViewType':null}}";
            int count = 0;
            while (count < 1000)
            {
                string rawV2 = raw.Replace("6246ae91a08a5f8fa22a8b82", Guid.NewGuid().ToString());
                MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes(rawV2));
                string partialKey = "shardId-000000000000";
                if ( count%4 == 1)
                {
                    partialKey = "shardId-000000000001";
                }
                else if (count % 4 == 2)
                {
                    partialKey = "shardId-000000000002";
                }
                else if (count % 4 == 3)
                {
                    partialKey = "shardId-000000000003";
                }
                string result = KinesisProvider.PutRecordRequest(kinesisClient, "misademo", stream, partialKey, "");
                //string result = KinesisProvider.PutRecordRequest(kinesisClient, "tcb-finance-eiv-data-sit-transaction_data", stream, partialKey, "");
                count++;
                if (count % 100 == 0)
                {
                    AppendLog($"Đã tạo được {count} dữ liệu ");
                }
            }


            AppendLog($"Done ");
        }

        private void button5_Click(object sender, EventArgs e)
        {
            AppendLog("Đang xóa stream Staging_Customer");
            DeleteStreamRequest deleteStreamReq = new DeleteStreamRequest();
            deleteStreamReq.StreamName = "Staging_CustomerV2";
            deleteStreamReq.EnforceConsumerDeletion = true;
            try
            {
                kinesisClient.DeleteStream(deleteStreamReq);
                AppendLog("Xóa stream Staging_Customer thành công");
            }
            catch (ResourceNotFoundException ex)
            {
                AppendLog("Không tìm thấy stream; " + ex);
            }
            catch (AmazonClientException ex)
            {
                AppendLog("Error deleting stream; " + ex);
            }
        }


        private void button2_Click(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog = new OpenFileDialog();
            openFileDialog.CheckFileExists = false;
            openFileDialog.AddExtension = true;
            openFileDialog.Multiselect = false;
            openFileDialog.Filter = "JSON files (*.json)|*.json";

            if (openFileDialog.ShowDialog() == System.Windows.Forms.DialogResult.OK)
            {
                string transData = File.ReadAllText(openFileDialog.FileName);
                AppendLog("Bắt đầu đẩy dữ liệu vào stream");
                List<TRANSACTION_DATA> transaction = JsonConvert.DeserializeObject<List<TRANSACTION_DATA>>(transData);
                //List<KinesisStreamRecord> transaction = JsonConvert.DeserializeObject<List<KinesisStreamRecord>>(transData, new MyDateTimeConverter());
                foreach (var item in transaction)
                {
                    string rawData = JsonConvert.SerializeObject(item);
                    string rawV2 = rawData.Replace("0001-01-01T00:00:00", "1648539247112");
                    
                    MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes(rawV2));
                    string result = KinesisProvider.PutRecordRequest(kinesisClient, "misakinesis", stream, "1", "");
                    //AppendLog(result);
                }
                AppendLog("Done");
            }
        }

        private void button6_Click(object sender, EventArgs e)
        {
            AppendLog("Đang khởi tạo stream Hóa đơn");
            string createStreamRequestRes = KinesisProvider.CreateStreamRequest(kinesisClient, "Staging_Trans_Data", 1);
            AppendLog($"Kết quả khởi tạo stream Hóa đơn: {createStreamRequestRes}");
        }

        private void button7_Click(object sender, EventArgs e)
        {
            AppendLog("Đang xóa stream Staging_Trans_Data");
            DeleteStreamRequest deleteStreamReq = new DeleteStreamRequest();
            deleteStreamReq.StreamName = "Staging_Trans_Data";
            deleteStreamReq.EnforceConsumerDeletion = true;
            try
            {
                kinesisClient.DeleteStream(deleteStreamReq);
                AppendLog("Xóa stream Staging_Trans_Data thành công");
            }
            catch (ResourceNotFoundException ex)
            {
                AppendLog("Không tìm thấy stream; " + ex);
            }
            catch (AmazonClientException ex)
            {
                AppendLog("Error deleting stream; " + ex);
            }
        }

        private void button8_Click(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog = new OpenFileDialog();
            openFileDialog.CheckFileExists = false;
            openFileDialog.AddExtension = true;
            openFileDialog.Multiselect = false;
            openFileDialog.Filter = "JSON files (*.json)|*.json";

            if (openFileDialog.ShowDialog() == System.Windows.Forms.DialogResult.OK)
            {
                customerDataText = File.ReadAllText(openFileDialog.FileName);
                AppendLog("Bắt đầu đẩy dữ liệu vào stream");
                List<CUSTOMER_BANK_ACCOUNT> customers = JsonConvert.DeserializeObject<List<CUSTOMER_BANK_ACCOUNT>>(customerDataText);
                //List<KinesisStreamRecord> customers = JsonConvert.DeserializeObject<List<KinesisStreamRecord>>(customerDataText, new MyDateTimeConverter());
                foreach (var item in customers)
                {
                    string rawData = JsonConvert.SerializeObject(item);
                    string rawV2 = rawData.Replace("0001-01-01T00:00:00", "1648539247112");
                    MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes(rawV2));
                    string result = KinesisProvider.PutRecordRequest(kinesisClient, "tcb-finance-eiv-data-sit-customer_bank_account", stream, "1", "");
                    //AppendLog(result);
                }
                AppendLog("Done");
            }
        }

        private void button9_Click(object sender, EventArgs e)
        {
           
        }

        private void button10_Click(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog = new OpenFileDialog();
            openFileDialog.CheckFileExists = false;
            openFileDialog.AddExtension = true;
            openFileDialog.Multiselect = false;
            openFileDialog.Filter = "JSON files (*.json)|*.json";

            if (openFileDialog.ShowDialog() == System.Windows.Forms.DialogResult.OK)
            {
                customerDataText = File.ReadAllText(openFileDialog.FileName);
                AppendLog("Bắt đầu đẩy dữ liệu vào stream");
                List<COMPANY> customers = JsonConvert.DeserializeObject<List<COMPANY>>(customerDataText);
                //List<Amazon.DynamoDBv2.Model.Record> customers = JsonConvert.DeserializeObject<List<Amazon.DynamoDBv2.Model.Record>>(customerDataText, new MyDateTimeConverter());
                foreach (var item in customers)
                {
                    string rawData = JsonConvert.SerializeObject(item);
                    string rawV2 = rawData.Replace("0001-01-01T00:00:00", "1648539247112");
                    MemoryStream stream = new MemoryStream(Encoding.UTF8.GetBytes(rawV2));
                    string result = KinesisProvider.PutRecordRequest(kinesisClient, "tcb-finance-eiv-data-sit-company", stream, "1", "");
                    //AppendLog(result);
                }
                AppendLog("Done");
            }
        }
    }
}
