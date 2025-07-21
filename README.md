# **June.so to BigQuery Migration Tool (RudderStack-Compatible)**

With **June.so shutting down**, you need a reliable way to preserve your valuable product analytics data. This tool is designed specifically for that purpose.  
It provides a simple web page to upload your standard June.so CSV exports (identifies, groups, and events) and migrate them directly into a Google BigQuery database. The tool automatically formats the data and creates tables that match the official **RudderStack warehouse schema**. This allows you to seamlessly switch your analytics to RudderStack or any other tool that uses BigQuery, without losing your historical data.

## **How It Works (The "Magic")**

Even though it seems simple on the surface, the tool performs several complex tasks automatically to ensure a smooth migration from June.so.

1. **You Upload Your June.so CSV Exports**: On the web page, you select the three standard CSV files provided by the June.so export feature.
2. **The Server Gets to Work**: When you click "Upload," the files are securely sent to a small server program running locally on your computer. This server is the "brains" of the operation.
3. **Smart Data Sorting**: The server reads each file and understands the specific structure of June.so's data. It correctly interprets that type 0 in your events file is a "page view" and type 2 is a custom "track" event.
4. **Automatic Table Creation**: The server connects to your Google BigQuery project.
   - It automatically creates the standard RudderStack tables: identifies, users, \_groups, pages, and tracks.
   - For every unique event it finds (like open_modal), it creates a dedicated table for it.
   - If a table already exists, it intelligently adds any new data columns it discovers without losing existing data.
5. **Data Formatting (The Most Important Step)**: The server automatically cleans and formats your June.so data to be 100% compatible with the RudderStack schema. It takes complex, nested JSON data (like the context or properties columns) and "flattens" it into individual, query-friendly columns. For example, a context object containing {"app": {"installType": "cdn"}} becomes a BigQuery column named context_app_install_type.
6. **Loading the Data**: Finally, the server securely loads all the formatted data into the correct tables in your BigQuery database, completing the migration.

## **Step-by-Step Instructions**

Follow these instructions carefully to set up and use the tool. The setup is a one-time process.

### **Part 1: Setting Up Google BigQuery (One-Time Setup)**

Before using the tool, you need to prepare your Google BigQuery account.

#### **Step 1: Create a Google Cloud Project**

A "Project" is the main container for all your cloud resources. If you don't have one, you'll need to create one.

- **Instructions**: Follow the official guide: [Creating and managing projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects).

#### **Step 2: Enable the BigQuery API**

You need to "turn on" the BigQuery service for your project so that our tool can communicate with it.

- **Instructions**: Go to the [BigQuery API page](https://console.cloud.google.com/apis/library/bigquery.googleapis.com) and click the **Enable** button.

#### **Step 3: Create a BigQuery Dataset**

A "Dataset" is like a folder or a database inside your project where your tables will live.

- **Instructions**: Follow the official guide: [Create datasets](https://www.google.com/search?q=https://cloud.google.com/bigquery/docs/datasets%23create-dataset).
- For the **Dataset ID**, we recommend using rudderstack_data. Remember the name you choose.

#### **Step 4: Create a Service Account**

A "Service Account" is a special, non-human user that our tool will use to log in and upload data on your behalf.

- **Instructions**: Follow the official guide: [Create service accounts](https://www.google.com/search?q=https://cloud.google.com/iam/docs/creating-managing-service-accounts%23creating).
- Give the service account a descriptive name, like june-migration-tool.

#### **Step 5: Grant Permissions to the Service Account**

You need to give your new Service Account permission to work with BigQuery.

- **Instructions**: Follow the guide on [granting roles](https://www.google.com/search?q=https://cloud.google.com/iam/docs/granting-changing-revoking-access%23grant-single-role).
- Find the Service Account you just created and grant it the following two roles:
  1. BigQuery Data Editor
  2. BigQuery Job User

#### **Step 6: Download the JSON Key**

This key is the password for your Service Account. It's a file that you must keep safe.

- **Instructions**: Follow the guide on [creating service account keys](https://www.google.com/search?q=https://cloud.google.com/iam/docs/creating-managing-service-account-keys%23creating_service_account_keys).
- When you create the key, make sure to select **JSON** as the key type.
- A file will be downloaded to your computer. **Rename this file to credentials.json**.

### **Part 2: Setting Up the Uploader Tool (One-Time Setup)**

Now, let's set up the uploader tool on your computer.

#### **Step 1: Install Node.js**

Node.js is the underlying technology that runs the server. If you don't have it, you'll need to install it.

- **Instructions**: Go to the [official Node.js website](https://nodejs.org/en/download/) and download the **LTS** version for your operating system (Windows, Mac, etc.). Run the installer.

#### **Step 2: Download and Place the Project Files**

Download the project folder (e.g., bq-uploader) which contains the files: server.js, index.html, schema.js, package.json, and .env.

#### **Step 3: Place Your Credentials File**

Move the credentials.json file (which you downloaded and renamed in Part 1, Step 6\) into the bq-uploader project folder.

#### **Step 4: Configure the Tool**

Open the .env file in a simple text editor. You will see two lines:

```plaintext
BIGQUERY\_PROJECT\_ID="your-gcp-project-id"
BIGQUERY\_DATASET\_ID="your\_bigquery\_dataset\_id"
```

- Replace "your-gcp-project-id" with the **Project ID** from your Google Cloud project.
- Replace "your_bigquery_dataset_id" with the **Dataset ID** you created in Part 1, Step 3 (e.g., rudderstack_data).
- Save and close the file.

#### **Step 5: Install Dependencies**

This step downloads the necessary code libraries for the server to run.

- Open your computer's command prompt or terminal.
- Navigate into the bq-uploader folder.
- Type the following command and press Enter:  
  npm install

- Wait for the process to complete. You should see a new folder called node_modules appear.

### **Part 3: Running the Uploader**

Once the setup is complete, you can run the tool anytime you need to upload data.

#### **Step 1: Start the Server**

- In your command prompt or terminal, while inside the bq-uploader folder, run the following command:  
  node server.js

- You should see the message: Server running at http://localhost:3000. Leave this terminal window open.

#### **Step 2: Open the Web Page**

- Open your web browser (like Chrome or Firefox) and navigate to the following address:  
  http://localhost:3000

#### **Step 3: Upload Your Files**

- You will see a simple form.
- Click the "Choose File" button for each category (Identifies, Groups, Events) and select the corresponding CSV file from your computer.
- Once all three files are selected, click the **🚀 Upload to BigQuery** button.

The tool will now process and upload your data. You will see a status message on the page when it's complete. You can then go to your BigQuery console to see your newly populated tables\!

### **Part 4: Merging Migrated Data with a Live RudderStack Database**

After migrating your historical June.so data, you will likely set up a live RudderStack pipeline that sends new data to a different BigQuery dataset. This leaves you with two separate datasets: one with your old data and one with your new data.  
The best practice for combining them for analysis is to create **VIEWs**. A view is a virtual, unified table that queries both your historical and live data in real-time without duplicating storage.

#### **Step 1: Identify Your Datasets**

You will have two datasets in your BigQuery project:

- **Migration Dataset**: The one you created for this tool (e.g., rudderstack_data).
- **Live RudderStack Dataset**: The one created by your live RudderStack pipeline (e.g., my_app_prod).

#### **Step 2: Create a New Dataset for Views**

It's a good practice to keep these combined views in a separate, dedicated dataset.

- Create a new dataset in BigQuery named something like analytics_views.

#### **Step 3: Create Combined Views Using SQL**

In the BigQuery SQL workspace, run the following queries. **Remember to replace the placeholder project and dataset names with your own.**  
**Example for the tracks table:**

```sql
CREATE OR REPLACE VIEW \`your-project-id.analytics\_views.tracks\` AS
SELECT \* FROM \`your-project-id.my\_app\_prod.tracks\`
UNION ALL
SELECT \* FROM \`your-project-id.rudderstack\_data.tracks\`;
```

**Example for the users table:**

```sql
CREATE OR REPLACE VIEW \`your-project-id.analytics\_views.users\` AS
SELECT \* FROM \`your-project-id.my\_app\_prod.users\`
UNION ALL
SELECT \* FROM \`your-project-id.rudderstack\_data.users\`;
```

**Example for a specific event table like open_modal:**

```sql
CREATE OR REPLACE VIEW \`your-project-id.analytics\_views.open\_modal\` AS
SELECT \* FROM \`your-project-id.my\_app\_prod.open\_modal\`
UNION ALL
SELECT \* FROM \`your-project-id.rudderstack\_data.open\_modal\`;
```

**Repeat this CREATE OR REPLACE VIEW pattern for all tables you wish to combine**, such as identifies, pages, and \_groups.

#### **Step 4: Use the Views for Analysis**

Now, for all your analysis, dashboards (like Looker Studio), and queries, you can use the unified views in your analytics_views dataset. They will always contain the complete, combined history of your June.so and live RudderStack data.
