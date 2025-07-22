# **June.so to RudderStack/BigQuery Migration Guide**

**Context:** The June.so founding team has been acquired by Amplitude, and the June.so product [will be shutting down on **August 8th, 2025**](https://www.june.so/blog/a-new-chapter). This gives you a limited time to export your valuable product analytics data before it's gone.

This guide will walk you through using our simple migration tool to safely export all your historical data from June.so and import it into your own Google BigQuery database. The tool automatically formats your data to be 100% compatible with the RudderStack warehouse schema, giving you a perfect foundation to continue your product analytics journey without missing a beat.

### **Why RudderStack and BigQuery?**

Moving from a closed analytics platform like June.so is an opportunity to adopt a more flexible and powerful setup for the future. Here’s why using this tool to migrate to a RudderStack and BigQuery stack is a smart choice, especially for growth-stage companies:

- **You Own Your Data**: Unlike with many analytics tools where your data is stored in their system, this process moves your data into your own Google BigQuery warehouse. It becomes your asset. You have complete control and can connect it to any tool you want in the future—BI platforms, other analytics tools, or even AI models—without ever being locked in again.
- **It's Extremely Cost-Effective**: For early-growth companies, cost is critical. Google BigQuery has a very generous free tier and low-cost storage, meaning you can store years of historical data for just a few dollars a month. RudderStack also offers a free tier for event collection, making this combination one of the most affordable and scalable analytics stacks on the market. You get the power of an enterprise-grade data warehouse without the enterprise price tag.
- **Connections and Unlimited Possibilities:** You can connect your product usage data from RudderStack/BigQuery data warehouse to other product analytics tools, such as Posthog. Moreover, you can make your own B2B SaaS dashboards over the BigQuery data using tools like [Looker Studio](https://lookerstudio.google.com) or [Apache SuperSet](https://superset.apache.org/).

### **How It Works**

This tool is designed with simplicity and security as top priorities. Here’s what happens when you use it:

- **Everything Runs Locally**: The entire application (the server and the web page) runs on your own computer. Your data is processed locally and is never sent to any third-party servers.
- **Direct and Secure Upload**: Your CSV files are read by the tool on your machine. When you click "Upload," the tool makes a direct, secure connection to your own Google BigQuery project using the credentials you provide.
- **RudderStack Compatible**: The tool automatically reads your June.so data, and restructures it to match the RudderStack schema. It also creates the necessary tables in BigQuery for you.
- **Instant Tracking Plan Generation**: As soon as you select a valid set of all three CSV files, the tool instantly generates a complete tracking plan on the right side of the screen. This gives you immediate insight into all the events and properties you've been tracking, without having to upload anything first.
- **Generate Post-Migration SQL**: You can choose the "Dry Run" option to process your files locally and generate key outputs without uploading any data. This allows you to the SQL commands needed for the **post-migration step** of merging your historical data with a live production system.

In short, your data goes from a file on your computer directly to your private BigQuery database. It doesn't go anywhere else.

### **Part 1: Exporting Your Data from June.so**

First, you need to get your data out of June.so. They have provided a CSV export feature for this purpose. The original June.so documentation is available [here](https://help.june.so/en/articles/11696025-how-to-export-your-data).

1. Go to the June Migration Page: Open your web browser and navigate to the special migration URL provided by June:  
   [https://analytics.june.so/a/go-to-my-workspace/migrate](https://analytics.june.so/a/go-to-my-workspace/migrate)
2. **Stop Sending New Data**: To ensure your export is complete and accurate, you should stop sending any new tracking data to June from your application. In the migration page, you will see a checklist to confirm you have stopped sending track, page, identify, and group calls. Mark each one as "done".
3. **Select the CSV Export Option**: Scroll down to the "Select a data export option" section. You will see two choices: "Amplitude" and "CSV". **Click on "CSV"**.
4. **Start the Export**: Click the green "Export data" button. The process may take anywhere from a few minutes to a few hours, depending on the amount of data you have.
5. **Receive Your Data via Email**: Once the export is complete, you will receive an email from system@june.so. This email will contain download links for three files:
   - Events
   - Identifies
   - Groups
6. **Download and Unzip the Files**: Click the links to download the files. They will likely be in a compressed .zst format.
   - **On Windows**: You can use a free tool like [7-Zip](https://www.7-zip.org/) to extract them. Right-click the file and choose "Extract Here".
   - **On macOS**: You may need to install a tool via the command line to unzip the files.
   - Once unzipped, you will have three CSV files: events.csv, identifies.csv, and groups.csv. Keep these handy for the next part.

### **Part 2: Setting Up Your New "Data Home" in Google BigQuery**

This is a one-time setup to create a secure, private database in Google Cloud where your June.so data will be stored.

1. **Create a Google Cloud Project**: If you don't already have one, create a free Google Cloud project. This is like your main account folder.
   - **Official Guide**: [Creating and managing projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
2. **Enable the BigQuery API**: You need to "turn on" the BigQuery service for your project.
   - **Instructions**: Go to the [BigQuery API page](https://console.cloud.google.com/apis/library/bigquery.googleapis.com) and click the **Enable** button.
3. **Create a BigQuery Dataset**: A Dataset is like a folder inside your project where your data tables will live.
   - **Official Guide**: [Create datasets](https://www.google.com/search?q=https://cloud.google.com/bigquery/docs/datasets%23create-dataset)
   - When asked for a **Dataset ID**, we recommend using juneso_migration_data.
4. **Create a Service Account**: This is a special, non-human user that our tool will use to securely upload data on your behalf.
   - **Official Guide**: [Create service accounts](https://www.google.com/search?q=https://cloud.google.com/iam/docs/creating-managing-service-accounts%23creating)
   - Give it a memorable name, like _june-migration-tool._
5. **Grant Permissions**: Give your new Service Account the necessary permissions to work with BigQuery.
   - **Official Guide**: [Granting roles](https://www.google.com/search?q=https://cloud.google.com/iam/docs/granting-changing-revoking-access%23grant-single-role)
   - Assign the following two roles to the service account you just created:
     1. BigQuery Data Editor
     2. BigQuery Job User
6. **Download Your JSON Key**: This is the password for the Service Account. Keep this file safe.
   - **Official Guide**: [Creating service account keys](https://www.google.com/search?q=https://cloud.google.com/iam/docs/creating-managing-service-account-keys%23creating_service_account_keys)
   - Choose **JSON** as the key type.
   - A file will be downloaded. **Rename this file to credentials.json**.

### **Part 3: Using the Migration Tool**

Now you're ready to use the tool to move your data.

1. **Download the Migration Tool**: Download the project files from the link below and unzip them into a folder on your computer.
   - **Download Link (GitHub)**: [v1.0](https://github.com/ArvoanDev/juneso2rudderstack/releases/tag/1.0)
2. **Install Node.js**: If you don't have it installed, download and install the **LTS** version from the [official Node.js website](https://nodejs.org/en/download/).
3. **Set Up the Tool**:
   - Move the credentials.json file (from Part 2, Step 6\) into the tool's folder.
   - Open the .env file in a text editor.
   - Replace your-gcp-project-id with your actual Google Cloud Project ID.
   - Replace your_bigquery_dataset_id with the Dataset ID you created (e.g., juneso_migration_data).
   - Open a command prompt or terminal, navigate into the tool's folder, and run the command: npm install
4. **Run the Migration**:
   - In the same command prompt/terminal, run the command: node server.js
   - Open your web browser (e.g., Chrome) and go to: http://localhost:3000
   - You will see the uploader interface. Select your identifies.csv, groups.csv, and events.csv files.
   - Once all three valid files are selected, the "Upload to BigQuery" button will become active. Click it.
   - The tool will now upload and format your data. This may take a few minutes.

### **Part 4: Next Steps \- Merging June.so Data with Production Environment**

After the upload is successful, the tool will display a set of SQL queries. These can be used to combine your historical June.so data with the new data you'll be collecting in your live RudderStack setup.

1. **Set Up a Live RudderStack Pipeline**: In RudderStack, create a new BigQuery destination. This will be your _new_ live dataset (e.g., rudderstack_prod_data).
2. **Use the Generated SQL**:  
   **Important Note**: The SQL queries generated by the tool are powerful starting points. Because your live RudderStack environment may track different or additional properties than your old June.so setup, the table schemas might not match perfectly. The tool generates queries based only on the columns found in your June.so data. You may need to manually adjust the SELECT statements to add or remove columns to match your live production tables perfectly.
   - Copy the queries from the **Views (Recommended)** tab in the tool.
   - In the BigQuery SQL workspace, paste the queries.
   - **Important**: Replace the placeholder your_live_rudderstack_dataset with the name of your new live dataset (e.g., rudderstack_prod_data).
   - Run the queries.

This creates a safe, virtual view of your data. Now, when you connect analytics tools to BigQuery, you can point them to the "views" dataset (e.g., analytics_views) to analyze both your historical June.so data and your new RudderStack data together as if they were in a single table.
