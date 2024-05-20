# Market Challenge for "5GFlix" 🎬📊

## Introduction

I participated in an interesting challenge proposed by "5GFlix," a new streaming app that aims to define its business strategy through market analysis. The objective was to analyze movies and series available on two competing platforms, Amazon and Netflix, using data provided by them. The CTO of "5GFlix," Alan Turing, requested Solvimm to create a logical structure that would enable the BI team to answer various business questions related to this data.

## 🎯 Objectives

To perform these analyses, two datasets were provided:

- **Netflix:** [Netflix Prize Data](https://www.kaggle.com/netflix-inc/netflix-prize-data) 🎥
- **Amazon:** [Amazon Customer Reviews Dataset](https://www.kaggle.com/datasets/cynthiarempel/amazon-us-customer-reviews-dataset) 🛒
  - **Subsets Considered:**
    - Video_v1_00
    - Video_DVD_v1_00
    - Digital_Video_Download_v1_00
      
## 🛠️ Implemented Solution

To solve the challenge, I used the following technical approach:

- **PySpark:** Used for extracting, cleaning, and loading the massive amounts of review data from Amazon and Netflix. This choice was crucial for efficiently handling large volumes of data. 🐍⚡
- **Amazon S3 Bucket:** Implemented for the staging phase, providing a scalable and durable storage solution for raw data. ☁️🗄️
- **Snowflake:** Used for implementing the staging phase, creating tables in the Data Warehouse, and transforming and loading data from staging to the Data Warehouse tables. ❄️🏛️

### Data Pipeline soluction
![image](https://github.com/lucasvittal2/5Gflix-case/assets/62555057/fdd99ab0-4933-4824-9a79-7d122331c17d)


### Datawarehouse Projeected
![image](https://github.com/lucasvittal2/5Gflix-case/assets/62555057/ee772aba-c6a7-4351-a477-74572594524a)


## ✅ Result

This solution allowed the data to be processed and organized efficiently, ensuring that the BI team at "5GFlix" could perform detailed analyses based on accurate data to formulate their market strategies.

A little demonstration of final outcome:

![image](https://github.com/lucasvittal2/5Gflix-case/assets/62555057/b1fdc810-b9f6-44c4-91d7-3e00c5cc3601)

Now 5GFLIX will be able to perform BI consulting to their business 👨‍💼👩‍💼📈📊


## 🚀 How to Set Up and Run the 5GFlix Market Analysis Project

Follow these steps to clone, set up, and run the 5GFlix market analysis project from the cloned GitHub repository.

### Step-by-Step Instructions 📋

1. **Clone the Project** 🌀

   ```bash
   $> git clone <REPO_URL>
   ```

2. **Navigate to the Project Folder** 📁

   ```bash
   $> cd <FOLDER_YOU_CLONED>/5Gflix-case
   ```

3. **Run Project Setup Script** 🛠️

   ```bash
   $> bash project_setup.sh
   ```

4. **Activate the Python Environment** 🐍

   ```bash
   $> source fivegflix-env/bin/activate
   ```
5.  ⚠️ Refers to aws and snowflake documentation get users credentials ⚠️ :
   - [Snowflake documentation] (https://docs.snowflake.com/)
   - [AWS Documentation] (https://docs.aws.amazon.com/)
     
6. 🔍 Once you found how to get credentials on each plataform replace rename .env.copy to .env and put your credentials inside there:
   
```
              #SNOWFLAKE CREDENTIALS
              
              export USER_NAME="<USER_NAME>"
              export USER_PASSWORD="USER_PASSWORD>"
              export USER_ACCOUNT="<USER_ACCOUNT>"
              export USER_URL="<USER_URL>"
              export USER_ROLE="<USER_ROLE>"
              export DATABASE_NAME="<DATABASE_NAME>"
              export SCHEMA_NAME="<SCHEMA_NAME>"
              
              #AWS S3 CREDENTIALS
              
              export AWS_REGION="<AWS_REGION>"
              export AWS_SECRET_KEY="<AWS_SECRET_KEY>"
              export AWS_ACCESS_KEY="<AWS_ACCESS_KEY>"
              export AWS_BUCKET_NAME="<AWS_BUCKET_NAME>"
```
5. **Set Up Environment Variables** 🌍

   ```bash
   $> source .env
   ```

6. **Download and Extract Data** 📦
   
   Go to the links below, download the data, and:
   - Extract Netflix data to `<FOLDER_YOU_CLONED>/5Gflix-case/assets/extracted-data/netflix`
   - Extract Amazon data to `<FOLDER_YOU_CLONED>/5Gflix-case/assets/extracted-data/amazon>`

7. **Ready to Test and Improve** 🚀

   Now you are ready to test and even improve my solution. 😎✊✨



