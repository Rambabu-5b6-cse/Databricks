# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](./Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Ingestion with LakeFlow Connect
# MAGIC  
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This course provides a comprehensive introduction to Lakeflow Connect, a scalable and simplified solution for ingesting data into Databricks from a wide range of sources. You’ll begin by exploring the different types of Lakeflow Connect connectors (Standard and Managed) and learn various data ingestion techniques, including batch, incremental batch, and streaming ingestion. You'll also review the key benefits of using Delta table and the Medallion architecture
# MAGIC
# MAGIC Next, you’ll develop practical skills for ingesting data from cloud object storage using Lakeflow Connect Standard Connectors. This includes working with methods such as CREATE TABLE AS SELECT (CTAS), COPY INTO, and Auto Loader, with an emphasis on the benefits and considerations of each approach. You’ll also learn how to append metadata columns to your bronze-level tables during ingestion into the Databricks Data Intelligence Platform. The course then covers how to handle records that don’t match your table schema using the rescued data column, along with strategies for managing and analyzing this data. You’ll also explore techniques for ingesting and flattening semi-structured JSON data.
# MAGIC
# MAGIC Following this, you’ll explore how to perform enterprise-grade data ingestion using Lakeflow Connect Managed Connectors to bring in data from databases and Software-as-a-Service (SaaS) applications. The course also introduces Partner Connect as an option for integrating partner tools into your ingestion workloads.
# MAGIC
# MAGIC Finally, the course wraps up with alternative ingestion strategies, including MERGE INTO operations and leveraging the Databricks Marketplace, equipping you with a strong foundation to support modern data engineering use cases.
# MAGIC
# MAGIC ## Terminal Objectives
# MAGIC - Describe Lakeflow Connect as a scalable and simplified solution for data ingestion into Databricks from a variety of sources.  
# MAGIC - Review the benefits of Delta tables and the Medallion architecture.  
# MAGIC - Demonstrate how to ingest data from cloud object storage into Delta tables using CREATE TABLE AS, COPY INTO, and Auto Loader, including capturing input file metadata in Bronze layer tables.  
# MAGIC - Explain how rescued columns are used during ingestion to manage malformed records.  
# MAGIC - Illustrate techniques for ingesting and flattening semi structured JSON data from cloud storage.  
# MAGIC - Describe available options for ingesting data from enterprise systems using Lakeflow Connect Managed Connectors.  
# MAGIC - Discuss alternative ingestion methods such as MERGE INTO and Databricks Marketplace.
# MAGIC
# MAGIC
# MAGIC ##### Course update and version can be found in the `Version Info` file.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Prerequisites
# MAGIC
# MAGIC Before starting this course, learners should be comfortable with the following:
# MAGIC
# MAGIC - Basic understanding of the Databricks Data Intelligence platform, including Databricks Workspaces, Apache Spark, Delta Lake, the Medallion Architecture and Unity Catalog.
# MAGIC - Basic understanding of data ingestion workflows (batch, streaming, incremental) and general ETL principles
# MAGIC - Experience working with various file formats (e.g., Parquet, CSV, JSON, TXT).
# MAGIC - Proficiency in SQL and Python.
# MAGIC - Familiarity with running code in Databricks notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Workspace Setup Information
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Databricks Provided Vocareum Workspace (Recommended)
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC - If you are running this notebook in a <strong>Databricks Academy provided Vocareum workspace</strong>, your Unity Catalog catalog is already created for you.
# MAGIC
# MAGIC - Your catalog name matches your Vocareum username and looks like: <strong>labuser12345</strong> (series of unique numbers)
# MAGIC
# MAGIC - If a <strong>Marketplace</strong> dataset is required, the share is already installed and available in the workspace.
# MAGIC
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/>
# MAGIC <a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> |
# MAGIC <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> |
# MAGIC <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>