# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ![DBAcademy](./Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lecture - Data Engineering in Databricks
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This lecture introduces the fundamentals of data engineering in Databricks, focusing on how **LakeFlow Connect** simplifies and unifies data ingestion from diverse sources into the **Databricks Data Intelligence Platform**. You will learn about the **different ingestion methods** available through LakeFlow Connect, including batch, incremental batch, and streaming. The lecture also provides a review of **Delta Lake**, covering Delta table components, key features, and the Medallion Architecture for progressively refining data quality.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC
# MAGIC 1. **Describe the purpose and benefits of LakeFlow Connect** for scalable data ingestion into Databricks
# MAGIC 2. **Identify the different types of connectors**, including Standard and Managed connectors
# MAGIC 3. **Explain various data ingestion techniques** such as batch, incremental batch, and streaming
# MAGIC 4. **Select the appropriate ingestion method** based on data and use case requirements
# MAGIC 5. **Review the key benefits of Delta tables** and the Medallion Architecture for data management and analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Introduction to Data Engineering in Databricks

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. Data Engineering Platform Overview
# MAGIC
# MAGIC Databricks offers LakeFlow, an end-to-end data engineering solution for delivering high-quality data for downstream analytics, AI, and operational applications.
# MAGIC
# MAGIC <div style="max-width:1000px;margin:0 auto;font-family:sans-serif;color:#0b2026;line-height:1.2;"><div style="display:flex;justify-content:center;position:relative;z-index:2;margin-bottom:-1px;"><div style="border:1px solid #FFD3CB;border-bottom:none;background:#fff;border-radius:10px 10px 0 0;padding:6px 22px;"><span style="font-size:18pt;font-weight:800;letter-spacing:-0.5px;">databricks</span> <span style="font-size:18pt;font-weight:800;color:#FF5F46;margin-left:6px;">LAKEFLOW</span></div></div><div style="border:1px solid #FFD3CB;border-radius:10px;padding:14px;"><div style="text-align:center;color:#FF5F46;font-weight:800;font-size:14pt;letter-spacing:0.5px;margin-bottom:12px;">UNIFIED DATA ENGINEERING FOR THE DATA INTELLIGENCE PLATFORM</div><div style="display:flex;gap:12px;margin-bottom:12px;flex-wrap:wrap;"><div style="flex:1;min-width:200px;border:1px solid #EEEDE9;border-radius:6px;overflow:hidden;box-shadow:0 1px 4px rgba(27,49,57,0.06);"><div style="background:#FF5F46;color:#fff;font-weight:800;font-size:14pt;text-align:center;letter-spacing:0.5px;min-height:56px;display:flex;align-items:center;justify-content:center;padding:8px 10px;">CONNECT</div><div style="padding:11px 14px;text-align:center;font-weight:700;font-size:14pt;">Efficient ingestion connectors</div></div><div style="flex:1;min-width:200px;border:1px solid #EEEDE9;border-radius:6px;overflow:hidden;box-shadow:0 1px 4px rgba(27,49,57,0.06);"><div style="background:#FF5F46;color:#fff;font-weight:800;font-size:14pt;text-align:center;letter-spacing:0.5px;min-height:56px;display:flex;align-items:center;justify-content:center;padding:8px 10px;">APACHE SPARK DECLARATIVE PIPELINES (SDP)</div><div style="padding:11px 14px;text-align:center;font-weight:700;font-size:14pt;">Accelerated ETL development</div></div><div style="flex:1;min-width:200px;border:1px solid #EEEDE9;border-radius:6px;overflow:hidden;box-shadow:0 1px 4px rgba(27,49,57,0.06);"><div style="background:#FF5F46;color:#fff;font-weight:800;font-size:14pt;text-align:center;letter-spacing:0.5px;min-height:56px;display:flex;align-items:center;justify-content:center;padding:8px 10px;">JOBS</div><div style="padding:11px 14px;text-align:center;font-weight:700;font-size:14pt;">Reliable orchestration for analytics and AI</div></div></div><div style="background:#FF5F46;color:#fff;border-radius:6px;text-align:center;padding:13px 14px;"><div style="font-weight:800;font-size:16pt;letter-spacing:0.5px;">INDUSTRY LEADING DATA PROCESSING ENGINE</div><div style="font-weight:700;font-size:14pt;margin-top:3px;">(Apache Spark + Structured Streaming)</div></div></div><div style="display:flex;margin-top:8px;border:1px solid #EEEDE9;border-radius:6px;overflow:hidden;"><div style="background:#618794;color:#fff;font-weight:800;font-size:14pt;letter-spacing:0.5px;padding:10px 14px;width:240px;display:flex;align-items:center;">UNIFIED GOVERNANCE</div><div style="flex:1;background:#fff;display:flex;align-items:center;justify-content:center;font-weight:800;font-size:14pt;padding:8px;">Unity Catalog</div></div><div style="display:flex;margin-top:6px;border:1px solid #EEEDE9;border-radius:6px;overflow:hidden;"><div style="background:#618794;color:#fff;font-weight:800;font-size:14pt;letter-spacing:0.5px;padding:10px 14px;width:240px;display:flex;align-items:center;">OPTIMIZED STORAGE</div><div style="flex:1;background:#fff;display:flex;align-items:center;justify-content:center;gap:24px;font-weight:800;font-size:14pt;padding:8px;flex-wrap:wrap;"><span>Delta Lake</span><span style="color:#DCE0E2;font-weight:400;">|</span><span>Parquet</span><span style="color:#DCE0E2;font-weight:400;">|</span><span>Iceberg</span></div></div></div>
# MAGIC
# MAGIC This course focuses on data ingestion with LakeFlow Connect into the Databricks Data Intelligence Platform.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC LakeFlow provides a unified platform for data ingestion, transformation, and orchestration:
# MAGIC
# MAGIC - **LakeFlow Connect** — Efficient ingestion connectors for enterprise applications, databases, cloud storage, message buses, and local files
# MAGIC - **Apache Spark™ Declarative Pipelines** — A framework for building batch and streaming data pipelines using SQL and Python
# MAGIC - **LakeFlow Jobs** — Workflow automation that orchestrates data processing workloads and coordinates multiple tasks within complex workflows
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. What Is LakeFlow Connect?
# MAGIC
# MAGIC LakeFlow Connect streamlines data ingestion with simple, efficient connectors that enable you to bring in data from files, cloud storage, databases, enterprise applications, and streaming sources directly into the Databricks Lakehouse — all within a unified, managed platform.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Traditional Data Ingestion Challenges
# MAGIC
# MAGIC Traditionally, organizations resort to a patchwork of solutions for data ingestion when working with enterprise systems, cloud storage, and streaming.
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="./Includes/images/lecture_lakeflow_connect/traditional-data-ingestion-challenges.png"
# MAGIC     alt="Traditional Data Ingestion Challenges"
# MAGIC     style="width: 900px; max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC - Multiple data sources each require their own set of tools and solutions.
# MAGIC - On-premises files, legacy systems, databases, cloud storage, SaaS applications, and real-time event streams all use different connectors.
# MAGIC - A mix of third-party tools and in-house solutions creates complexity.
# MAGIC - The result is increased operational overhead, inconsistent governance, and fragmented observability.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. LakeFlow Connect: Unified Ingestion
# MAGIC
# MAGIC With LakeFlow Connect, you can build efficient ingestion pipelines entirely within Databricks. It provides simple setup and maintenance, along with unified orchestration, observability, and governance — all within the Databricks Data Intelligence Platform.
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="./Includes/images/lecture_lakeflow_connect/lakeflow-connect-unified-ingestion.png"
# MAGIC     alt="LakeFlow Connect Unified Ingestion"
# MAGIC     style="width: 900px; max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B3. Key Benefits of LakeFlow Connect
# MAGIC
# MAGIC LakeFlow Connect provides built-in connectors for the Databricks Data Intelligence Platform to streamline data ingestion.
# MAGIC
# MAGIC <div style="text-align: center; margin: 32px 0 8px 0;">
# MAGIC   <div style="display: inline-block; background: #0b2026; color: #F9F7F4;
# MAGIC               font-size: 17px; font-weight: bold; letter-spacing: 0.5px;
# MAGIC               padding: 12px 40px; border-radius: 8px;">
# MAGIC     Key Benefits
# MAGIC   </div>
# MAGIC   <div style="width: 2px; height: 32px; background: #618794; margin: 0 auto;"></div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="display: flex; gap: 16px; margin: 0 0 32px 0;">
# MAGIC
# MAGIC   <div style="flex: 1; background: #F9F7F4; border-top: 4px solid #4299E0;
# MAGIC               border-radius: 8px; padding: 22px 20px; text-align: center;
# MAGIC               box-shadow: 0 2px 8px rgba(11,32,38,0.07);">
# MAGIC     <div style="font-weight: bold; color: #0b2026; font-size: 16pt; margin-bottom: 8px;">
# MAGIC       Managed and Efficient Solution
# MAGIC     </div>
# MAGIC     <div style="font-size: 14pt; color: #618794; line-height: 1.7;">
# MAGIC       Lower costs and quicker time to value
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex: 1; background: #F9F7F4; border-top: 4px solid #00A972;
# MAGIC               border-radius: 8px; padding: 22px 20px; text-align: center;
# MAGIC               box-shadow: 0 2px 8px rgba(11,32,38,0.07);">
# MAGIC     <div style="font-weight: bold; color: #0b2026; font-size: 16pt; margin-bottom: 8px;">
# MAGIC       Self-Serve Interfaces for Every Practitioner
# MAGIC     </div>
# MAGIC     <div style="font-size: 14pt; color: #618794; line-height: 1.7;">
# MAGIC       Democratized data with an accelerated rate of innovation
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex: 1; background: #F9F7F4; border-top: 4px solid #FF5F46;
# MAGIC               border-radius: 8px; padding: 22px 20px; text-align: center;
# MAGIC               box-shadow: 0 2px 8px rgba(11,32,38,0.07);">
# MAGIC     <div style="font-weight: bold; color: #0b2026; font-size: 16pt; margin-bottom: 8px;">
# MAGIC       Unified Observability and Governance
# MAGIC     </div>
# MAGIC     <div style="font-size: 14pt; color: #618794; line-height: 1.7;">
# MAGIC       Secured and healthy pipelines and tables
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B4. LakeFlow Connect Connectors Overview
# MAGIC
# MAGIC LakeFlow Connect provides three categories of connectors, each designed for a different type of data source and ingestion pattern.
# MAGIC
# MAGIC <div style="max-width: 1100px; margin: 20px auto 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC
# MAGIC <div style="display: flex; gap: 16px; align-items: stretch;">
# MAGIC
# MAGIC   <div style="flex: 1; border: 2px solid #FF5F46; border-radius: 10px; padding: 20px;">
# MAGIC     <div style="text-align: center; margin-bottom: 12px;">
# MAGIC       <img src="./Includes/images/icons/cloud_upload.png" alt="Upload Files icon" style="max-height: 80px; max-width: 100px; width: auto; height: auto;">
# MAGIC     </div>
# MAGIC     <div style="font-size: 16pt; font-weight: 700; margin-bottom: 12px;">Upload Files</div>
# MAGIC     <ul style="font-size: 14pt; line-height: 1.7; padding-left: 18px; margin: 0;">
# MAGIC       <li>Upload local files to Databricks</li>
# MAGIC       <li>Upload a file to a volume</li>
# MAGIC       <li>Create a table from a local file</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex: 1; border: 2px solid #FF5F46; border-radius: 10px; padding: 20px;">
# MAGIC     <div style="text-align: center; margin-bottom: 12px;">
# MAGIC       <img src="./Includes/images/icons/standard_connectors.png" alt="Standard Connectors icon" style="max-height: 80px; max-width: 100px; width: auto; height: auto;">
# MAGIC     </div>
# MAGIC     <div style="font-size: 16pt; font-weight: 700; margin-bottom: 12px;">Standard Connectors</div>
# MAGIC     <div style="font-size: 14pt; line-height: 1.6;">
# MAGIC       <strong>Supported Sources:</strong>
# MAGIC       <ul style="padding-left: 18px; margin: 4px 0 10px;">
# MAGIC         <li>Cloud Object Storage</li>
# MAGIC         <li>Kafka</li>
# MAGIC         <li>Other Sources</li>
# MAGIC       </ul>
# MAGIC       <strong>Ingestion Methods:</strong>
# MAGIC       <ul style="padding-left: 18px; margin: 4px 0 0;">
# MAGIC         <li>Batch</li>
# MAGIC         <li>Incremental Batch</li>
# MAGIC         <li>Streaming</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex: 1; border: 2px solid #FF5F46; border-radius: 10px; padding: 20px;">
# MAGIC     <div style="text-align: center; margin-bottom: 12px;">
# MAGIC       <img src="./Includes/images/icons/manager_connectors.png" alt="Managed Connectors icon" style="max-height: 80px; max-width: 100px; width: auto; height: auto;">
# MAGIC     </div>
# MAGIC     <div style="font-size: 16pt; font-weight: 700; margin-bottom: 12px;">Managed Connectors</div>
# MAGIC     <div style="font-size: 14pt; line-height: 1.6;">
# MAGIC       Ingest data into the lakehouse from:
# MAGIC       <ul style="padding-left: 18px; margin: 4px 0 10px;">
# MAGIC         <li>Software as a Service (SaaS) applications</li>
# MAGIC         <li>Databases</li>
# MAGIC       </ul>
# MAGIC       <ul style="padding-left: 18px; margin: 0;">
# MAGIC         <li>Leverage efficient <strong>incremental reads and writes</strong></li>
# MAGIC         <li>Faster, scalable, and more cost-efficient</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC - **Manual File Uploads** allow users to quickly upload local files into volumes or tables.
# MAGIC - **Standard Connectors** enable ingestion from sources like cloud storage and Kafka using batch, incremental, or streaming modes.
# MAGIC - **Managed Connectors** are designed for enterprise applications and databases, enabling scalable and efficient incremental data ingestion into the lakehouse.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Ingestion Methods
# MAGIC
# MAGIC When ingesting data into Databricks using LakeFlow Connect Standard Connectors, you can choose from several ingestion methods.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. Batch, Incremental Batch, and Streaming Ingestion
# MAGIC
# MAGIC Select each tab to explore the three ingestion methods available in LakeFlow Connect.
# MAGIC
# MAGIC <div style="width: 100%; margin: auto; font-family: sans-serif;">
# MAGIC
# MAGIC <style>
# MAGIC .four-grid {
# MAGIC     display: flex;
# MAGIC     flex-direction: column;
# MAGIC     gap: 60px;
# MAGIC     justify-content: center;
# MAGIC     align-items: center;
# MAGIC }
# MAGIC .ing-box {
# MAGIC     width: 80%;
# MAGIC     min-height: 400px;
# MAGIC     background: #F9F7F4;
# MAGIC     border: none;
# MAGIC     border-radius: 8px;
# MAGIC     box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC     overflow: hidden;
# MAGIC     display: flex;
# MAGIC     flex-direction: column;
# MAGIC     gap: 12px;
# MAGIC     padding: 20px;
# MAGIC     text-align: center;
# MAGIC     position: relative;
# MAGIC     box-sizing: border-box;
# MAGIC }
# MAGIC .ing-box::before {
# MAGIC     content: "";
# MAGIC     position: absolute;
# MAGIC     top: 0; left: 0;
# MAGIC     width: 100%; height: 8px;
# MAGIC }
# MAGIC .ing-box.batch::before       { background: #2574B5; }
# MAGIC .ing-box.incremental::before { background: #02A36F; }
# MAGIC .ing-box.streaming::before   { background: #FE3722; }
# MAGIC
# MAGIC .ing-box-title { font-size: 16pt; font-weight: bold; text-align: center; }
# MAGIC .ing-box-icon  { display: inline-flex; align-items: center; justify-content: center; gap: 8px; }
# MAGIC .ing-box-icon img { width: 50px; height: auto; border-radius: 4px; background: transparent; mix-blend-mode: multiply;
# MAGIC filter: contrast(1.15) brightness(1); }
# MAGIC .ing-box-content {
# MAGIC     display: flex;
# MAGIC     align-items: center;
# MAGIC     justify-content: center;
# MAGIC     gap: 24px;
# MAGIC     width: 100%;
# MAGIC }
# MAGIC .ing-box-text {
# MAGIC     font-size: 14pt;
# MAGIC     max-width: 500px;
# MAGIC     text-align: left;
# MAGIC     line-height: 1.6;
# MAGIC }
# MAGIC .ing-box-text ul { text-align: left; padding-left: 18px; margin: 0 0 14px 0; }
# MAGIC .ing-box-text li { margin-bottom: 10px; }
# MAGIC .ing-box-text li:last-child { margin-bottom: 0; }
# MAGIC .ing-example {
# MAGIC     padding: 12px 14px;
# MAGIC     border-radius: 8px;
# MAGIC     font-size: 14pt;
# MAGIC     line-height: 1.6;
# MAGIC     font-weight: 400;
# MAGIC }
# MAGIC .batch .ing-example       { background: rgba(37,116,181,0.10); border-left: 4px solid #2574B5; }
# MAGIC .incremental .ing-example { background: rgba(2,163,111,0.10);  border-left: 4px solid #02A36F; }
# MAGIC .streaming .ing-example   { background: rgba(254,55,34,0.10);  border-left: 4px solid #FE3722; }
# MAGIC </style>
# MAGIC
# MAGIC <!-- Tabs -->
# MAGIC <div style="display: flex; border-bottom: 2px solid #EEEDE9; margin-bottom: 0;">
# MAGIC   <button class="dbtab" onclick="showTab(1)" style="padding: 10px 18px; border: none; border-bottom: 3px solid #2574B5; background: none; font-size: 14pt; font-weight: bold; color: #2574B5; cursor: pointer; margin-bottom: -2px;">Batch Ingestion</button>
# MAGIC   <button class="dbtab" onclick="showTab(2)" style="padding: 10px 18px; border: none; border-bottom: 3px solid transparent; background: none; font-size: 14pt; font-weight: bold; color: #888; cursor: pointer; margin-bottom: -2px;">Incremental Batch Ingestion</button>
# MAGIC   <button class="dbtab" onclick="showTab(3)" style="padding: 10px 18px; border: none; border-bottom: 3px solid transparent; background: none; font-size: 14pt; font-weight: bold; color: #888; cursor: pointer; margin-bottom: -2px;">Streaming Ingestion</button>
# MAGIC </div>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <!-- TAB 1 -->
# MAGIC <div class="dbpanel" style="display: block;">
# MAGIC   <div class="four-grid">
# MAGIC     <div class="ing-box batch">
# MAGIC       <div class="ing-box-title">
# MAGIC         <div class="ing-box-icon">
# MAGIC           <img src="./Includes/images/icons/batch.png" alt="Batch icon">
# MAGIC           <span>Batch Ingestion</span>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div class="ing-box-content">
# MAGIC         <div>
# MAGIC           <img src="./Includes/images/lecture_lakeflow_connect/batch_ingestion.png"
# MAGIC                alt="Batch Ingestion diagram"
# MAGIC                style="width: 500px; max-width: 100%; height: auto; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC         </div> 
# MAGIC         <div class="ing-box-text">
# MAGIC           <ul>
# MAGIC             <li>Load data as <strong>batches of rows into Databricks</strong>, often based on a schedule</li>
# MAGIC             <li>Traditional batch ingestion <strong>processes all records</strong> each time it runs</li>
# MAGIC           </ul>
# MAGIC           <div class="ing-example">
# MAGIC             <strong>Common techniques include:</strong>
# MAGIC             <ol>
# MAGIC               <li>The SQL statement: <code>CREATE TABLE AS SELECT</code></li>
# MAGIC               <li>The Python method: <code>spark.read.load()</code></li>
# MAGIC             </ol>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <!-- TAB 2 -->
# MAGIC <div class="dbpanel" style="display: none;">
# MAGIC   <div class="four-grid">
# MAGIC     <div class="ing-box incremental">
# MAGIC       <div class="ing-box-title">
# MAGIC         <div class="ing-box-icon">
# MAGIC           <img src="./Includes/images/icons/incremental.png" alt="Incremental icon">
# MAGIC           <span>Incremental Batch Ingestion</span>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div class="ing-box-content">
# MAGIC         <div>
# MAGIC           <img src="./Includes/images/lecture_lakeflow_connect/incremental_ingestion.png"
# MAGIC                alt="Incremental Batch Ingestion diagram"
# MAGIC                style="width: 500px; max-width: 100%; height: auto; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC         </div>
# MAGIC         <div class="ing-box-text">
# MAGIC           <ul>
# MAGIC             <li><strong>Only new data is ingested</strong> — previously loaded records are <strong>skipped automatically</strong></li>
# MAGIC             <li>Provides <strong>faster</strong> and more <strong>resource-efficient</strong> ingestion by processing less data</li>
# MAGIC           </ul>
# MAGIC           <div class="ing-example">
# MAGIC             <strong>Common techniques include:</strong>
# MAGIC             <ol>
# MAGIC               <li>The SQL statement: <code>COPY INTO</code></li>
# MAGIC               <li>The Python method: <code>spark.readStream</code> (Auto Loader with a timed trigger)</li>
# MAGIC               <li>Declarative Pipelines: <code>CREATE OR REFRESH STREAMING TABLE</code></li>
# MAGIC             </ol>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <!-- TAB 3 -->
# MAGIC <div class="dbpanel" style="display: none;">
# MAGIC   <div class="four-grid">
# MAGIC     <div class="ing-box streaming">
# MAGIC       <div class="ing-box-title">
# MAGIC         <div class="ing-box-icon">
# MAGIC           <img src="./Includes/images/icons/streaming.png" alt="Streaming icon">
# MAGIC           <span>Streaming Ingestion</span>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div class="ing-box-content">
# MAGIC         <div>
# MAGIC           <img src="./Includes/images/lecture_lakeflow_connect/streaming_ingestion.png"
# MAGIC                alt="Streaming Ingestion diagram"
# MAGIC                style="width: 500px; max-width: 100%; height: auto; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC         </div>
# MAGIC         <div class="ing-box-text">
# MAGIC           <ul>
# MAGIC             <li><strong>Continuously load data</strong> rows or batches of data rows as they are generated, so you can query them as they <strong>arrive in near real-time</strong></li>
# MAGIC             <li><strong>Micro-batch</strong> processes small batches at very <strong>short, frequent intervals</strong></li>
# MAGIC           </ul>
# MAGIC           <div class="ing-example">
# MAGIC             <strong>Common techniques include:</strong>
# MAGIC             <ol>
# MAGIC               <li><code>spark.readStream</code> (Auto Loader with continuous trigger)</li>
# MAGIC               <li>Declarative Pipelines (trigger mode continuous)</li>
# MAGIC             </ol>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function showTab(n) {
# MAGIC   var tabs   = document.getElementsByClassName("dbtab");
# MAGIC   var panels = document.getElementsByClassName("dbpanel");
# MAGIC   var colors = ["#2574B5", "#02A36F", "#FE3722"];
# MAGIC   for (var i = 0; i < tabs.length; i++) {
# MAGIC     tabs[i].style.color        = "#888";
# MAGIC     tabs[i].style.borderBottom = "3px solid transparent";
# MAGIC     panels[i].style.display    = "none";
# MAGIC   }
# MAGIC   tabs[n-1].style.color        = colors[n-1];
# MAGIC   tabs[n-1].style.borderBottom = "3px solid " + colors[n-1];
# MAGIC   panels[n-1].style.display    = "block";
# MAGIC }
# MAGIC window.onload = function() { showTab(1); };
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC #### Batch Ingestion
# MAGIC 1. Batch ingestion is well-suited for large volumes of historical data where real-time processing is not required.
# MAGIC 2. It is typically simpler to implement and manage, making it a common choice for scheduled data pipelines.
# MAGIC
# MAGIC #### Incremental Batch Ingestion
# MAGIC 1. Databricks supports both traditional batch ingestion and incremental batch ingestion options.
# MAGIC 2. While traditional batch ingestion processes all records every time it runs, incremental batch ingestion automatically detects new records in the data source and skips records that have already been ingested.
# MAGIC
# MAGIC #### Streaming Ingestion
# MAGIC 1. With streaming ingestion, data is continuously loaded as it is generated, allowing you to query it in near real-time. This method is ideal for loading streaming data from sources such as Apache Kafka, Amazon Kinesis, Google Pub/Sub, and Apache Pulsar.
# MAGIC 2. Streaming ingestion processes data as it arrives, enabling low-latency analysis and immediate action. In contrast, micro-batch ingestion collects data over short, frequent intervals (seconds or minutes) and processes it in small batches — striking a balance between latency and system efficiency.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Delta Lake Review
# MAGIC
# MAGIC Delta Lake delivers open, reliable, and scalable data management for the Lakehouse, empowering you to ingest data from external sources and efficiently manage it across **Bronze (raw)**, **Silver (cleaned)**, and **Gold (curated)** layers — all with full ACID transactions, time travel, schema enforcement, and support for both batch and streaming workloads.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. Ingesting Data Into Delta Lake
# MAGIC
# MAGIC The goal is to ingest files from **external data sources** like cloud object storage into **Delta Lake as Delta tables**.
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="./Includes/images/lecture_lakeflow_connect/delta-lake.png"
# MAGIC     alt="Ingesting Data Into Delta Lake"
# MAGIC     style="width: 900px; max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC Delta Lake is an open-source protocol for reading and writing files to cloud storage. Delta tables offer an open table format that supports the **Lakehouse architecture** and storage on AWS, Azure, and GCP cloud data lakes.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D2. Delta Table Components Overview
# MAGIC
# MAGIC Within Delta Lake you will work with Delta tables. The diagram below shows how data is stored internally.
# MAGIC
# MAGIC <div style="display: flex; gap: 0; margin: 20px 0; align-items: stretch;">
# MAGIC
# MAGIC   <div style="flex: 1; background: #E8F5E9; border-top: 4px solid #388E3C; border-radius: 12px 0 0 12px; padding: 24px;">
# MAGIC     <div style="font-size: 14pt; color: #1B5E20; margin-bottom: 16px; line-height: 1.7;">
# MAGIC       <ol>
# MAGIC         <li>Data is stored in Delta tables.</li>
# MAGIC         <li>Internally, these tables store data as files within a folder directory.</li>
# MAGIC       </ol>
# MAGIC     </div>
# MAGIC     <div style="text-align: center;">
# MAGIC       <img src="./Includes/images/lecture_lakeflow_connect/delta-table.png"
# MAGIC            alt="Delta table folder structure"
# MAGIC            style="max-width: 90%; height: auto; border-radius: 6px; border: 1px solid #e0e0e0;">
# MAGIC       <p style="margin: 12px 0 0 0; font-size: 13px; color: #666; font-style: italic;">Delta tables store data as files within a folder directory.</p>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="display: flex; align-items: center; justify-content: center; padding: 0 20px; background: #F8F9FA; border-top: 1px solid #E0E0E0; border-bottom: 1px solid #E0E0E0;">
# MAGIC     <div style="font-size: 24px; color: #999; font-weight: 300;">→</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="flex: 1; background: #E3F2FD; border-top: 4px solid #1976D2; border-radius: 0 12px 12px 0; padding: 24px;">
# MAGIC     <div style="font-size: 14pt; color: #0D47A1; margin-bottom: 16px; line-height: 1.7;">
# MAGIC       <ol>
# MAGIC         <li>Data is stored as Parquet files in the directory.</li>
# MAGIC         <li>Delta Lake adds JSON-based Delta logs alongside these files.</li>
# MAGIC         <li>Delta logs track transactions and table versions.</li>
# MAGIC       </ol>
# MAGIC     </div>
# MAGIC     <div style="text-align: center;">
# MAGIC       <img src="./Includes/images/lecture_lakeflow_connect/delta-lake-table.png"
# MAGIC            alt="Delta Lake Parquet files and transaction logs"
# MAGIC            style="max-width: 100%; height: auto; border-radius: 6px; border: 1px solid #e0e0e0;">
# MAGIC       <p style="margin: 12px 0 0 0; font-size: 13px; color: #666; font-style: italic;">Delta Lake stores data as Parquet files and maintains transaction logs.</p>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D3. Delta Table Key Features
# MAGIC
# MAGIC Delta tables provide a variety of key features in a cloud data lake:
# MAGIC
# MAGIC <div class="mermaid">
# MAGIC flowchart LR
# MAGIC     acid["<img src='./Includes/images/lecture_lakeflow_connect/ACID.png'/><br/><b style='font-size:15px;'>ACID Transactions</b><br/><br/><b>A</b>tomicity<br><b>C</b>onsistency<br><b>I</b>solation<br><b>D</b>urability<br/>For all operations, allowing <b>multiple users to read and write data</b> concurrently without conflicts."]
# MAGIC     dml["<img src='./Includes/images/lecture_lakeflow_connect/DML.png'/><br/><b style='font-size:15px;'>Data Manipulation Language (DML)</b><br/><br/>Supports DML operations such as <b>INSERT, UPDATE, DELETE, and MERGE</b>, enabling flexible data management."]
# MAGIC     tt["<img src='./Includes/images/lecture_lakeflow_connect/time-travel.png'/><br/><b style='font-size:15px;'>Time Travel</b><br/><br/>Allows users to <b>query</b> and <b>revert</b> to previous versions of data, facilitating <b>auditing and recovery</b>."]
# MAGIC     schema["<img src='./Includes/images/lecture_lakeflow_connect/schema.png'/><br/><b style='font-size:15px;'>Schema Evolution and Enforcement</b><br/><br/>Enforces a defined <b>schema for data integrity</b> while allowing schema evolution, enabling structural changes without breaking existing workflows."]
# MAGIC     mm["<img src='./Includes/images/lecture_lakeflow_connect/many-more.png'/><br/><b style='font-size:15px;'>Many More!</b><br/><br/>Provides additional features including unified batch and streaming processing, performance optimization, and scalability."]
# MAGIC     acid --- dml --- tt --- schema --- mm
# MAGIC classDef process fill:#FFFFFF,color:#0b2026,stroke:#FF5F46,stroke-width:3px
# MAGIC class dml,tt,schema process
# MAGIC classDef endpoint fill:#FFFFFF,color:#0b2026,stroke:#1B5162,stroke-width:3px
# MAGIC class acid,mm endpoint
# MAGIC linkStyle 0 stroke:#618794,stroke-width:3px
# MAGIC linkStyle 1 stroke:#618794,stroke-width:3px
# MAGIC linkStyle 2 stroke:#618794,stroke-width:3px
# MAGIC linkStyle 3 stroke:#618794,stroke-width:3px
# MAGIC </div>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({
# MAGIC   startOnLoad: true,
# MAGIC   theme: "base",
# MAGIC   htmlLabels: true,
# MAGIC   flowchart: {
# MAGIC     htmlLabels: true,
# MAGIC     curve: "basis",
# MAGIC     nodeSpacing: 30,
# MAGIC     rankSpacing: 40,
# MAGIC     padding: 12
# MAGIC   },
# MAGIC   themeVariables: {
# MAGIC     background: "#FFFFFF",
# MAGIC     primaryColor: "#FFFFFF",
# MAGIC     primaryTextColor: "#0b2026",
# MAGIC     primaryBorderColor: "#1B5162",
# MAGIC     lineColor: "#618794",
# MAGIC     secondaryColor: "#FFFFFF",
# MAGIC     tertiaryColor: "#FFFFFF",
# MAGIC     edgeLabelBackground: "#FFFFFF",
# MAGIC     fontFamily: "Arial, sans-serif"
# MAGIC   }
# MAGIC });
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC The **transaction log** enables key functionality in Delta tables by maintaining table states.
# MAGIC
# MAGIC - It records every **insert, update, and delete** as transactions, ensuring the table stays consistent and up to date.
# MAGIC - This allows for **reliable data views and time travel**, making it easy to access previous versions of data.
# MAGIC
# MAGIC Traditionally, modifying data in a data lake required **manually recreating files and tracking changes**, which was complex and inefficient.
# MAGIC
# MAGIC - With Delta Lake, data modifications are **simpler, faster, and more efficient to manage**.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Medallion Architecture Review

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E1. Overview
# MAGIC
# MAGIC The Medallion Architecture is a layered data design pattern that progressively improves data quality as it moves through Bronze, Silver, and Gold layers.
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="./Includes/images/lecture_lakeflow_connect/medallion-architecture.png"
# MAGIC     alt="Medallion Architecture Review"
# MAGIC     style="width: 900px; max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### E2. Medallion Architecture Steps in Detail
# MAGIC
# MAGIC Each step in the Medallion Architecture builds on the previous one, refining data quality and usability.
# MAGIC
# MAGIC <div class="mermaid">
# MAGIC flowchart LR
# MAGIC     S0["<b>Step 1</b><br/><b>Ingest Data</b><br/>Data is ingested into Delta Lake using batch, streaming, or both methods.<br/>This marks the starting point for processing in Databricks."]
# MAGIC     S1["<b>Step 2</b><br/><b>Process and Improve Data Quality</b><br/>Data is incrementally refined as it moves through layers.<br/>Each stage improves structure, quality, and usability."]
# MAGIC     S2["<b>Step 3</b><br/><b>Bronze Layer (Raw Data)</b><br/>Stores raw, unprocessed data from multiple sources.<br/>Acts as the foundation for all downstream processing."]
# MAGIC     S3["<b>Step 4</b><br/><b>Silver Layer (Cleaned Data)</b><br/>Data is cleaned, transformed, and enriched.<br/>Produces structured and analysis-ready datasets."]
# MAGIC     S4["<b>Step 5</b><br/><b>Gold Layer (Business-Ready Data)</b><br/>Contains curated and aggregated data.<br/>Optimized for reporting, BI, and advanced analytics."]
# MAGIC     S0 --> S1 --> S2 --> S3 --> S4
# MAGIC     style S0 fill:#E8F4FD,stroke:#5A9BD5,stroke-width:2px
# MAGIC     style S1 fill:#E5F5F3,stroke:#5BA8A0,stroke-width:2px
# MAGIC     style S2 fill:#EFF6E8,stroke:#7CB342,stroke-width:2px
# MAGIC     style S3 fill:#FFF8E6,stroke:#E6AC00,stroke-width:2px
# MAGIC     style S4 fill:#FFEFE8,stroke:#E86A4A,stroke-width:2px
# MAGIC </div>
# MAGIC
# MAGIC <script type="module">
# MAGIC import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";
# MAGIC mermaid.initialize({ startOnLoad: true, theme: "default" });
# MAGIC </script>

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Conclusion
# MAGIC
# MAGIC In this lecture, you learned the fundamentals of data engineering in Databricks with a focus on LakeFlow Connect and Delta Lake:
# MAGIC
# MAGIC 1. **LakeFlow Connect** is the ingestion layer within the Databricks Data Intelligence Platform that replaces the traditional patchwork of ingestion tools with a unified, managed solution.
# MAGIC 2. LakeFlow Connect provides three types of connectors: **Upload Files**, **Standard Connectors**, and **Managed Connectors**, each designed for different data source types.
# MAGIC 3. Three ingestion methods are available: **Batch** (processes all records), **Incremental Batch** (processes only new records), and **Streaming** (continuous, near real-time ingestion).
# MAGIC 4. **Delta Lake** is an open-source protocol that stores data as Parquet files with transaction logs, enabling ACID transactions, time travel, DML support, and schema enforcement.
# MAGIC 5. The **Medallion Architecture** (Bronze, Silver, Gold) progressively improves data quality as it moves through each layer.
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next section, you will begin working hands-on with LakeFlow Connect to ingest data into Delta Lake tables.

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