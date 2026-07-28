# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ![DBAcademy](./Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lecture - Additional Features and Ingesting into Existing Delta Tables
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Databricks provides additional features such as Lakehouse Federation, Zerobus, Delta Sharing, and Databricks Marketplace to expand data integration, sharing, and collaboration capabilities in the Lakehouse.
# MAGIC
# MAGIC This lecture also introduces how `MERGE INTO` streamlines ingestion into existing Delta tables by applying updates, inserts, and deletes from a source in a single atomic operation.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC
# MAGIC 1. **Describe additional Databricks features** that extend data integration, sharing, and collaboration capabilities
# MAGIC 2. **Identify key Databricks Marketplace components** and explore shared data assets
# MAGIC 3. **Explain the purpose of MERGE INTO** for applying updates, inserts, and deletes to existing Delta tables
# MAGIC 4. **Describe MERGE INTO clauses** including matched updates, matched deletes, and not matched inserts
# MAGIC 5. **Write a MERGE INTO statement** to merge source data into a target Delta table

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. What's Next: Features Outside This Course
# MAGIC
# MAGIC While this course focuses on LakeFlow Connect managed connectors, there are other ingestion features in Databricks that may be useful as your architecture evolves.
# MAGIC
# MAGIC ##### Click on the tabs to switch between the features.
# MAGIC <br>
# MAGIC <div style="width: 100%; margin: auto; font-family: sans-serif;">
# MAGIC
# MAGIC <style>
# MAGIC .feat-grid {
# MAGIC     display: flex;
# MAGIC     flex-direction: column;
# MAGIC     gap: 60px;
# MAGIC     justify-content: center;
# MAGIC     align-items: center;
# MAGIC }
# MAGIC .feat-box {
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
# MAGIC .feat-box::before {
# MAGIC     content: "";
# MAGIC     position: absolute;
# MAGIC     top: 0; left: 0;
# MAGIC     width: 100%; height: 8px;
# MAGIC     background: #FF5F46;
# MAGIC }
# MAGIC .feat-box-title {
# MAGIC     font-size: 16pt;
# MAGIC     font-weight: bold;
# MAGIC     text-align: center;
# MAGIC     margin-top: 4px;
# MAGIC }
# MAGIC .feat-box-content {
# MAGIC     display: flex;
# MAGIC     align-items: center;
# MAGIC     justify-content: center;
# MAGIC     gap: 24px;
# MAGIC     width: 100%;
# MAGIC }
# MAGIC .feat-box-text {
# MAGIC     font-size: 14pt;
# MAGIC     max-width: 420px;
# MAGIC     text-align: left;
# MAGIC     line-height: 1.6;
# MAGIC }
# MAGIC .feat-box-text ul { text-align: left; padding-left: 18px; margin: 0 0 14px 0; }
# MAGIC .feat-box-text li { margin-bottom: 10px; }
# MAGIC .feat-box-text li:last-child { margin-bottom: 0; }
# MAGIC </style>
# MAGIC
# MAGIC <!-- Tabs -->
# MAGIC <div style="display: flex; border-bottom: 2px solid #EEEDE9; margin-bottom: 0;">
# MAGIC   <button class="feattab" onclick="showFeatTab(1)" style="padding: 10px 18px; border: none; border-bottom: 3px solid #FF5F46; background: none; font-size: 14pt; font-weight: bold; color: #FF5F46; cursor: pointer; margin-bottom: -2px;">Lakehouse Federation</button>
# MAGIC   <button class="feattab" onclick="showFeatTab(2)" style="padding: 10px 18px; border: none; border-bottom: 3px solid transparent; background: none; font-size: 14pt; font-weight: bold; color: #888; cursor: pointer; margin-bottom: -2px;">Zerobus</button>
# MAGIC   <button class="feattab" onclick="showFeatTab(3)" style="padding: 10px 18px; border: none; border-bottom: 3px solid transparent; background: none; font-size: 14pt; font-weight: bold; color: #888; cursor: pointer; margin-bottom: -2px;">Delta Sharing</button>
# MAGIC </div>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <!-- TAB 1: Lakehouse Federation -->
# MAGIC <div class="featpanel" style="display: block;">
# MAGIC   <div class="feat-grid">
# MAGIC     <div class="feat-box">
# MAGIC       <div class="feat-box-title">Lakehouse Federation</div>
# MAGIC       <div class="feat-box-content">
# MAGIC         <div>
# MAGIC           <img src="./Includes/images/lecture_additional_features/lakehouse_federation.png"
# MAGIC                alt="Lakehouse Federation diagram"
# MAGIC                style="width: 620px; max-width: 100%; height: auto; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC         </div>
# MAGIC         <div class="feat-box-text">
# MAGIC           Allows you to <strong>query external data sources</strong> without moving your data.
# MAGIC           <br><br>
# MAGIC           Especially useful for:
# MAGIC           <ul>
# MAGIC             <li><strong>Ad hoc reporting</strong></li>
# MAGIC             <li><strong>Proof-of-concept</strong> work</li>
# MAGIC             <li>The <strong>exploratory phase</strong> of new ETL pipelines or reports</li>
# MAGIC             <li>Supporting workloads during <strong>incremental migration</strong></li>
# MAGIC           </ul>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- TAB 2: Zerobus -->
# MAGIC <div class="featpanel" style="display: none;">
# MAGIC   <div class="feat-grid">
# MAGIC     <div class="feat-box">
# MAGIC       <div class="feat-box-title">Zerobus <em>(coming soon)</em></div>
# MAGIC       <div class="feat-box-content">
# MAGIC         <div>
# MAGIC           <img src="./Includes/images/lecture_additional_features/Zerobus.png"
# MAGIC                alt="Zerobus diagram"
# MAGIC                style="width: 620px; max-width: 100%; height: auto; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC         </div>
# MAGIC         <div class="feat-box-text">
# MAGIC           A <strong>LakeFlow Connect API</strong> that allows developers to <strong>write event data directly to their lakehouse</strong> at very high throughput (100 MB/s) with near real-time latency (&lt;5 seconds).
# MAGIC           <br><br>
# MAGIC           <strong>Simplify ingestion</strong> for:
# MAGIC           <ul>
# MAGIC             <li>IOT</li>
# MAGIC             <li>Clickstreams</li>
# MAGIC             <li>Telemetry, and more</li>
# MAGIC           </ul>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- TAB 3: Delta Sharing -->
# MAGIC <div class="featpanel" style="display: none;">
# MAGIC   <div class="feat-grid">
# MAGIC     <div class="feat-box">
# MAGIC       <div class="feat-box-title">Delta Sharing</div>
# MAGIC       <div class="feat-box-content">
# MAGIC         <div>
# MAGIC           <img src="./Includes/images/lecture_additional_features/Delta_sharing.png"
# MAGIC                alt="Delta Sharing diagram"
# MAGIC                style="width: 620px; max-width: 100%; height: auto; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC         </div>
# MAGIC         <div class="feat-box-text">
# MAGIC           Allows you to <strong>securely share data</strong> across platforms, clouds, and regions.
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function showFeatTab(n) {
# MAGIC   var tabs   = document.getElementsByClassName("feattab");
# MAGIC   var panels = document.getElementsByClassName("featpanel");
# MAGIC   for (var i = 0; i < tabs.length; i++) {
# MAGIC     tabs[i].style.color        = "#888";
# MAGIC     tabs[i].style.borderBottom = "3px solid transparent";
# MAGIC     panels[i].style.display    = "none";
# MAGIC   }
# MAGIC   tabs[n-1].style.color        = "#FF5F46";
# MAGIC   tabs[n-1].style.borderBottom = "3px solid #FF5F46";
# MAGIC   panels[n-1].style.display    = "block";
# MAGIC }
# MAGIC window.onload = function() { showFeatTab(1); };
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Documentation
# MAGIC
# MAGIC <ol>
# MAGIC   <li><strong>Lakehouse Federation</strong>
# MAGIC     <ul>
# MAGIC       <li><a href="https://docs.databricks.com/aws/en/query-federation/" style="color:#1976D2;">What is Lakehouse Federation</a> documentation</li>
# MAGIC       <li><a href="https://www.databricks.com/resources/demos/videos/governance/lakehouse-federation" style="color:#1976D2;">Lakehouse Federation: Discover, query and govern your data — no matter where it lives</a></li>
# MAGIC     </ul>
# MAGIC   </li>
# MAGIC   <br>
# MAGIC   <li><strong>Zerobus</strong>
# MAGIC     <ul>
# MAGIC       <li><a href="https://www.databricks.com/dataaisummit/session/eliminate-hops-your-streaming-architecture-zerobus-part-lakeflow" style="color:#1976D2;">Eliminate Hops in Your Streaming Architecture with Zerobus, Part of LakeFlow Connect</a></li>
# MAGIC       <li><a href="https://www.databricks.com/blog/announcing-general-availability-databricks-lakeflow" style="color:#1976D2;">Announcing the General Availability of Databricks LakeFlow</a></li>
# MAGIC     </ul>
# MAGIC   </li>
# MAGIC   <br>
# MAGIC   <li><strong>Delta Sharing</strong>
# MAGIC     <ul>
# MAGIC       <li><a href="https://www.databricks.com/resources/demos/videos/data-sharing/delta-sharing" style="color:#1976D2;">Delta Sharing Demo</a></li>
# MAGIC       <li><a href="https://docs.databricks.com/aws/en/delta-sharing/" style="color:#1976D2;">What is Delta Sharing documentation</a></li>
# MAGIC     </ul>
# MAGIC   </li>
# MAGIC </ol>

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Ingesting Data with Databricks Marketplace
# MAGIC
# MAGIC Databricks Marketplace is an open marketplace for all your data, analytics, and AI, powered by the open source Delta Sharing standard. The Databricks Marketplace expands your opportunity to deliver innovation and advance all your analytics and AI initiatives.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div class="two-col-section">
# MAGIC
# MAGIC <style>
# MAGIC .two-col-section {
# MAGIC     display: flex;
# MAGIC     justify-content: center;
# MAGIC     align-items: stretch;
# MAGIC     gap: 0;
# MAGIC     max-width: 960px;
# MAGIC     margin: 0 auto;
# MAGIC     border-radius: 10px;
# MAGIC     overflow: hidden;
# MAGIC     box-shadow: 0 4px 20px rgba(27, 49, 57, 0.10);
# MAGIC     background: #F9F7F4;
# MAGIC }
# MAGIC
# MAGIC .two-col-image {
# MAGIC     flex: 1 1 45%;
# MAGIC     min-width: 300px;
# MAGIC     background: #FFFFFF;
# MAGIC     display: flex;
# MAGIC     align-items: center;
# MAGIC     justify-content: center;
# MAGIC     padding: 32px;
# MAGIC     border-right: 1px solid #E8E5E0;
# MAGIC }
# MAGIC
# MAGIC .two-col-image img {
# MAGIC     width: 100%;
# MAGIC     max-width: 380px;
# MAGIC     height: auto;
# MAGIC     object-fit: contain;
# MAGIC     display: block;
# MAGIC }
# MAGIC
# MAGIC .two-col-content {
# MAGIC     flex: 1 1 55%;
# MAGIC     padding: 40px 44px;
# MAGIC     display: flex;
# MAGIC     flex-direction: column;
# MAGIC     justify-content: center;
# MAGIC     position: relative;
# MAGIC }
# MAGIC
# MAGIC .two-col-content::before {
# MAGIC     content: "";
# MAGIC     position: absolute;
# MAGIC     top: 0;
# MAGIC     left: 0;
# MAGIC     width: 100%;
# MAGIC     height: 6px;
# MAGIC     background: #FF5F46;
# MAGIC     border-radius: 0 10px 0 0;
# MAGIC }
# MAGIC
# MAGIC .two-col-content h2 {
# MAGIC     font-size: 22pt;
# MAGIC     font-weight: 700;
# MAGIC     color: #1B3139;
# MAGIC     margin-bottom: 10px;
# MAGIC     margin-top: 16px;
# MAGIC }
# MAGIC
# MAGIC .two-col-content .subtitle {
# MAGIC     font-size: 13pt;
# MAGIC     color: #4A5E65;
# MAGIC     margin-bottom: 28px;
# MAGIC     line-height: 1.6;
# MAGIC }
# MAGIC
# MAGIC .two-col-content .subtitle strong {
# MAGIC     color: #1B3139;
# MAGIC }
# MAGIC
# MAGIC .product-list {
# MAGIC     list-style: none;
# MAGIC     padding: 0;
# MAGIC     margin: 0;
# MAGIC     display: flex;
# MAGIC     flex-direction: column;
# MAGIC     gap: 12px;
# MAGIC }
# MAGIC
# MAGIC .product-list li {
# MAGIC     display: flex;
# MAGIC     align-items: center;
# MAGIC     gap: 12px;
# MAGIC     font-size: 13pt;
# MAGIC     color: #1B3139;
# MAGIC     font-weight: 500;
# MAGIC }
# MAGIC
# MAGIC .product-list li::before {
# MAGIC     content: "";
# MAGIC     display: inline-block;
# MAGIC     width: 8px;
# MAGIC     height: 8px;
# MAGIC     min-width: 8px;
# MAGIC     border-radius: 50%;
# MAGIC     background: #FF5F46;
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <div class="two-col-image">
# MAGIC   <img
# MAGIC     src="./Includes/images/lecture_additional_features/databricks_marketplace.png"
# MAGIC     alt="Databricks Marketplace"
# MAGIC     style="width: 900px; max-width: 100%; height: auto;">
# MAGIC </div>
# MAGIC
# MAGIC <div class="two-col-content">
# MAGIC   <h2>Databricks Marketplace</h2>
# MAGIC   <p class="subtitle">
# MAGIC     It is an open exchange for all data products:
# MAGIC   </p>
# MAGIC   <ul class="product-list">
# MAGIC     <li>Datasets</li>
# MAGIC     <li>Notebooks</li>
# MAGIC     <li>Dashboards</li>
# MAGIC     <li>ML Models</li>
# MAGIC     <li>Solution Accelerators</li>
# MAGIC   </ul>
# MAGIC   <br>
# MAGIC   <b>Powered by Delta Sharing</b>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### Documentation
# MAGIC
# MAGIC - <a href="https://docs.databricks.com/aws/en/marketplace" style="color:#1976D2;">What is Databricks Marketplace</a> documentation
# MAGIC - <a href="https://www.databricks.com/product/marketplace" style="color:#1976D2;">Databricks Marketplace</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Delta Sharing with Databricks Marketplace
# MAGIC
# MAGIC Getting started with data assets from Databricks Marketplace is quick and easy in just three simple steps.
# MAGIC
# MAGIC ##### Click any step on the left to see detailed instructions and visuals here.
# MAGIC <br>
# MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="display: flex; align-items: stretch; gap: 16px;">
# MAGIC   <!-- Left arrow label -->
# MAGIC   <div style="
# MAGIC       writing-mode: vertical-lr;
# MAGIC       transform: rotate(180deg);
# MAGIC       text-align: center;
# MAGIC       font-weight: 700;
# MAGIC       font-size: 15pt;
# MAGIC       color: #618794;
# MAGIC       padding: 0 6px;
# MAGIC       display: flex;
# MAGIC       justify-content: flex-end;
# MAGIC   ">
# MAGIC   &larr; GETTING STARTED IN 3 STEPS
# MAGIC   </div>
# MAGIC   <!-- Main two-column layout -->
# MAGIC   <div style="flex: 1; display: flex; gap: 20px; align-items: flex-start;">
# MAGIC     <!-- LEFT: Step buttons -->
# MAGIC     <div style="display: flex; flex-direction: column; gap: 6px; width: 450px; flex-shrink: 0;">
# MAGIC       <!-- Step 1 -->
# MAGIC       <div style="display: flex; gap: 0; align-items: stretch;">
# MAGIC         <div onclick="showStep('step1')" style="cursor: pointer; background: #1C3037; color: white; border-radius: 8px 0 0 4px; padding: 22px 24px; text-align: center; flex: 1;">
# MAGIC           <div style="font-size: 14pt; font-weight: 700;">1. Open the Marketplace</div>
# MAGIC           <div style="font-size: 11pt; margin-top: 6px; opacity: 0.9;">In your Databricks Workspace, navigate to <strong>Databricks Marketplace</strong> from the left sidebar</div>
# MAGIC         </div>
# MAGIC         <div onclick="showStep('step1')" id="btn-step1" style="cursor: pointer; background: #152830; color: #a8c4cc; border-radius: 0 8px 4px 0; padding: 0 14px; display: flex; align-items: center; font-size: 18pt; font-weight: 700; user-select: none; min-width: 40px; justify-content: center;">&#8250;</div>
# MAGIC       </div>
# MAGIC       <!-- Step 2 -->
# MAGIC       <div style="display: flex; gap: 0; align-items: stretch;">
# MAGIC         <div onclick="showStep('step2')" style="cursor: pointer; background: #2574B5; color: white; border-radius: 4px 0 0 4px; padding: 18px 24px; text-align: center; flex: 1;">
# MAGIC           <div style="font-size: 14pt; font-weight: 700;">2. Search for Assets</div>
# MAGIC           <div style="font-size: 11pt; margin-top: 6px; opacity: 0.9;">Use the search bar to browse available data assets — start with assets provided directly by <strong>Databricks</strong></div>
# MAGIC         </div>
# MAGIC         <div onclick="showStep('step2')" id="btn-step2" style="cursor: pointer; background: #1a5d91; color: #a8cfe8; border-radius: 0 4px 4px 0; padding: 0 14px; display: flex; align-items: center; font-size: 18pt; font-weight: 700; user-select: none; min-width: 40px; justify-content: center;">&#8250;</div>
# MAGIC       </div>
# MAGIC       <!-- Step 3 -->
# MAGIC       <div style="display: flex; gap: 0; align-items: stretch;">
# MAGIC         <div onclick="showStep('step3')" style="cursor: pointer; background: #FF5F46; color: white; border-radius: 4px 0 8px 0; padding: 18px 24px; text-align: center; flex: 1;">
# MAGIC           <div style="font-size: 14pt; font-weight: 700;">3. Get Instant Access</div>
# MAGIC           <div style="font-size: 11pt; margin-top: 6px; opacity: 0.9;">Select the asset and click <strong>"Get instant access"</strong> — ready to start using shared, curated data assets!</div>
# MAGIC         </div>
# MAGIC         <div onclick="showStep('step3')" id="btn-step3" style="cursor: pointer; background: #cc4a34; color: #ffd5ce; border-radius: 0 4px 0 8px; padding: 0 14px; display: flex; align-items: center; font-size: 18pt; font-weight: 700; user-select: none; min-width: 40px; justify-content: center;">&#8250;</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- RIGHT: Expandable content panel -->
# MAGIC     <div style="flex: 1; min-height: 300px;">
# MAGIC       <!-- Default placeholder -->
# MAGIC       <div id="placeholder" style="
# MAGIC           height: 100%;
# MAGIC           min-height: 300px;
# MAGIC           background: #f5f7f8;
# MAGIC           border-radius: 8px;
# MAGIC           border: 2px dashed #c2d0d4;
# MAGIC           display: flex;
# MAGIC           flex-direction: column;
# MAGIC           align-items: center;
# MAGIC           justify-content: center;
# MAGIC           color: #8aa8b2;
# MAGIC           font-size: 13pt;
# MAGIC           text-align: center;
# MAGIC           padding: 32px;
# MAGIC           gap: 12px;
# MAGIC           box-sizing: border-box;
# MAGIC       ">
# MAGIC         <div style="font-size: 32pt;"> </div>
# MAGIC         <div style="font-weight: 700; color: #618794;">Select a step to explore</div>
# MAGIC         <div style="font-size: 11pt; max-width: 260px; line-height: 1.6;">Click any step on the left to see detailed instructions and visuals here.</div>
# MAGIC       </div>
# MAGIC       <!-- Step 1 Panel -->
# MAGIC       <div id="step1" style="display: none; background: #f0f5f6; border-left: 4px solid #1C3037; border-radius: 0 8px 8px 0; padding: 24px 28px; box-sizing: border-box;">
# MAGIC         <div style="font-size: 13pt; font-weight: 700; color: #1C3037; margin-bottom: 14px;">Where to find it</div>
# MAGIC         <div style="display: flex; gap: 28px; align-items: flex-start;">
# MAGIC           <ul style="font-size: 12pt; color: #2e4e58; line-height: 1.9; padding-left: 20px; margin: 0; flex: 1;">
# MAGIC             <li>Log in to your <strong>Databricks Workspace</strong></li>
# MAGIC             <li>Look for <strong>Marketplace</strong> in the left navigation sidebar</li>
# MAGIC             <li>Available to all workspace users by default</li>
# MAGIC           </ul>
# MAGIC           <div style="flex-shrink: 0; width: 340px;">
# MAGIC             <img
# MAGIC               src="./Includes/images/lecture_additional_features/step1_marketplace.png"
# MAGIC               style="width: 100%; border-radius: 6px; border: 2px solid #c8d8dc; display: block;"
# MAGIC             />
# MAGIC             <div style="font-size: 10pt; color: #618794; text-align: center; margin-top: 6px; font-style: italic;">
# MAGIC               Marketplace in the left sidebar
# MAGIC             </div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <!-- Step 2 Panel -->
# MAGIC       <div id="step2" style="display: none; background: #eef4fa; border-left: 4px solid #2574B5; border-radius: 0 8px 8px 0; padding: 24px 28px; box-sizing: border-box;">
# MAGIC         <div style="font-size: 13pt; font-weight: 700; color: #2574B5; margin-bottom: 14px;">What to look for</div>
# MAGIC         <ul style="font-size: 12pt; color: #1a3a52; line-height: 1.9; padding-left: 20px; margin: 0;">
# MAGIC           <li>Search by <strong>name, category, or provider</strong></li>
# MAGIC           <li>Filter by <strong>Free</strong> to find no-cost datasets</li>
# MAGIC           <li>Start with <strong>Databricks-provided</strong> assets — they are well-documented and ready to use</li>
# MAGIC           <li>Check <strong>Tables, Files,</strong> and <strong>Notebooks</strong> tabs for different asset types</li>
# MAGIC         </ul>
# MAGIC       </div>
# MAGIC       <!-- Step 3 Panel -->
# MAGIC       <div id="step3" style="display: none; background: #fff2f0; border-left: 4px solid #FF5F46; border-radius: 0 8px 8px 0; padding: 24px 28px; box-sizing: border-box;">
# MAGIC         <div style="font-size: 13pt; font-weight: 700; color: #cc4a34; margin-bottom: 14px;">What happens next</div>
# MAGIC         <div style="display: flex; gap: 28px; align-items: flex-start;">
# MAGIC           <ul style="font-size: 12pt; color: #4a1a10; line-height: 1.9; padding-left: 20px; margin: 0; flex: 1;">
# MAGIC             <li>The asset is added to your <strong>Unity Catalog</strong> automatically</li>
# MAGIC             <li>Access is <strong>instant</strong> — no approval workflow needed for free assets</li>
# MAGIC             <li>Query the data directly using <strong>SQL, notebooks, or dashboards</strong></li>
# MAGIC             <li>Data stays <strong>live and up-to-date</strong> via Delta Sharing — no copying required</li>
# MAGIC           </ul>
# MAGIC         </div>
# MAGIC         <br>
# MAGIC         <div style="text-align:center; flex-shrink: 0;">
# MAGIC           <img
# MAGIC             src="./Includes/images/lecture_additional_features/step2_get_access.png"
# MAGIC             alt="Get instant access button on Databricks Marketplace asset page"
# MAGIC             style="width:560px; max-width:100%; height:auto; border-radius: 6px; border: 2px solid #f5c4bc; display: block;">
# MAGIC         </div>
# MAGIC         <div style="font-size: 10pt; color: #cc4a34; text-align: center; margin-top: 6px; font-style: italic;">
# MAGIC           Click "Get instant access" to unlock the asset
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC   let activeStep = null;
# MAGIC   function showStep(id) {
# MAGIC     const allPanels = ['step1', 'step2', 'step3'];
# MAGIC     if (activeStep === id) {
# MAGIC       document.getElementById(id).style.display = 'none';
# MAGIC       document.getElementById('btn-' + id).innerHTML = '&#8250;';
# MAGIC       document.getElementById('placeholder').style.display = 'flex';
# MAGIC       activeStep = null;
# MAGIC       return;
# MAGIC     }
# MAGIC     allPanels.forEach(function(s) {
# MAGIC       document.getElementById(s).style.display = 'none';
# MAGIC       document.getElementById('btn-' + s).innerHTML = '&#8250;';
# MAGIC     });
# MAGIC     document.getElementById('placeholder').style.display = 'none';
# MAGIC     document.getElementById(id).style.display = 'block';
# MAGIC     document.getElementById('btn-' + id).innerHTML = '&#8964;';
# MAGIC     activeStep = id;
# MAGIC   }
# MAGIC </script>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Updates, Inserts, and Deletes on Delta Tables with MERGE INTO
# MAGIC
# MAGIC The MERGE INTO command allows you to apply updates, inserts, and deletes from a source table into an existing Delta table, all in a single, atomic operation.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. MERGE INTO Overview

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width:1000px; margin:0 auto; font-family:'Segoe UI',sans-serif; color:#0B2026;">
# MAGIC   <div style="background:#F9F7F4; border:2px solid #EEEDE9; border-radius:14px; padding:22px 24px; box-shadow:0 3px 12px rgba(0,0,0,0.06);">
# MAGIC     <div style="display:inline-block; background:#FF5F46; color:white; font-weight:800; font-size:10pt; padding:5px 12px; border-radius:999px; margin-bottom:12px;">
# MAGIC       MERGE INTO
# MAGIC     </div>
# MAGIC     <div style="font-size:11pt; line-height:1.8; color:#1B3139; margin-bottom:18px;">
# MAGIC       Merges a set of updates, insertions, and deletions from a source table into a target Delta table.
# MAGIC       MERGE INTO supports schema enforcement or schema evolution and allows different actions depending on whether a row is matched between a source and target table:
# MAGIC     </div>
# MAGIC     <div style="display:grid; grid-template-columns:repeat(3, 1fr); gap:12px; margin-bottom:18px;">
# MAGIC       <div style="background:white; border-radius:10px; border-top:4px solid #2574B5; padding:14px 16px; box-shadow:0 1px 6px rgba(0,0,0,0.05);">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:6px;">Matched rows</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.7;">UPDATE or DELETE</div>
# MAGIC       </div>
# MAGIC       <div style="background:white; border-radius:10px; border-top:4px solid #02A36F; padding:14px 16px; box-shadow:0 1px 6px rgba(0,0,0,0.05);">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:6px;">Unmatched rows by target</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.7;">INSERT</div>
# MAGIC       </div>
# MAGIC       <div style="background:white; border-radius:10px; border-top:4px solid #FFAB00; padding:14px 16px; box-shadow:0 1px 6px rgba(0,0,0,0.05);">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:6px;">Unmatched rows by source</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.7;">UPDATE or DELETE</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div style="background:rgba(255,95,70,0.08); border-bottom:4px solid #FF5F46; border-radius:8px; padding:12px 14px; font-size:10.5pt; line-height:1.75; color:#1B3139;">
# MAGIC       This functionality makes MERGE INTO ideal for handling slowly changing dimensions (SCDs), incremental loads, and complex change data capture (CDC) scenarios.
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. MERGE INTO Example Walkthrough
# MAGIC
# MAGIC There are situations where you need to update, insert, or delete records in a target table based on information from another table.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 1000px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC   <div style="display: flex; gap: 24px; align-items: flex-start; flex-wrap: wrap;">
# MAGIC     <!-- Left: Text -->
# MAGIC     <div style="flex: 0 0 34%; min-width: 280px; font-size: 12pt; line-height: 1.7;">
# MAGIC       <div style="margin-bottom: 10px; font-weight: 700;"><br>In this scenario, we have:</div>
# MAGIC       <ul style="margin: 0; padding-left: 20px;">
# MAGIC         <li>A <strong>target_table</strong> with users peter and zebi, both with status "current"</li><br>
# MAGIC         <li>A <strong>source_table</strong> with three rows indicating changes:
# MAGIC           <ul style="margin-top: 6px; padding-left: 20px;">
# MAGIC             <li><strong>peter</strong>: status = "delete" (remove from target)</li>
# MAGIC             <li><strong>zebi</strong>: status = "update" (update email to zebi@other.com)</li>
# MAGIC             <li><strong>samarth</strong>: status = "new" (insert as a new row)</li>
# MAGIC           </ul>
# MAGIC         </li>
# MAGIC       </ul>
# MAGIC       <div style="margin-top: 14px;">
# MAGIC         The goal is to update the <strong>target_table</strong> by applying all three types of changes in a single <strong>MERGE INTO</strong> operation.
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Right: Visual -->
# MAGIC     <div style="flex: 1; min-width: 500px;">
# MAGIC       <div style="display: flex; gap: 20px; align-items: flex-start;">
# MAGIC         <!-- Target Table -->
# MAGIC         <div style="flex: 1;">
# MAGIC           <div style="font-size: 14pt; font-weight: 700; margin-bottom: 10px;">target_table</div>
# MAGIC           <table style="width: 100%; border-collapse: collapse; font-size: 12pt;">
# MAGIC             <thead>
# MAGIC               <tr style="background: #1B5162; color: white;">
# MAGIC                 <th style="padding: 8px 10px; border: 1px solid #EEEDE9;">users</th>
# MAGIC                 <th style="padding: 8px 10px; border: 1px solid #EEEDE9;">email</th>
# MAGIC                 <th style="padding: 8px 10px; border: 1px solid #EEEDE9;">status</th>
# MAGIC               </tr>
# MAGIC             </thead>
# MAGIC             <tbody>
# MAGIC               <tr style="background: #F9F7F4;">
# MAGIC                 <td style="padding: 8px 10px; border: 1px solid #EEEDE9;">peter</td>
# MAGIC                 <td style="padding: 8px 10px; border: 1px solid #EEEDE9;">peter@email.com</td>
# MAGIC                 <td style="padding: 8px 10px; border: 1px solid #EEEDE9;">current</td>
# MAGIC               </tr>
# MAGIC               <tr>
# MAGIC                 <td style="padding: 8px 10px; border: 1px solid #EEEDE9;">zebi</td>
# MAGIC                 <td style="padding: 8px 10px; border: 1px solid #EEEDE9;">zebi@email.com</td>
# MAGIC                 <td style="padding: 8px 10px; border: 1px solid #EEEDE9;">current</td>
# MAGIC               </tr>
# MAGIC               <tr style="background: #F9F7F4;">
# MAGIC                 <td style="padding: 8px 10px; border: 1px solid #EEEDE9; color: #aaa;">...</td>
# MAGIC                 <td style="padding: 8px 10px; border: 1px solid #EEEDE9; color: #aaa;">...</td>
# MAGIC                 <td style="padding: 8px 10px; border: 1px solid #EEEDE9; color: #aaa;">...</td>
# MAGIC               </tr>
# MAGIC             </tbody>
# MAGIC           </table>
# MAGIC         </div>
# MAGIC         <!-- Arrow + label -->
# MAGIC         <div style="text-align: center; padding-top: 40px; min-width: 180px;">
# MAGIC           <svg width="120" height="34" viewBox="0 0 120 34" fill="none" xmlns="http://www.w3.org/2000/svg" style="display:block; margin: 0 auto 10px auto;">
# MAGIC             <polygon points="0,17 28,0 28,11 120,11 120,23 28,23 28,34" fill="#1a2e3b"/>
# MAGIC           </svg>
# MAGIC           <div style="font-size: 14pt; font-weight: 400; line-height: 1.6;">
# MAGIC             Update <strong>target_table</strong><br>with the <strong>source_table</strong>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC         <!-- Source Table -->
# MAGIC         <div style="flex: 1;">
# MAGIC           <div style="font-size: 14pt; font-weight: 700; margin-bottom: 10px;">source_table</div>
# MAGIC           <table style="width: 100%; border-collapse: collapse; font-size: 12pt;">
# MAGIC             <thead>
# MAGIC               <tr style="background: #1B5162; color: white;">
# MAGIC                 <th style="padding: 8px 10px; border: 1px solid #EEEDE9;">users</th>
# MAGIC                 <th style="padding: 8px 10px; border: 1px solid #EEEDE9;">email</th>
# MAGIC                 <th style="padding: 8px 10px; border: 1px solid #EEEDE9;">status</th>
# MAGIC               </tr>
# MAGIC             </thead>
# MAGIC             <tbody>
# MAGIC               <tr style="background: #FFF0F0;">
# MAGIC                 <td style="padding: 8px 10px; border: 1px solid #EEEDE9;">peter</td>
# MAGIC                 <td style="padding: 8px 10px; border: 1px solid #EEEDE9;">peter@email.com</td>
# MAGIC                 <td style="padding: 8px 10px; border: 1px solid #EEEDE9; color: #98102A; font-weight: 600;">delete</td>
# MAGIC               </tr>
# MAGIC               <tr style="background: #FFFFF0;">
# MAGIC                 <td style="padding: 8px 10px; border: 1px solid #EEEDE9;">zebi</td>
# MAGIC                 <td style="padding: 8px 10px; border: 1px solid #EEEDE9;">zebi@other.com</td>
# MAGIC                 <td style="padding: 8px 10px; border: 1px solid #EEEDE9; color: #FFAB00; font-weight: 600;">update</td>
# MAGIC               </tr>
# MAGIC               <tr style="background: #F0FFF0;">
# MAGIC                 <td style="padding: 8px 10px; border: 1px solid #EEEDE9;">samarth</td>
# MAGIC                 <td style="padding: 8px 10px; border: 1px solid #EEEDE9;">samarth@other.com</td>
# MAGIC                 <td style="padding: 8px 10px; border: 1px solid #EEEDE9; color: #00A972; font-weight: 600;">new</td>
# MAGIC               </tr>
# MAGIC             </tbody>
# MAGIC           </table>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <!-- Down arrow -->
# MAGIC <div style="margin-top: 16px; margin-bottom: 6px; display: flex; justify-content: flex-end; padding-right: 500px;">
# MAGIC   <svg width="38" height="48" viewBox="0 0 38 48" fill="none" xmlns="http://www.w3.org/2000/svg">
# MAGIC     <polygon points="19,48 0,27 11,27 11,0 27,0 27,27 38,27" fill="#4299E0"/>
# MAGIC   </svg>
# MAGIC </div>
# MAGIC       <!-- Result -->
# MAGIC       <div style="border: 2px dashed #4299E0; border-radius: 8px; padding: 16px; display: table; margin: 0 auto;">
# MAGIC         <div style="font-size: 14pt; font-weight: 700; margin-bottom: 10px;">target_table</div>
# MAGIC         <table style="border-collapse: collapse; font-size: 12pt; min-width: 480px;">
# MAGIC           <thead>
# MAGIC             <tr style="background: #1B5162; color: white;">
# MAGIC               <th style="padding: 8px 10px; border: 1px solid #EEEDE9;">users</th>
# MAGIC               <th style="padding: 8px 10px; border: 1px solid #EEEDE9;">email</th>
# MAGIC               <th style="padding: 8px 10px; border: 1px solid #EEEDE9;">status</th>
# MAGIC             </tr>
# MAGIC           </thead>
# MAGIC           <tbody>
# MAGIC             <tr style="background: #F9F7F4;">
# MAGIC               <td style="padding: 8px 10px; border: 1px solid #EEEDE9; font-weight: 600;">zebi</td>
# MAGIC               <td style="padding: 8px 10px; border: 1px solid #EEEDE9; font-weight: 600;">zebi@other.com</td>
# MAGIC               <td style="padding: 8px 10px; border: 1px solid #EEEDE9;">update</td>
# MAGIC             </tr>
# MAGIC             <tr>
# MAGIC               <td style="padding: 8px 10px; border: 1px solid #EEEDE9; font-weight: 600;">samarth</td>
# MAGIC               <td style="padding: 8px 10px; border: 1px solid #EEEDE9; font-weight: 600;">samarth@other.com</td>
# MAGIC               <td style="padding: 8px 10px; border: 1px solid #EEEDE9;">new</td>
# MAGIC             </tr>
# MAGIC             <tr style="background: #F9F7F4;">
# MAGIC               <td style="padding: 8px 10px; border: 1px solid #EEEDE9; color: #aaa;">...</td>
# MAGIC               <td style="padding: 8px 10px; border: 1px solid #EEEDE9; color: #aaa;">...</td>
# MAGIC               <td style="padding: 8px 10px; border: 1px solid #EEEDE9; color: #aaa;">...</td>
# MAGIC             </tr>
# MAGIC           </tbody>
# MAGIC         </table>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. MERGE INTO SQL Syntax

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width:1100px; margin:0 auto; font-family:'Segoe UI',sans-serif; color:#0b2026;">
# MAGIC
# MAGIC   <style>
# MAGIC     .demo-shell{
# MAGIC       display:grid;
# MAGIC       grid-template-columns: 250px 330px 450px;
# MAGIC       gap:16px;
# MAGIC       align-items:start;
# MAGIC     }
# MAGIC
# MAGIC     .panel{
# MAGIC       background:#F9F7F4;
# MAGIC       border:1.5px solid #e8e5e0;
# MAGIC       border-radius:12px;
# MAGIC       padding:14px;
# MAGIC       box-sizing:border-box;
# MAGIC       min-height:620px;
# MAGIC       overflow:hidden;
# MAGIC     }
# MAGIC
# MAGIC     .section-title{
# MAGIC       font-size:12pt;
# MAGIC       font-weight:800;
# MAGIC       margin-bottom:10px;
# MAGIC       color:#1b3139;
# MAGIC     }
# MAGIC
# MAGIC     .step{
# MAGIC       display:flex;
# MAGIC       gap:10px;
# MAGIC       align-items:flex-start;
# MAGIC       padding:10px;
# MAGIC       border-radius:10px;
# MAGIC       cursor:pointer;
# MAGIC       margin-bottom:8px;
# MAGIC       border:1.5px solid transparent;
# MAGIC       background:#fff;
# MAGIC       transition:.2s;
# MAGIC     }
# MAGIC     .step:hover{ box-shadow:0 2px 10px rgba(0,0,0,.08); }
# MAGIC     .step.active{
# MAGIC       border-color:#2574B5;
# MAGIC       box-shadow:0 2px 12px rgba(0,0,0,.08);
# MAGIC     }
# MAGIC     .step-num{
# MAGIC       width:28px;
# MAGIC       height:28px;
# MAGIC       border-radius:50%;
# MAGIC       background:#2574B5;
# MAGIC       color:#fff;
# MAGIC       font-size:12px;
# MAGIC       font-weight:800;
# MAGIC       display:flex;
# MAGIC       align-items:center;
# MAGIC       justify-content:center;
# MAGIC       flex-shrink:0;
# MAGIC     }
# MAGIC     .step-title{
# MAGIC       font-size:11pt;
# MAGIC       font-weight:700;
# MAGIC       line-height:1.25;
# MAGIC       color:#1b3139;
# MAGIC     }
# MAGIC     .step-sub{
# MAGIC       font-size:9pt;
# MAGIC       color:#7a7974;
# MAGIC       margin-top:2px;
# MAGIC       line-height:1.35;
# MAGIC     }
# MAGIC
# MAGIC     .middle-wrap{
# MAGIC       min-height:560px;
# MAGIC       display:flex;
# MAGIC       flex-direction:column;
# MAGIC       justify-content:flex-start;
# MAGIC     }
# MAGIC     .table-block{
# MAGIC       margin-bottom:12px;
# MAGIC     }
# MAGIC     .table-title{
# MAGIC       font-size:11pt;
# MAGIC       font-weight:700;
# MAGIC       margin-bottom:6px;
# MAGIC       color:#1b3139;
# MAGIC     }
# MAGIC     .tbl{
# MAGIC       width:100%;
# MAGIC       border-collapse:collapse;
# MAGIC       font-size:9pt;
# MAGIC       background:#fff;
# MAGIC       border-radius:8px;
# MAGIC       overflow:hidden;
# MAGIC       table-layout:fixed;
# MAGIC     }
# MAGIC     .tbl th{
# MAGIC       background:#1B5162;
# MAGIC       color:#fff;
# MAGIC       text-align:left;
# MAGIC       padding:6px 8px;
# MAGIC       font-size:8.5pt;
# MAGIC       border:1px solid #EEEDE9;
# MAGIC     }
# MAGIC     .tbl td{
# MAGIC       padding:6px 8px;
# MAGIC       border:1px solid #EEEDE9;
# MAGIC       word-break:break-word;
# MAGIC     }
# MAGIC     .row-soft{ background:#F9F7F4; }
# MAGIC     .muted{ color:#aaa; }
# MAGIC     .delete-row td{
# MAGIC       text-decoration:line-through;
# MAGIC       color:#7a7a7a;
# MAGIC       background:#FFF0F0;
# MAGIC     }
# MAGIC     .update-row td{
# MAGIC       background:#FFFFF0;
# MAGIC     }
# MAGIC     .new-row td{
# MAGIC       background:#F0FFF0;
# MAGIC     }
# MAGIC     .arrow-down{
# MAGIC       display:flex;
# MAGIC       justify-content:center;
# MAGIC       margin:4px 0 10px;
# MAGIC     }
# MAGIC
# MAGIC     .code-wrap{
# MAGIC       background:#fff;
# MAGIC       border:1.5px solid #e0e0e0;
# MAGIC       border-radius:10px;
# MAGIC       overflow:hidden;
# MAGIC     }
# MAGIC     .code-head{
# MAGIC       display:flex;
# MAGIC       justify-content:space-between;
# MAGIC       align-items:center;
# MAGIC       padding:8px 10px;
# MAGIC       background:#f3f4f6;
# MAGIC       border-bottom:1px solid #e5e7eb;
# MAGIC       font-size:9pt;
# MAGIC       color:#5A6F77;
# MAGIC       font-weight:700;
# MAGIC     }
# MAGIC     .copy-btn{
# MAGIC       padding:4px 10px;
# MAGIC       font-size:12px;
# MAGIC       background:#ddd;
# MAGIC       color:#333;
# MAGIC       border:1px solid #ccc;
# MAGIC       border-radius:4px;
# MAGIC       cursor:pointer;
# MAGIC     }
# MAGIC     pre{
# MAGIC       margin:0;
# MAGIC       padding:14px 16px;
# MAGIC       background:#f8f8f8;
# MAGIC       overflow-x:auto;
# MAGIC       font-family:Consolas, Monaco, monospace;
# MAGIC       font-size:12.5px;
# MAGIC       line-height:1.6;
# MAGIC       white-space:pre;
# MAGIC       min-height:360px;
# MAGIC       box-sizing:border-box;
# MAGIC     }
# MAGIC     .hint{
# MAGIC       margin-top:10px;
# MAGIC       padding:10px 12px;
# MAGIC       border-left:4px solid #4299E0;
# MAGIC       background:rgba(66,153,224,0.08);
# MAGIC       border-radius:6px;
# MAGIC       font-size:9.5pt;
# MAGIC       line-height:1.5;
# MAGIC     }
# MAGIC   </style>
# MAGIC
# MAGIC   <div class="demo-shell">
# MAGIC     <!-- Left -->
# MAGIC     <div class="panel">
# MAGIC       <div class="section-title">Steps</div>
# MAGIC       <div class="step active" data-step="1">
# MAGIC         <div class="step-num">1</div>
# MAGIC         <div>
# MAGIC           <div class="step-title">Declare Target and Source Table</div>
# MAGIC           <div class="step-sub">MERGE INTO target_table target USING source_table source</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div class="step" data-step="2">
# MAGIC         <div class="step-num">2</div>
# MAGIC         <div>
# MAGIC           <div class="step-title">Specify the condition for merging</div>
# MAGIC           <div class="step-sub">ON target.id = source.id</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div class="step" data-step="3">
# MAGIC         <div class="step-num">3</div>
# MAGIC         <div>
# MAGIC           <div class="step-title">First WHEN clause for merge</div>
# MAGIC           <div class="step-sub">WHEN MATCHED AND source.status = 'update'</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div class="step" data-step="4">
# MAGIC         <div class="step-num">4</div>
# MAGIC         <div>
# MAGIC           <div class="step-title">Second WHEN clause for merge</div>
# MAGIC           <div class="step-sub">WHEN MATCHED AND source.status = 'delete'</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div class="step" data-step="5">
# MAGIC         <div class="step-num">5</div>
# MAGIC         <div>
# MAGIC           <div class="step-title">WHEN NOT MATCHED clause for merge</div>
# MAGIC           <div class="step-sub">Insert rows not present in target</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div class="step" data-step="6">
# MAGIC         <div class="step-num">6</div>
# MAGIC         <div>
# MAGIC           <div class="step-title">Final Results</div>
# MAGIC           <div class="step-sub">Fully Updated target_table</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Middle -->
# MAGIC     <div class="panel">
# MAGIC       <div class="section-title">Tables</div>
# MAGIC       <div id="middle-content" class="middle-wrap"></div>
# MAGIC     </div>
# MAGIC     <!-- Right -->
# MAGIC     <div class="panel">
# MAGIC       <div class="section-title">SQL</div>
# MAGIC       <div class="code-wrap">
# MAGIC         <div class="code-head">
# MAGIC           <span>MERGE INTO</span>
# MAGIC           <button class="copy-btn" id="copyBtn">Copy</button>
# MAGIC         </div>
# MAGIC         <pre id="codeBlock" contenteditable="true"></pre>
# MAGIC       </div>
# MAGIC       <div class="hint" id="stepHint"></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <script>
# MAGIC     const sqlLines = [
# MAGIC       "MERGE INTO target_table target",
# MAGIC       "USING source_table source",
# MAGIC       "ON target.id = source.id",
# MAGIC       "WHEN MATCHED AND source.status = 'update' THEN",
# MAGIC       "  UPDATE SET",
# MAGIC       "    target.email = source.email,",
# MAGIC       "    target.status = source.status",
# MAGIC       "WHEN MATCHED AND source.status = 'delete' THEN",
# MAGIC       "  DELETE",
# MAGIC       "WHEN NOT MATCHED THEN",
# MAGIC       "  INSERT (id, first_name, email, sign_up_date", 
# MAGIC       "  status)",
# MAGIC       "  VALUES (source.id, source.first_name", 
# MAGIC       "  source.email, source.sign_up_date", 
# MAGIC       "  source.status);",
# MAGIC     ];
# MAGIC
# MAGIC     /* cumulative SQL reveal */
# MAGIC     const stepToLineCount = {
# MAGIC       1: 2,
# MAGIC       2: 3,
# MAGIC       3: 7,
# MAGIC       4: 9,
# MAGIC       5: 12,
# MAGIC       6: 12
# MAGIC     };
# MAGIC
# MAGIC     const hints = {
# MAGIC       1: "Declare the target and source tables used in the MERGE INTO statement.",
# MAGIC       2: "Define the matching key between source and target with ON target.id = source.id.",
# MAGIC       3: "Add the first WHEN MATCHED clause for updates.",
# MAGIC       4: "Add the second WHEN MATCHED clause for deletes.",
# MAGIC       5: "Add the WHEN NOT MATCHED clause for inserts.",
# MAGIC       6: "Review the final updated target_table."
# MAGIC     };
# MAGIC
# MAGIC     const middleViews = {
# MAGIC       1: `
# MAGIC         <div class="table-block">
# MAGIC           <div class="table-title">source_table</div>
# MAGIC           <table class="tbl">
# MAGIC             <thead><tr><th>id</th><th>users</th><th>email</th><th>status</th></tr></thead>
# MAGIC             <tbody>
# MAGIC               <tr class="row-soft"><td>1</td><td>peter</td><td>peter@email</td><td>delete</td></tr>
# MAGIC               <tr><td>2</td><td>zebi</td><td>zebi@email</td><td>update</td></tr>
# MAGIC               <tr class="row-soft"><td>3</td><td>samarth</td><td>samarth@email</td><td>new</td></tr>
# MAGIC             </tbody>
# MAGIC           </table>
# MAGIC         </div>
# MAGIC
# MAGIC         <div class="arrow-down">
# MAGIC           <svg width="38" height="48" viewBox="0 0 38 48" fill="none" xmlns="http://www.w3.org/2000/svg">
# MAGIC             <polygon points="19,48 0,27 11,27 11,0 27,0 27,27 38,27" fill="#1a2e3b"/>
# MAGIC           </svg>
# MAGIC         </div>
# MAGIC
# MAGIC         <div class="table-block">
# MAGIC           <div class="table-title">Original target_table</div>
# MAGIC           <table class="tbl">
# MAGIC             <thead><tr><th>id</th><th>users</th><th>email</th><th>status</th></tr></thead>
# MAGIC             <tbody>
# MAGIC               <tr class="row-soft"><td>1</td><td>peter</td><td>peter@email</td><td>current</td></tr>
# MAGIC               <tr><td>2</td><td>zebi</td><td>zebi@email</td><td>current</td></tr>
# MAGIC               <tr class="row-soft"><td>4</td><td>matt</td><td>matt@email</td><td>current</td></tr>
# MAGIC             </tbody>
# MAGIC           </table>
# MAGIC         </div>
# MAGIC       `,
# MAGIC       2: `
# MAGIC         <div class="table-block">
# MAGIC           <div class="table-title">source_table</div>
# MAGIC           <table class="tbl">
# MAGIC             <thead><tr><th>id</th><th>users</th><th>email</th><th>status</th></tr></thead>
# MAGIC             <tbody>
# MAGIC               <tr class="row-soft"><td>1</td><td>peter</td><td>peter@email</td><td>delete</td></tr>
# MAGIC               <tr><td>2</td><td>zebi</td><td>zebi@email</td><td>update</td></tr>
# MAGIC               <tr class="row-soft"><td>3</td><td>samarth</td><td>samarth@email</td><td>new</td></tr>
# MAGIC             </tbody>
# MAGIC           </table>
# MAGIC         </div>
# MAGIC
# MAGIC         <div class="arrow-down">
# MAGIC           <svg width="38" height="48" viewBox="0 0 38 48" fill="none" xmlns="http://www.w3.org/2000/svg">
# MAGIC             <polygon points="19,48 0,27 11,27 11,0 27,0 27,27 38,27" fill="#1a2e3b"/>
# MAGIC           </svg>
# MAGIC         </div>
# MAGIC
# MAGIC         <div class="table-block">
# MAGIC           <div class="table-title">Updated target_table</div>
# MAGIC           <table class="tbl">
# MAGIC             <thead><tr><th>id</th><th>users</th><th>email</th><th>status</th></tr></thead>
# MAGIC             <tbody>
# MAGIC               <tr class="row-soft"><td>1</td><td>peter</td><td>peter@email</td><td>current</td></tr>
# MAGIC               <tr><td>2</td><td>zebi</td><td>zebi@email</td><td>current</td></tr>
# MAGIC               <tr class="row-soft"><td>4</td><td>matt</td><td>matt@email</td><td>current</td></tr>
# MAGIC             </tbody>
# MAGIC           </table>
# MAGIC         </div>
# MAGIC       `,
# MAGIC       3: `
# MAGIC         <div class="table-block">
# MAGIC           <div class="table-title">source_table</div>
# MAGIC           <table class="tbl">
# MAGIC             <thead><tr><th>id</th><th>users</th><th>email</th><th>status</th></tr></thead>
# MAGIC             <tbody>
# MAGIC               <tr class="row-soft"><td>1</td><td>peter</td><td>peter@email</td><td>delete</td></tr>
# MAGIC               <tr class="update-row"><td>2</td><td>zebi</td><td>zebi@email</td><td>update</td></tr>
# MAGIC               <tr class="row-soft"><td>3</td><td>samarth</td><td>samarth@email</td><td>new</td></tr>
# MAGIC             </tbody>
# MAGIC           </table>
# MAGIC         </div>
# MAGIC
# MAGIC         <div class="arrow-down">
# MAGIC           <svg width="38" height="48" viewBox="0 0 38 48" fill="none" xmlns="http://www.w3.org/2000/svg">
# MAGIC             <polygon points="19,48 0,27 11,27 11,0 27,0 27,27 38,27" fill="#1a2e3b"/>
# MAGIC           </svg>
# MAGIC         </div>
# MAGIC
# MAGIC         <div class="table-block">
# MAGIC           <div class="table-title">Updated target_table</div>
# MAGIC           <table class="tbl">
# MAGIC             <thead><tr><th>id</th><th>users</th><th>email</th><th>status</th></tr></thead>
# MAGIC             <tbody>
# MAGIC               <tr class="row-soft"><td>1</td><td>peter</td><td>peter@email</td><td>current</td></tr>
# MAGIC               <tr class="update-row"><td>2</td><td>zebi</td><td>zebi@email</td><td>update</td></tr>
# MAGIC               <tr class="row-soft"><td>4</td><td>matt</td><td>matt@email</td><td>current</td></tr>
# MAGIC             </tbody>
# MAGIC           </table>
# MAGIC         </div>
# MAGIC       `,
# MAGIC       4: `
# MAGIC         <div class="table-block">
# MAGIC           <div class="table-title">source_table</div>
# MAGIC           <table class="tbl">
# MAGIC             <thead><tr><th>id</th><th>users</th><th>email</th><th>status</th></tr></thead>
# MAGIC             <tbody>
# MAGIC               <tr class="delete-row"><td>1</td><td>peter</td><td>peter@email</td><td>delete</td></tr>
# MAGIC               <tr class="update-row"><td>2</td><td>zebi</td><td>zebi@email</td><td>update</td></tr>
# MAGIC               <tr class="row-soft"><td>3</td><td>samarth</td><td>samarth@email</td><td>new</td></tr>
# MAGIC             </tbody>
# MAGIC           </table>
# MAGIC         </div>
# MAGIC
# MAGIC         <div class="arrow-down">
# MAGIC           <svg width="38" height="48" viewBox="0 0 38 48" fill="none" xmlns="http://www.w3.org/2000/svg">
# MAGIC             <polygon points="19,48 0,27 11,27 11,0 27,0 27,27 38,27" fill="#1a2e3b"/>
# MAGIC           </svg>
# MAGIC         </div>
# MAGIC
# MAGIC         <div class="table-block">
# MAGIC           <div class="table-title">Updated target_table</div>
# MAGIC           <table class="tbl">
# MAGIC             <thead><tr><th>id</th><th>users</th><th>email</th><th>status</th></tr></thead>
# MAGIC             <tbody>
# MAGIC               <tr class="delete-row"><td>1</td><td>peter</td><td>peter@email</td><td>current</td></tr>
# MAGIC               <tr class="update-row"><td>2</td><td>zebi</td><td>zebi@email</td><td>update</td></tr>
# MAGIC               <tr class="row-soft"><td>4</td><td>matt</td><td>matt@email</td><td>current</td></tr>
# MAGIC             </tbody>
# MAGIC           </table>
# MAGIC         </div>
# MAGIC       `,
# MAGIC       5: `
# MAGIC         <div class="table-block">
# MAGIC           <div class="table-title">source_table</div>
# MAGIC           <table class="tbl">
# MAGIC             <thead><tr><th>id</th><th>users</th><th>email</th><th>status</th></tr></thead>
# MAGIC             <tbody>
# MAGIC               <tr class="delete-row"><td>1</td><td>peter</td><td>peter@email</td><td>delete</td></tr>
# MAGIC               <tr class="update-row"><td>2</td><td>zebi</td><td>zebi@email</td><td>update</td></tr>
# MAGIC               <tr class="new-row"><td>3</td><td>samarth</td><td>samarth@email</td><td>new</td></tr>
# MAGIC             </tbody>
# MAGIC           </table>
# MAGIC         </div>
# MAGIC
# MAGIC         <div class="arrow-down">
# MAGIC           <svg width="38" height="48" viewBox="0 0 38 48" fill="none" xmlns="http://www.w3.org/2000/svg">
# MAGIC             <polygon points="19,48 0,27 11,27 11,0 27,0 27,27 38,27" fill="#1a2e3b"/>
# MAGIC           </svg>
# MAGIC         </div>
# MAGIC
# MAGIC         <div class="table-block">
# MAGIC           <div class="table-title">Updated target_table</div>
# MAGIC           <table class="tbl">
# MAGIC             <thead><tr><th>id</th><th>users</th><th>email</th><th>status</th></tr></thead>
# MAGIC             <tbody>
# MAGIC               <tr class="update-row"><td>2</td><td>zebi</td><td>zebi@email</td><td>update</td></tr>
# MAGIC               <tr class="row-soft"><td>4</td><td>matt</td><td>matt@email</td><td>current</td></tr>
# MAGIC               <tr class="new-row"><td>3</td><td>samarth</td><td>samarth@email</td><td>new</td></tr>
# MAGIC             </tbody>
# MAGIC           </table>
# MAGIC         </div>
# MAGIC       `,
# MAGIC       6: `
# MAGIC         <div class="table-block">
# MAGIC           <div class="table-title">source_table</div>
# MAGIC           <table class="tbl">
# MAGIC             <thead><tr><th>id</th><th>users</th><th>email</th><th>status</th></tr></thead>
# MAGIC             <tbody>
# MAGIC               <tr class="row-soft"><td>1</td><td>peter</td><td>peter@email</td><td>delete</td></tr>
# MAGIC               <tr><td>2</td><td>zebi</td><td>zebi@email</td><td>update</td></tr>
# MAGIC               <tr class="row-soft"><td>3</td><td>samarth</td><td>samarth@email</td><td>new</td></tr>
# MAGIC             </tbody>
# MAGIC           </table>
# MAGIC         </div>
# MAGIC
# MAGIC         <div class="arrow-down">
# MAGIC           <svg width="38" height="48" viewBox="0 0 38 48" fill="none" xmlns="http://www.w3.org/2000/svg">
# MAGIC             <polygon points="19,48 0,27 11,27 11,0 27,0 27,27 38,27" fill="#4299E0"/>
# MAGIC           </svg>
# MAGIC         </div>
# MAGIC
# MAGIC         <div class="table-block">
# MAGIC           <div class="table-title">Fully Updated target_table</div>
# MAGIC           <table class="tbl">
# MAGIC             <thead><tr><th>id</th><th>users</th><th>email</th><th>status</th></tr></thead>
# MAGIC             <tbody>
# MAGIC               <tr class="row-soft"><td>2</td><td>zebi</td><td>zebi@email</td><td>current</td></tr>
# MAGIC               <tr><td>4</td><td>matt</td><td>matt@email</td><td>current</td></tr>
# MAGIC               <tr class="row-soft"><td>3</td><td>samarth</td><td>samarth@email</td><td>new</td></tr>
# MAGIC             </tbody>
# MAGIC           </table>
# MAGIC         </div>
# MAGIC       `
# MAGIC     };
# MAGIC
# MAGIC     function renderStep(step) {
# MAGIC       document.querySelectorAll(".step").forEach(el => el.classList.remove("active"));
# MAGIC       document.querySelector('.step[data-step="' + step + '"]').classList.add("active");
# MAGIC
# MAGIC       const lineCount = stepToLineCount[step];
# MAGIC       document.getElementById("codeBlock").textContent = sqlLines.slice(0, lineCount).join("\n");
# MAGIC       document.getElementById("middle-content").innerHTML = middleViews[step];
# MAGIC       document.getElementById("stepHint").textContent = hints[step];
# MAGIC     }
# MAGIC
# MAGIC     document.querySelectorAll(".step").forEach(el => {
# MAGIC       el.addEventListener("click", () => renderStep(Number(el.dataset.step)));
# MAGIC     });
# MAGIC
# MAGIC     document.getElementById("copyBtn").addEventListener("click", function() {
# MAGIC       const text = document.getElementById("codeBlock").textContent;
# MAGIC       const t = document.createElement("textarea");
# MAGIC       t.value = text;
# MAGIC       document.body.appendChild(t);
# MAGIC       t.select();
# MAGIC       document.execCommand("copy");
# MAGIC       document.body.removeChild(t);
# MAGIC       this.textContent = "✓ Copied!";
# MAGIC       setTimeout(() => this.textContent = "Copy", 2000);
# MAGIC     });
# MAGIC
# MAGIC     renderStep(1);
# MAGIC   </script>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC <ol>
# MAGIC   <li><strong>Step 1: Declare Target and Source Table</strong></br>
# MAGIC   <ul>
# MAGIC   <li>MERGE INTO <code>target_table</code> target - This declares the target table, the table you want to update, insert into, or delete from. You're giving it the alias target to reference it later in the query.</li>
# MAGIC   <li>USING <code>source_table</code> source - This defines the source table, the table that contains the new data or changes you want to apply to the target. It's given the alias source.</li>
# MAGIC   </ul>
# MAGIC   </li>
# MAGIC
# MAGIC   <br>
# MAGIC   <li><strong>Step 2: Specify the Merge Condition</strong><br>
# MAGIC   <code>ON target.id = source.id</code> defines the matching condition between the source and target tables, typically using a unique key like <code>id</code>.
# MAGIC   </li>
# MAGIC
# MAGIC   <br>
# MAGIC   <li><strong>Step 3: WHEN MATCHED - UPDATE</strong><br>
# MAGIC   WHEN MATCHED AND source.status = 'update' THEN
# MAGIC   
# MAGIC   If a row exists in both tables and the source indicates it's an update.
# MAGIC   
# MAGIC   UPDATE SET  - Updates the target table with new values from the source:
# MAGIC   <ul>
# MAGIC   <li>email</li>
# MAGIC   <li>status</li>
# MAGIC   </ul>
# MAGIC   </li>
# MAGIC   <br>
# MAGIC   <li><strong>Step 4: WHEN MATCHED - DELETE</strong><br>
# MAGIC   WHEN MATCHED AND source.status = 'delete' THEN DELETE
# MAGIC   
# MAGIC   If the row exists in both tables and the source says it should be deleted, it’s removed from the target.
# MAGIC
# MAGIC   <br>
# MAGIC   <li><strong>Step 5: WHEN NOT MATCHED - INSERT</strong><br>
# MAGIC   If the row exists in the source but not in the target...
# MAGIC   
# MAGIC   INSERT (...) VALUES (...) - A new row is inserted into the target table with values from the source.
# MAGIC
# MAGIC   <br>
# MAGIC   <li><strong>Step 6: Final Result</strong><br>
# MAGIC   This single MERGE INTO command:
# MAGIC   <ul>
# MAGIC   <li>Updates rows when status = 'update'</li>
# MAGIC   <li>Deletes rows when status = 'delete'</li>
# MAGIC   <li>Inserts new rows not present in the target</li>
# MAGIC   </ul>
# MAGIC   <br>
# MAGIC   This pattern is perfect for incremental data loads, CDC, or SCD Type 1 patterns. Matt's row (id=4) was not in the source table, so it remains unchanged in the target.
# MAGIC   </li>
# MAGIC </ol>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Conclusion
# MAGIC
# MAGIC - Databricks supports broader integration and collaboration through Lakehouse Federation, Zerobus, Delta Sharing, and Databricks Marketplace.
# MAGIC - Databricks Marketplace and Delta Sharing enable secure discovery, access, and exchange of data and AI assets without unnecessary data copying.
# MAGIC - `MERGE INTO` performs atomic updates, inserts, and deletes in Delta tables, supporting incremental ingestion patterns such as SCD, CDC, and data synchronization.
# MAGIC

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