# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create analytics views for Genie and AI/BI Dashboard
# MAGIC -- This is a SQL notebook
# MAGIC
# MAGIC USE CATALOG bricksiitm;
# MAGIC USE SCHEMA bns_legal;
# MAGIC
# MAGIC -- View 1: Clean sections for Genie
# MAGIC CREATE OR REPLACE VIEW vw_bns_sections AS
# MAGIC SELECT 
# MAGIC     section_number,
# MAGIC     LEFT(section_text, 800) as section_text,
# MAGIC     LENGTH(section_text) as text_length
# MAGIC FROM bns_sections
# MAGIC WHERE section_text IS NOT NULL
# MAGIC ORDER BY section_number;
# MAGIC
# MAGIC -- View 2: Statistics for dashboard
# MAGIC CREATE OR REPLACE VIEW vw_bns_stats AS
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_sections,
# MAGIC     SUM(LENGTH(section_text)) as total_characters,
# MAGIC     ROUND(AVG(LENGTH(section_text)), 0) as avg_section_length
# MAGIC FROM bns_sections;
# MAGIC
# MAGIC -- View 3: Chunks summary for Genie
# MAGIC CREATE OR REPLACE VIEW vw_bns_chunks AS
# MAGIC SELECT 
# MAGIC     chunk_id,
# MAGIC     section_number,
# MAGIC     LEFT(chunk_text, 300) as chunk_preview,
# MAGIC     LENGTH(chunk_text) as chunk_length
# MAGIC FROM bns_chunks
# MAGIC LIMIT 1000;
# MAGIC
# MAGIC -- Verify all views
# MAGIC SHOW VIEWS;

# COMMAND ----------

# Update requirements.txt with all dependencies
updated_requirements = """
streamlit==1.35.0
pandas==2.0.3
numpy==1.24.3
sentence-transformers==2.2.2
faiss-cpu==1.7.4
torch==2.0.1
transformers==4.35.0
"""

# Overwrite requirements.txt
dbutils.fs.put("/Workspace/Users/da25b048@smail.iitm.ac.in/DataAI/requirements.txt", updated_requirements, overwrite=True)

print("✅ requirements.txt updated!")

# COMMAND ----------

