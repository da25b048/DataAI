# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create catalog
# MAGIC CREATE CATALOG IF NOT EXISTS bricksiitm
# MAGIC COMMENT 'BNS Legal Assistant Project';
# MAGIC
# MAGIC -- Use the catalog
# MAGIC USE CATALOG bricksiitm;
# MAGIC
# MAGIC -- Create schema (database)
# MAGIC CREATE SCHEMA IF NOT EXISTS bns_legal
# MAGIC COMMENT 'BNS 2023 Legal Data';
# MAGIC
# MAGIC -- Use the schema
# MAGIC USE SCHEMA bns_legal;
# MAGIC
# MAGIC -- Create volume for raw files
# MAGIC CREATE VOLUME IF NOT EXISTS bricksiitm.bns_legal.raw_files
# MAGIC COMMENT 'Raw PDF and legal documents';
# MAGIC
# MAGIC -- Verify everything is created
# MAGIC SHOW SCHEMAS;
# MAGIC
# MAGIC -- List volume contents (should be empty for now)
# MAGIC LIST '/Volumes/bricksiitm/bns_legal/raw_files';

# COMMAND ----------

