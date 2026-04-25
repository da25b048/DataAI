# Databricks notebook source
# Run this in Databricks to create app files
import os

app_code = '''
import streamlit as st
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
import faiss
import re

st.set_page_config(page_title="BNS Legal Assistant", page_icon="⚖️")

st.title("⚖️ BNS Legal Assistant")
st.caption("Bharatiya Nyaya Sanhita 2023")

# Simple chat
if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "assistant", "content": "Ask me about BNS - punishment for rape, theft, murder?"}]

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

if prompt := st.chat_input("Ask your question..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    with st.chat_message("assistant"):
        response = f"**Based on BNS 2023:**\\n\\nI found information about: *{prompt}*\\n\\n⚠️ For complete responses, see the main notebook with FAISS vector search."
        st.markdown(response)
        st.session_state.messages.append({"role": "assistant", "content": response})
'''

requirements = "streamlit==1.35.0\npandas\nnumpy"

yaml_content = """
command:
  - streamlit
  - run
  - app.py
"""

# Create files
with open("/Workspace/Users/da25b048@smail.iitm.ac.in/DataAI/app.py", "w") as f:
    f.write(app_code)

with open("/Workspace/Users/da25b048@smail.iitm.ac.in/DataAI/requirements.txt", "w") as f:
    f.write(requirements)

with open("/Workspace/Users/da25b048@smail.iitm.ac.in/DataAI/app.yaml", "w") as f:
    f.write(yaml_content)

print("✅ App files created!")

# COMMAND ----------

