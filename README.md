# ⚖️ BNS Legal Assistant

An AI-powered legal assistant for the **Bharatiya Nyaya Sanhita (BNS) 2023** - India's new criminal code. Ask questions in plain English and get instant answers about criminal offenses, punishments, and legal definitions.

## Hackathon
Built for BharatBricks Hackathon 2026 at IIT Madras

Track: Nyaya-Sahayak (Governance & Access to Justice)

## 🎥 Demo Video: In google drive submission

## ✨ Features

- 🔍 Ask questions about BNS offenses (theft, murder, rape, robbery, etc.)
- ⚖️ Get accurate BNS section numbers and punishments
- 💬 Simple chat interface
- 🚀 Deployed as a Streamlit app on Databricks
- 📚 Built with RAG + FAISS vector search

## 🏗️ Architecture
User Question → Keyword Matching → BNS Section → Answer Response <br>
↓<br>
Databricks Free Edition<br>
↓<br>
FAISS Vector Search (455+ chunks)<br>
↓<br>
Sentence Transformers (Embeddings)<br>
↓<br><br><br>
Made with ❤️ for BharatBricks Hackathon 2026
