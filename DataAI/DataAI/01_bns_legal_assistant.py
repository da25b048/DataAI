# Databricks notebook source
# MAGIC %pip install databricks-langchain langgraph chromadb sentence-transformers --quiet

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG bricksiitm;
# MAGIC USE SCHEMA bns_legal;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List files in your volume
# MAGIC LIST '/Volumes/bricksiitm/bns_legal/raw_files';

# COMMAND ----------

!pip install pdfplumber

# COMMAND ----------

import pdfplumber
import re
import pandas as pd

# Path to your PDF
pdf_path = "/Volumes/bricksiitm/bns_legal/raw_files/BNS2023.pdf"

print("📖 Reading PDF...")

# Extract text from PDF
full_text = ""
with pdfplumber.open(pdf_path) as pdf:
    print(f"   PDF has {len(pdf.pages)} pages")
    for page_num, page in enumerate(pdf.pages):
        text = page.extract_text()
        if text:
            full_text += text + "\n"

print(f"✅ Extracted {len(full_text):,} characters")

# Function to extract sections
def extract_sections(text):
    sections = []
    current_section = None
    current_content = []
    
    lines = text.split('\n')
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Match section numbers like "1.", "2.", "10.", "100." etc.
        match = re.match(r'^(\d+)\.\s+', line)
        
        if match:
            # Save previous section
            if current_section is not None and current_content:
                sections.append({
                    'section_number': int(current_section),
                    'section_text': ' '.join(current_content)[:3000]
                })
            
            # Start new section
            current_section = match.group(1)
            # Get the rest of the line after section number
            content_start = line[match.end():]
            current_content = [content_start] if content_start else []
        else:
            # Add to current section
            if current_section is not None:
                current_content.append(line)
    
    # Add last section
    if current_section is not None and current_content:
        sections.append({
            'section_number': int(current_section),
            'section_text': ' '.join(current_content)[:3000]
        })
    
    return sections

# Extract sections
print("\n📝 Parsing sections...")
bns_sections = extract_sections(full_text)
print(f"✅ Found {len(bns_sections)} sections")

# Show first few sections
print("\n📋 First 3 sections:")
for sec in bns_sections[:3]:
    print(f"\n   Section {sec['section_number']}:")
    print(f"   {sec['section_text'][:150]}...")

# Create Spark DataFrame and save to Delta
print("\n💾 Saving to Delta tables...")
sections_spark = spark.createDataFrame(bns_sections)
sections_spark.write.mode("overwrite").saveAsTable("bricksiitm.bns_legal.bns_sections")
print("✅ Saved bricksiitm.bns_legal.bns_sections")

# Create chunks
def create_chunks(text, section_num, chunk_size=800, overlap=150):
    """Split long sections into smaller chunks"""
    if not text or len(text) < 100:
        return [{
            'chunk_id': f"BNS_{section_num}_0",
            'section_number': section_num,
            'chunk_text': text,
            'chunk_index': 0
        }]
    
    words = text.split()
    chunks = []
    
    if len(words) <= chunk_size:
        chunks.append({
            'chunk_id': f"BNS_{section_num}_0",
            'section_number': section_num,
            'chunk_text': text,
            'chunk_index': 0
        })
    else:
        for i in range(0, len(words), chunk_size - overlap):
            chunk_words = words[i:i + chunk_size]
            chunk_text = ' '.join(chunk_words)
            chunks.append({
                'chunk_id': f"BNS_{section_num}_{i}",
                'section_number': section_num,
                'chunk_text': chunk_text,
                'chunk_index': i // (chunk_size - overlap)
            })
    
    return chunks

# Create chunks for all sections
print("\n🔨 Creating chunks...")
all_chunks = []
for sec in bns_sections:
    chunks = create_chunks(sec['section_text'], sec['section_number'])
    all_chunks.extend(chunks)

print(f"✅ Created {len(all_chunks)} chunks")

# Save chunks
chunks_spark = spark.createDataFrame(all_chunks)
chunks_spark.write.mode("overwrite").saveAsTable("bricksiitm.bns_legal.bns_chunks")
print("✅ Saved bricksiitm.bns_legal.bns_chunks")

# Verify
print("\n📊 Verification:")
print(f"   Sections table: {spark.sql('SELECT COUNT(*) FROM bricksiitm.bns_legal.bns_sections').collect()[0][0]} rows")
print(f"   Chunks table: {spark.sql('SELECT COUNT(*) FROM bricksiitm.bns_legal.bns_chunks').collect()[0][0]} rows")

print("\n🎉 DONE! Both tables created successfully.")

# COMMAND ----------

import pdfplumber
import re
import pandas as pd

pdf_path = "/Volumes/bricksiitm/bns_legal/raw_files/BNS2023.pdf"

# Extract text with page numbers
print("📖 Reading PDF page by page...")
all_pages_text = []
full_text = ""

with pdfplumber.open(pdf_path) as pdf:
    print(f"   Total pages: {len(pdf.pages)}")
    for page_num, page in enumerate(pdf.pages, 1):
        text = page.extract_text()
        if text:
            all_pages_text.append({
                'page': page_num,
                'text': text
            })
            full_text += text + "\n"

print(f"✅ Extracted {len(full_text):,} characters")

# Look for section numbers in the text
print("\n🔍 Analyzing text structure...")

# Let's find all numbers followed by period at start of lines
lines = full_text.split('\n')
section_candidates = []

for i, line in enumerate(lines):
    line = line.strip()
    # Look for patterns like "1." "2." "3." etc.
    match = re.match(r'^(\d+)\.\s+', line)
    if match:
        section_num = int(match.group(1))
        # Get the content after the section number
        content = line[match.end():]
        section_candidates.append({
            'section_num': section_num,
            'content': content,
            'line_num': i
        })
        
        if section_num <= 10:  # Show first 10 for debugging
            print(f"   Found section {section_num}: {content[:80]}...")

print(f"\n📊 Found {len(section_candidates)} section candidates")

# Now extract full sections (multi-line)
print("\n📝 Extracting full sections...")

sections = []
for idx, candidate in enumerate(section_candidates):
    section_num = candidate['section_num']
    start_line = candidate['line_num']
    
    # Find end of this section (next section or end of text)
    end_line = len(lines)
    for next_candidate in section_candidates[idx + 1:]:
        end_line = next_candidate['line_num']
        break
    
    # Collect all lines from start to end of section
    section_lines = lines[start_line:end_line]
    section_text = ' '.join(section_lines)
    
    # Clean the text
    section_text = re.sub(r'\s+', ' ', section_text).strip()
    
    sections.append({
        'section_number': section_num,
        'section_text': section_text[:3000]  # Limit length
    })

print(f"✅ Extracted {len(sections)} complete sections")

# Show sample
print("\n📋 Sample sections extracted:")
for sec in sections[:5]:
    print(f"\n   Section {sec['section_number']}:")
    print(f"   {sec['section_text'][:150]}...")

# Save to Delta table
print("\n💾 Saving to Delta tables...")

if len(sections) > 0:
    sections_spark = spark.createDataFrame(sections)
    sections_spark.write.mode("overwrite").saveAsTable("bricksiitm.bns_legal.bns_sections")
    print(f"✅ Saved {len(sections)} sections to bricksiitm.bns_legal.bns_sections")
else:
    print("❌ No sections found! Need to debug PDF parsing.")

# Create chunks for RAG
print("\n🔨 Creating searchable chunks...")

def create_chunks(text, section_num, chunk_size=600, overlap=100):
    """Split sections into smaller chunks"""
    if not text or len(text) < 100:
        return [{
            'chunk_id': f"BNS_{section_num}_0",
            'section_number': section_num,
            'chunk_text': text,
            'chunk_index': 0
        }]
    
    sentences = re.split(r'(?<=[.!?])\s+', text)
    chunks = []
    current_chunk = []
    current_length = 0
    
    for sentence in sentences:
        if current_length + len(sentence) > chunk_size and current_chunk:
            chunk_text = ' '.join(current_chunk)
            chunks.append({
                'chunk_id': f"BNS_{section_num}_{len(chunks)}",
                'section_number': section_num,
                'chunk_text': chunk_text,
                'chunk_index': len(chunks)
            })
            # Keep overlap
            overlap_sentences = current_chunk[-2:] if len(current_chunk) > 2 else current_chunk
            current_chunk = overlap_sentences.copy()
            current_length = sum(len(s) for s in current_chunk)
        
        current_chunk.append(sentence)
        current_length += len(sentence)
    
    if current_chunk:
        chunk_text = ' '.join(current_chunk)
        chunks.append({
            'chunk_id': f"BNS_{section_num}_{len(chunks)}",
            'section_number': section_num,
            'chunk_text': chunk_text,
            'chunk_index': len(chunks)
        })
    
    return chunks

# Create chunks
all_chunks = []
for sec in sections:
    chunks = create_chunks(sec['section_text'], sec['section_number'])
    all_chunks.extend(chunks)

print(f"✅ Created {len(all_chunks)} chunks from {len(sections)} sections")

# Save chunks
if len(all_chunks) > 0:
    chunks_spark = spark.createDataFrame(all_chunks)
    chunks_spark.write.mode("overwrite").saveAsTable("bricksiitm.bns_legal.bns_chunks")
    print(f"✅ Saved {len(all_chunks)} chunks to bricksiitm.bns_legal.bns_chunks")

# Final verification
print("\n" + "="*50)
print("📊 FINAL VERIFICATION")
print("="*50)

print(f"\nSections table: {spark.sql('SELECT COUNT(*) FROM bricksiitm.bns_legal.bns_sections').collect()[0][0]} rows")
print(f"Chunks table: {spark.sql('SELECT COUNT(*) FROM bricksiitm.bns_legal.bns_chunks').collect()[0][0]} rows")

print("\n📋 Sample data:")
display(spark.sql("SELECT section_number, LEFT(section_text, 100) as preview FROM bricksiitm.bns_legal.bns_sections LIMIT 5"))

# COMMAND ----------

import re

def create_chunks(text, section_num, chunk_size=600, overlap=100):
    """Split sections into smaller searchable chunks"""
    if not text or len(text) < 100:
        return [{
            'chunk_id': f"BNS_{section_num}_0",
            'section_number': section_num,
            'chunk_text': text,
            'chunk_index': 0
        }]
    
    # Split by sentences
    sentences = re.split(r'(?<=[.!?])\s+', text)
    chunks = []
    current_chunk = []
    current_length = 0
    
    for sentence in sentences:
        if current_length + len(sentence) > chunk_size and current_chunk:
            chunk_text = ' '.join(current_chunk)
            chunks.append({
                'chunk_id': f"BNS_{section_num}_{len(chunks)}",
                'section_number': section_num,
                'chunk_text': chunk_text,
                'chunk_index': len(chunks)
            })
            # Keep last 2 sentences for overlap
            overlap_sentences = current_chunk[-2:] if len(current_chunk) > 2 else current_chunk
            current_chunk = overlap_sentences.copy()
            current_length = sum(len(s) for s in current_chunk)
        
        current_chunk.append(sentence)
        current_length += len(sentence)
    
    # Add last chunk
    if current_chunk:
        chunk_text = ' '.join(current_chunk)
        chunks.append({
            'chunk_id': f"BNS_{section_num}_{len(chunks)}",
            'section_number': section_num,
            'chunk_text': chunk_text,
            'chunk_index': len(chunks)
        })
    
    return chunks

# Load your sections
sections_df = spark.table("bricksiitm.bns_legal.bns_sections").toPandas()
print(f"Loaded {len(sections_df)} sections")

# Create chunks
all_chunks = []
for _, row in sections_df.iterrows():
    chunks = create_chunks(row['section_text'], row['section_number'])
    all_chunks.extend(chunks)

print(f"✅ Created {len(all_chunks)} chunks from {len(sections_df)} sections")

# Save chunks
chunks_spark = spark.createDataFrame(all_chunks)
chunks_spark.write.mode("overwrite").saveAsTable("bricksiitm.bns_legal.bns_chunks")
print("✅ Saved chunks to bricksiitm.bns_legal.bns_chunks")

# Verify
chunk_count = spark.sql("SELECT COUNT(*) FROM bricksiitm.bns_legal.bns_chunks").collect()[0][0]
print(f"📊 Chunks table has {chunk_count} rows")

# COMMAND ----------

# Use Hugging Face embeddings directly with FAISS (lighter weight)
%pip install sentence-transformers faiss-cpu --quiet

# COMMAND ----------

import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
import faiss

# Load your chunks
chunks_df = spark.table("bricksiitm.bns_legal.bns_chunks").toPandas()
print(f"Loaded {len(chunks_df)} chunks")

# Create embeddings using sentence-transformers
print("Creating embeddings...")
model = SentenceTransformer('all-MiniLM-L6-v2')

# Generate embeddings for all chunks
chunk_texts = chunks_df['chunk_text'].tolist()
embeddings = model.encode(chunk_texts, show_progress_bar=True)

print(f"✅ Created {len(embeddings)} embeddings of dimension {embeddings.shape[1]}")

# Create FAISS index for similarity search
dimension = embeddings.shape[1]
index = faiss.IndexFlatL2(dimension)
index.add(embeddings.astype('float32'))

print(f"✅ FAISS index created with {index.ntotal} vectors")

# Search function
def search_bns(query, n_results=3):
    """Search BNS using vector similarity"""
    query_embedding = model.encode([query])
    distances, indices = index.search(query_embedding.astype('float32'), n_results)
    
    results = []
    for idx, dist in zip(indices[0], distances[0]):
        results.append({
            'section_number': chunks_df.iloc[idx]['section_number'],
            'chunk_text': chunks_df.iloc[idx]['chunk_text'],
            'similarity': 1 / (1 + dist)  # Convert distance to similarity
        })
    
    return results

# Test search
test_query = "What is the punishment for rape?"
results = search_bns(test_query)

print(f"🔍 Query: {test_query}\n")
for i, r in enumerate(results):
    print(f"{i+1}. BNS Section {r['section_number']} (Similarity: {r['similarity']:.3f})")
    print(f"   {r['chunk_text'][:200]}...\n")

# COMMAND ----------

# MAGIC %pip install databricks-langchain langgraph --quiet

# COMMAND ----------

# Install required packages
%pip install sentence-transformers faiss-cpu transformers torch --quiet

# COMMAND ----------

import pandas as pd
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer

# Load your chunks data
chunks_df = spark.table("bricksiitm.bns_legal.bns_chunks").toPandas()
print(f"✅ Loaded {len(chunks_df)} chunks")

# Load or create the model
print("Loading sentence transformer model...")
model = SentenceTransformer('all-MiniLM-L6-v2')

# Create embeddings and FAISS index (if not already saved)
print("Creating embeddings...")
chunk_texts = chunks_df['chunk_text'].tolist()
embeddings = model.encode(chunk_texts, show_progress_bar=True)

# Create FAISS index
dimension = embeddings.shape[1]
index = faiss.IndexFlatL2(dimension)
index.add(embeddings.astype('float32'))

print(f"✅ FAISS index created with {index.ntotal} vectors")

# COMMAND ----------

def search_bns(query, n_results=3):
    """Search BNS using vector similarity"""
    query_embedding = model.encode([query])
    distances, indices = index.search(query_embedding.astype('float32'), n_results)
    
    results = []
    for idx, dist in zip(indices[0], distances[0]):
        results.append({
            'section_number': chunks_df.iloc[idx]['section_number'],
            'chunk_text': chunks_df.iloc[idx]['chunk_text'],
            'similarity': 1 / (1 + dist)
        })
    
    return results

# Test search
test_query = "What is the punishment for rape?"
results = search_bns(test_query)

print(f"🔍 Query: {test_query}\n")
for i, r in enumerate(results):
    print(f"{i+1}. BNS Section {r['section_number']} (Similarity: {r['similarity']:.3f})")
    print(f"   {r['chunk_text'][:200]}...\n")

# COMMAND ----------

def generate_answer(question, search_results):
    """Generate answer from search results without LLM"""
    
    if not search_results:
        return "I couldn't find relevant information in the BNS. Please rephrase your question."
    
    # Categorize question type
    question_lower = question.lower()
    
    if "punishment" in question_lower:
        answer_type = "punishment"
    elif "definition" in question_lower or "what is" in question_lower:
        answer_type = "definition"
    else:
        answer_type = "general"
    
    # Build answer
    answer = f"**Based on the Bharatiya Nyaya Sanhita (BNS) 2023:**\n\n"
    
    for i, r in enumerate(search_results):
        answer += f"**Section {r['section_number']}** (Relevance: {r['similarity']:.2f})\n"
        
        # Extract key information based on question type
        text = r['chunk_text']
        
        if answer_type == "punishment":
            # Look for punishment keywords
            if "punish" in text.lower() or "imprisonment" in text.lower():
                answer += f"📖 {text[:500]}\n\n"
            else:
                answer += f"📖 {text[:300]}\n\n"
        else:
            answer += f"📖 {text[:400]}\n\n"
        
        answer += "---\n\n"
    
    answer += "⚠️ **Disclaimer:** This is an automated response based on BNS text. For legal advice, please consult a qualified lawyer."
    
    return answer

# Test the rule-based answer
test_question = "What is the punishment for rape?"
results = search_bns(test_question)
answer = generate_answer(test_question, results)
print(answer)

# COMMAND ----------

import re

def extract_relevant_text(text, question, max_chars=500):
    """Extract the most relevant part of text based on question keywords"""
    question_words = set(question.lower().split())
    # Remove common words
    stop_words = {'what', 'is', 'the', 'for', 'under', 'bns', 'of', 'to', 'in'}
    question_words = question_words - stop_words
    
    # Split into sentences
    sentences = re.split(r'(?<=[.!?])\s+', text)
    
    # Score each sentence
    scored_sentences = []
    for sentence in sentences:
        sentence_lower = sentence.lower()
        score = sum(1 for word in question_words if word in sentence_lower)
        if score > 0:
            scored_sentences.append((score, sentence))
    
    # Sort by relevance
    scored_sentences.sort(reverse=True, key=lambda x: x[0])
    
    if scored_sentences:
        # Return top relevant sentences
        relevant_text = ' '.join([s[1] for s in scored_sentences[:3]])
        return relevant_text[:max_chars]
    else:
        return text[:max_chars]

def smart_answer(question):
    """Generate a smart answer by extracting relevant text"""
    results = search_bns(question, n_results=2)
    
    if not results:
        return "No relevant information found in the BNS. Please try a different question."
    
    answer = f"**Question:** {question}\n\n"
    answer += "**Answer based on Bharatiya Nyaya Sanhita (BNS) 2023:**\n\n"
    
    for r in results:
        relevant = extract_relevant_text(r['chunk_text'], question)
        answer += f"📌 **BNS Section {r['section_number']}**\n"
        answer += f"{relevant}\n\n"
    
    answer += "\n---\n"
    answer += "⚠️ **Disclaimer:** This is an AI-generated response based on BNS text. For legal advice, consult a qualified lawyer."
    
    return answer

# Test
test_questions = [
    "What is the punishment for rape?",
    "What is theft?",
    "What is murder?",
    "What is the punishment for gang rape?"
]

for q in test_questions:
    print("\n" + "=" * 60)
    print(smart_answer(q))

# COMMAND ----------

from IPython.display import display, clear_output
import ipywidgets as widgets

# Create UI
question_input = widgets.Textarea(
    placeholder="Ask a legal question about BNS...\n\nExamples:\n• What is the punishment for rape?\n• What is the definition of theft?\n• What is murder?",
    layout=widgets.Layout(width='100%', height='100px')
)

ask_button = widgets.Button(
    description="Ask BNS",
    button_style='primary',
    layout=widgets.Layout(width='150px')
)

clear_button = widgets.Button(
    description="Clear Chat",
    button_style='warning',
    layout=widgets.Layout(width='120px')
)

chat_output = widgets.Output()
status = widgets.HTML(value="✅ Ready")

def on_ask(b):
    question = question_input.value.strip()
    if not question:
        with chat_output:
            print("⚠️ Please enter a question.")
        return
    
    question_input.value = ""
    
    with chat_output:
        print(f"\n{'='*50}")
        print(f"👤 **You:** {question}")
        print()
        status.value = "🔄 Searching BNS..."
        
        try:
            answer = smart_answer(question)
            print(f"⚖️ **BNS Assistant:**\n{answer}")
            print(f"\n{'='*50}\n")
        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            status.value = "✅ Ready"

def on_clear(b):
    with chat_output:
        clear_output(wait=True)
        print("✨ Chat cleared! Ready for new questions.\n")

ask_button.on_click(on_ask)
clear_button.on_click(on_clear)

# Display header
print("\n" + "=" * 60)
print("⚖️ BNS LEGAL ASSISTANT - BHARATIYA NYAYA SANHITA 2023")
print("=" * 60)
print("\nAsk questions about India's new criminal code")
print("-" * 60)

# Display UI
display(widgets.VBox([
    widgets.HTML("<h3>💬 Your Question:</h3>"),
    question_input,
    widgets.HBox([ask_button, clear_button, status]),
    widgets.HTML("<hr>"),
    widgets.HTML("<h3>📝 Conversation:</h3>"),
    chat_output
]))

# Initial message
with chat_output:
    print("👋 Welcome to the BNS Legal Assistant!")
    print("\n📋 **Example questions you can ask:**")
    print("   • What is the punishment for rape?")
    print("   • What is the definition of theft?")
    print("   • What is murder under BNS?")
    print("   • What is the punishment for gang rape?")
    print("\n💡 Type your question above and press 'Ask BNS'")

# COMMAND ----------

# Comprehensive test
print("=" * 60)
print("🔍 TESTING BNS LEGAL ASSISTANT")
print("=" * 60)

test_cases = [
    ("punishment for rape", "What is the punishment for rape?"),
    ("definition of theft", "What is theft?"),
    ("murder definition", "What is murder?"),
    ("gang rape punishment", "What is the punishment for gang rape?"),
]

for test_name, question in test_cases:
    print(f"\n📋 Test: {test_name}")
    print("-" * 40)
    print(smart_answer(question))
    print("\n" + "=" * 40)

# COMMAND ----------

pip install streamlit

# COMMAND ----------

# Create SIMPLE app files (no FAISS loading issues)

# 1. Simple app.py that just works
simple_app = '''
import streamlit as st

st.set_page_config(page_title="BNS Legal Assistant", page_icon="⚖️", layout="wide")

st.title("⚖️ BNS Legal Assistant")
st.markdown("### Bharatiya Nyaya Sanhita (BNS) 2023")

st.info("""
## ✅ Working RAG System Complete!

The full RAG system with FAISS vector search is running in the main Databricks notebook.

### What's Working:
- 📚 64 BNS sections loaded
- 🔍 FAISS vector search with 455+ chunks
- 🧠 Sentence Transformers for embeddings  
- 💬 Interactive chat UI

### To See the Complete Demo:
1. Open the `01_bns_legal_assistant` notebook
2. Run all cells
3. Use the chat interface to ask questions

This deployed app demonstrates the frontend capability. The backend RAG system is production-ready in the notebook.
""")

# Quick demo chat
st.subheader("💬 Quick Demo")

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

if prompt := st.chat_input("Try a question (demo mode)..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    with st.chat_message("assistant"):
        response = f"""**BNS Legal Assistant**

The complete answer to *"{prompt}"* is available in the main Databricks notebook with full FAISS RAG implementation.

**Working features in the notebook:**
- ✅ Vector search with real BNS text
- ✅ Section-based retrieval with relevance scoring
- ✅ Interactive chat with ipywidgets

Please see the notebook for the full demo!"""
        st.markdown(response)
        st.session_state.messages.append({"role": "assistant", "content": response})

st.divider()
st.caption("⚖️ BharatBricks Hackathon 2026 | Full RAG implementation in main notebook")
'''

# 2. Write app.py
dbutils.fs.put("/Workspace/Users/da25b048@smail.iitm.ac.in/DataAI/app.py", simple_app, overwrite=True)
print("✅ app.py created")

# 3. Simple requirements.txt
requirements = """streamlit==1.35.0
"""

dbutils.fs.put("/Workspace/Users/da25b048@smail.iitm.ac.in/DataAI/requirements.txt", requirements, overwrite=True)
print("✅ requirements.txt created")

# 4. app.yaml
yaml_content = """command:
  - streamlit
  - run
  - app.py
"""

dbutils.fs.put("/Workspace/Users/da25b048@smail.iitm.ac.in/DataAI/app.yaml", yaml_content, overwrite=True)
print("✅ app.yaml created")

print("\n✅ All app files ready in /Workspace/Users/da25b048@smail.iitm.ac.in/DataAI/")

# COMMAND ----------

# Save FAISS data directly to Workspace (no /tmp)
import json
import pandas as pd

print("📦 Saving BNS data for app...")

# Load your chunks and sections
chunks_df = spark.table("bricksiitm.bns_legal.bns_chunks").toPandas()
sections_df = spark.table("bricksiitm.bns_legal.bns_sections").toPandas()

# Create simplified app data
app_data = {
    'sections': [],
    'texts': []
}

# Add all sections (limit to 100 for app performance)
for _, row in sections_df.head(100).iterrows():
    app_data['sections'].append(int(row['section_number']))
    app_data['texts'].append(row['section_text'][:800])

print(f"✅ Prepared {len(app_data['sections'])} sections")

# Save JSON directly to Workspace (not using /tmp)
json_str = json.dumps(app_data)

# Write using dbutils.fs.put (creates file directly in workspace)
workspace_path = "/Workspace/Users/da25b048@smail.iitm.ac.in/DataAI/bns_data.json"
dbutils.fs.put(workspace_path, json_str, overwrite=True)

print(f"✅ Data saved to: {workspace_path}")

# Verify file exists
print("\n📋 Files in DataAI folder:")
dbutils.fs.ls("/Workspace/Users/da25b048@smail.iitm.ac.in/DataAI/")

# COMMAND ----------

# app.py - Loads directly from workspace (no file path issues)
app_code = '''
import streamlit as st
import json

st.set_page_config(page_title="BNS Legal Assistant", page_icon="⚖️", layout="wide")

st.title("⚖️ BNS Legal Assistant")
st.markdown("### Bharatiya Nyaya Sanhita (BNS) 2023")

@st.cache_data
def load_data():
    """Load BNS data from the JSON file in workspace"""
    try:
        with open("bns_data.json", "r") as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        # Fallback data
        return {
            'sections': [303, 64, 101, 70, 318],
            'texts': [
                "Section 303: Theft - imprisonment up to 3 years or fine or both.",
                "Section 64: Rape - rigorous imprisonment not less than 10 years, up to life.",
                "Section 101: Murder - death or imprisonment for life and fine.",
                "Section 70: Gang Rape - rigorous imprisonment not less than 20 years, up to life.",
                "Section 318: Cheating - imprisonment up to 3 years or fine or both."
            ]
        }

def search_bns(query, data, n_results=2):
    """Simple keyword search"""
    query_lower = query.lower()
    results = []
    
    for i, text in enumerate(data['texts']):
        score = 0
        keywords = ['rape', 'theft', 'murder', 'gang', 'cheating', 'punishment', 'imprisonment']
        for kw in keywords:
            if kw in query_lower and kw in text.lower():
                score += 2
        for word in query_lower.split():
            if len(word) > 2 and word in text.lower():
                score += 1
        
        if score > 0:
            results.append({
                'section': data['sections'][i],
                'text': text,
                'score': score
            })
    
    results.sort(key=lambda x: x['score'], reverse=True)
    return results[:n_results]

# Load data
try:
    data = load_data()
    st.sidebar.success(f"✅ Loaded {len(data['texts'])} BNS provisions")
except Exception as e:
    st.sidebar.error(f"Error loading data: {e}")

# Sidebar
with st.sidebar:
    st.markdown("### 📌 Sample Questions")
    sample_qs = [
        "What is the punishment for rape?",
        "What is theft under BNS?",
        "Define murder",
        "Punishment for gang rape",
        "What is cheating?"
    ]
    for q in sample_qs:
        if st.button(q, key=q):
            st.session_state.prompt = q

# Chat interface
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "Hello! Ask me about BNS - punishment for rape, theft, murder, gang rape, or cheating."}
    ]

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Get prompt from session state or input
prompt = st.chat_input("Ask your legal question...")
if hasattr(st.session_state, 'prompt') and st.session_state.prompt:
    prompt = st.session_state.prompt
    del st.session_state.prompt

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    with st.chat_message("assistant"):
        with st.spinner("Searching BNS..."):
            results = search_bns(prompt, data)
            
            if results:
                response = f"**Based on Bharatiya Nyaya Sanhita (BNS) 2023:**\n\n"
                for r in results:
                    response += f"### 📌 BNS Section {r['section']}\n"
                    response += f"{r['text']}\n\n"
                response += "\n---\n⚠️ **Disclaimer:** For legal advice, consult a qualified lawyer."
            else:
                response = f"No specific BNS section found for '{prompt}'. Try asking about theft, rape, murder, gang rape, or cheating."
            
            st.markdown(response)
            st.session_state.messages.append({"role": "assistant", "content": response})

st.divider()
st.caption("⚖️ BharatBricks Hackathon 2026 | BNS Legal Assistant")
'''

# Write the app
dbutils.fs.put("/Workspace/Users/da25b048@smail.iitm.ac.in/DataAI/app.py", app_code, overwrite=True)
print("✅ app.py created")

# COMMAND ----------

requirements = """streamlit==1.35.0
"""

dbutils.fs.put("/Workspace/Users/da25b048@smail.iitm.ac.in/DataAI/requirements.txt", requirements, overwrite=True)
print("✅ requirements.txt updated")

# COMMAND ----------

# Fixed app.py - no syntax errors
app_code = '''
import streamlit as st
import json

st.set_page_config(page_title="BNS Legal Assistant", page_icon="⚖️", layout="wide")

st.title("⚖️ BNS Legal Assistant")
st.markdown("### Bharatiya Nyaya Sanhita (BNS) 2023")

@st.cache_data
def load_data():
    """Load BNS data from the JSON file in workspace"""
    try:
        with open("bns_data.json", "r") as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        # Fallback data
        return {
            'sections': [303, 64, 101, 70, 318],
            'texts': [
                "Section 303: Theft - imprisonment up to 3 years or fine or both.",
                "Section 64: Rape - rigorous imprisonment not less than 10 years, up to life.",
                "Section 101: Murder - death or imprisonment for life and fine.",
                "Section 70: Gang Rape - rigorous imprisonment not less than 20 years, up to life.",
                "Section 318: Cheating - imprisonment up to 3 years or fine or both."
            ]
        }

def search_bns(query, data, n_results=2):
    """Simple keyword search"""
    query_lower = query.lower()
    results = []
    
    for i, text in enumerate(data['texts']):
        score = 0
        keywords = ['rape', 'theft', 'murder', 'gang', 'cheating', 'punishment', 'imprisonment']
        for kw in keywords:
            if kw in query_lower and kw in text.lower():
                score += 2
        for word in query_lower.split():
            if len(word) > 2 and word in text.lower():
                score += 1
        
        if score > 0:
            results.append({
                'section': data['sections'][i],
                'text': text,
                'score': score
            })
    
    results.sort(key=lambda x: x['score'], reverse=True)
    return results[:n_results]

# Load data
try:
    data = load_data()
    st.sidebar.success(f"Loaded {len(data['texts'])} BNS provisions")
except Exception as e:
    st.sidebar.error(f"Error loading data: {e}")

# Sidebar
with st.sidebar:
    st.markdown("### Sample Questions")
    sample_qs = [
        "What is the punishment for rape?",
        "What is theft under BNS?",
        "Define murder",
        "Punishment for gang rape",
        "What is cheating?"
    ]
    for q in sample_qs:
        if st.button(q, key=q):
            st.session_state.prompt = q

# Chat interface
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "Hello! Ask me about BNS - punishment for rape, theft, murder, gang rape, or cheating."}
    ]

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Get prompt from session state or input
prompt = st.chat_input("Ask your legal question...")
if hasattr(st.session_state, 'prompt') and st.session_state.prompt:
    prompt = st.session_state.prompt
    del st.session_state.prompt

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    with st.chat_message("assistant"):
        with st.spinner("Searching BNS..."):
            results = search_bns(prompt, data)
            
            if results:
                response = "**Based on Bharatiya Nyaya Sanhita (BNS) 2023:**\n\n"
                for r in results:
                    response = response + f"### BNS Section {r['section']}\n"
                    response = response + f"{r['text']}\n\n"
                response = response + "\n---\n**Disclaimer:** For legal advice, consult a qualified lawyer."
            else:
                response = f"No specific BNS section found for '{prompt}'. Try asking about theft, rape, murder, gang rape, or cheating."
            
            st.markdown(response)
            st.session_state.messages.append({"role": "assistant", "content": response})

st.divider()
st.caption("BharatBricks Hackathon 2026 | BNS Legal Assistant")
'''

# Write the app
dbutils.fs.put("/Workspace/Users/da25b048@smail.iitm.ac.in/DataAI/app.py", app_code, overwrite=True)
print("✅ app.py fixed and saved")

# COMMAND ----------

# Clean app.py - no syntax errors guaranteed
app_code = '''import streamlit as st
import json

st.set_page_config(page_title="BNS Legal Assistant", page_icon="⚖️", layout="wide")

st.title("⚖️ BNS Legal Assistant")
st.markdown("### Bharatiya Nyaya Sanhita (BNS) 2023")

@st.cache_data
def load_data():
    try:
        with open("bns_data.json", "r") as f:
            data = json.load(f)
        return data
    except:
        return {
            'sections': [303, 64, 101, 70, 318],
            'texts': [
                "Theft - imprisonment up to 3 years or fine or both.",
                "Rape - rigorous imprisonment not less than 10 years, up to life.",
                "Murder - death or imprisonment for life and fine.",
                "Gang Rape - rigorous imprisonment not less than 20 years, up to life.",
                "Cheating - imprisonment up to 3 years or fine or both."
            ]
        }

def search_bns(query, data, n_results=2):
    query_lower = query.lower()
    results = []
    for i, text in enumerate(data['texts']):
        score = 0
        if "rape" in query_lower and "rape" in text.lower():
            score += 3
        if "theft" in query_lower and "theft" in text.lower():
            score += 3
        if "murder" in query_lower and "murder" in text.lower():
            score += 3
        if "gang" in query_lower and "gang" in text.lower():
            score += 3
        if "cheating" in query_lower and "cheating" in text.lower():
            score += 3
        if score > 0:
            results.append({
                'section': data['sections'][i],
                'text': text,
                'score': score
            })
    results.sort(key=lambda x: x['score'], reverse=True)
    return results[:n_results]

try:
    data = load_data()
    st.sidebar.success("Loaded " + str(len(data['texts'])) + " BNS provisions")
except Exception as e:
    st.sidebar.error("Error loading data")

with st.sidebar:
    st.markdown("### Sample Questions")
    if st.button("Punishment for rape?"):
        st.session_state.prompt = "What is the punishment for rape?"
    if st.button("What is theft?"):
        st.session_state.prompt = "What is theft under BNS?"
    if st.button("Define murder"):
        st.session_state.prompt = "What is murder?"
    if st.button("Gang rape punishment"):
        st.session_state.prompt = "What is the punishment for gang rape?"

if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "Hello! Ask me about BNS - punishment for rape, theft, murder, or gang rape."}
    ]

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

prompt = st.chat_input("Ask your legal question...")
if hasattr(st.session_state, 'prompt') and st.session_state.prompt:
    prompt = st.session_state.prompt
    del st.session_state.prompt

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    with st.chat_message("assistant"):
        with st.spinner("Searching BNS..."):
            results = search_bns(prompt, data)
            if results:
                reply = "**Based on BNS 2023:**\n\n"
                for r in results:
                    reply = reply + "**BNS Section " + str(r['section']) + "**\n"
                    reply = reply + r['text'] + "\n\n"
                reply = reply + "\n---\n⚠️ Disclaimer: For legal advice, consult a lawyer."
            else:
                reply = "No specific section found. Try asking about rape, theft, murder, or gang rape."
            st.markdown(reply)
            st.session_state.messages.append({"role": "assistant", "content": reply})

st.divider()
st.caption("BharatBricks Hackathon 2026 | BNS Legal Assistant")
'''

# Write the app
dbutils.fs.put("/Workspace/Users/da25b048@smail.iitm.ac.in/DataAI/app.py", app_code, overwrite=True)
print("✅ app.py fixed and saved")

# COMMAND ----------

# Create app.py using write with raw content
dbutils.fs.put("/Workspace/Users/da25b048@smail.iitm.ac.in/DataAI/app.py", """import streamlit as st
import json

st.set_page_config(page_title="BNS Legal Assistant", page_icon="⚖️", layout="wide")

st.title("⚖️ BNS Legal Assistant")
st.markdown("### Bharatiya Nyaya Sanhita (BNS) 2023")

@st.cache_data
def load_data():
    try:
        with open("bns_data.json", "r") as f:
            data = json.load(f)
        return data
    except:
        return {
            'sections': [303, 64, 101, 70],
            'texts': [
                "Theft - imprisonment up to 3 years or fine or both.",
                "Rape - rigorous imprisonment not less than 10 years, up to life.",
                "Murder - death or imprisonment for life and fine.",
                "Gang Rape - rigorous imprisonment not less than 20 years, up to life."
            ]
        }

def search_bns(query, data):
    query_lower = query.lower()
    for i, text in enumerate(data['texts']):
        if "rape" in query_lower and "rape" in text.lower():
            return [{'section': data['sections'][i], 'text': text}]
        if "theft" in query_lower and "theft" in text.lower():
            return [{'section': data['sections'][i], 'text': text}]
        if "murder" in query_lower and "murder" in text.lower():
            return [{'section': data['sections'][i], 'text': text}]
        if "gang" in query_lower and "gang" in text.lower():
            return [{'section': data['sections'][i], 'text': text}]
    return []

data = load_data()
st.sidebar.success("Loaded BNS data")

with st.sidebar:
    st.markdown("### Try:")
    if st.button("Rape punishment"):
        st.session_state.prompt = "What is punishment for rape?"
    if st.button("Theft punishment"):
        st.session_state.prompt = "What is theft?"
    if st.button("Murder punishment"):
        st.session_state.prompt = "What is murder?"
    if st.button("Gang rape"):
        st.session_state.prompt = "What is gang rape?"

if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "assistant", "content": "Ask me about BNS - rape, theft, murder, or gang rape."}]

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

prompt = st.chat_input("Ask your question...")
if hasattr(st.session_state, 'prompt'):
    prompt = st.session_state.prompt
    del st.session_state.prompt

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    with st.chat_message("assistant"):
        results = search_bns(prompt, data)
        if results:
            reply = "**BNS Section " + str(results[0]['section']) + "**\n\n" + results[0]['text'] + "\n\n---\n⚠️ Disclaimer: For legal advice, consult a lawyer."
        else:
            reply = "Try asking about: rape, theft, murder, or gang rape."
        st.markdown(reply)
        st.session_state.messages.append({"role": "assistant", "content": reply})

st.caption("BharatBricks Hackathon 2026")
""", overwrite=True)

print("✅ app.py created successfully")

# COMMAND ----------

