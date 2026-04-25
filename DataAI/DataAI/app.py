import streamlit as st
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
            reply = "**BNS Section " + str(results[0]['section']) + "**

" + results[0]['text'] + "

---
⚠️ Disclaimer: For legal advice, consult a lawyer."
        else:
            reply = "Try asking about: rape, theft, murder, or gang rape."
        st.markdown(reply)
        st.session_state.messages.append({"role": "assistant", "content": reply})

st.caption("BharatBricks Hackathon 2026")
