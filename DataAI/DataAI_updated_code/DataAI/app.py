
import streamlit as st

st.set_page_config(page_title="BNS Legal Assistant", page_icon="⚖️")

st.title("BNS Legal Assistant")
st.write("Ask questions about Bharatiya Nyaya Sanhita 2023")

answers = {
    "rape": "BNS Section 64: Rape is punishable with rigorous imprisonment for not less than 10 years, up to imprisonment for life.",
    "theft": "BNS Section 303: Theft is punishable with imprisonment up to 3 years, or fine, or both.",
    "murder": "BNS Section 101: Murder is punishable with death or imprisonment for life and fine.",
    "gang rape": "BNS Section 70: Gang rape is punishable with rigorous imprisonment for not less than 20 years, up to life.",
    "robbery": "BNS Section 310: Robbery is punishable with imprisonment up to 10 years and fine.",
    "cheating": "BNS Section 318: Cheating is punishable with imprisonment up to 3 years or fine or both.",
    "kidnapping": "BNS Section 137: Kidnapping is punishable with imprisonment up to 7 years and fine."
}

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    st.chat_message(msg["role"]).write(msg["content"])

prompt = st.chat_input("Ask a question...")

if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    st.chat_message("user").write(prompt)
    
    answer = None
    for key in answers:
        if key in prompt.lower():
            answer = answers[key]
            break
    
    if not answer:
        answer = "Try asking about: rape, theft, murder, gang rape, robbery, cheating, or kidnapping."
    
    st.chat_message("assistant").write(answer)
    st.session_state.messages.append({"role": "assistant", "content": answer})
