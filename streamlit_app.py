from langchain.chains.question_answering import load_qa_chain
from langchain.chat_models import ChatOpenAI
from langchain.document_loaders import PyPDFLoader, OnlinePDFLoader
from langchain.document_loaders.base import Document
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.llms import OpenAI
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.vectorstores import Pinecone
from sentence_transformers import SentenceTransformer
from streamlit_lottie import st_lottie_spinner
import os
import pinecone
import requests
import streamlit as st

favicon = 'https://polimata.ai/wp-content/uploads/2023/07/favicon-32x32-1.png'
st.set_page_config(
    page_title="Fr8Tech",
    page_icon=favicon,
    initial_sidebar_state="expanded"
)

hide_default_format = """
       <style>
       header { visibility: hidden; }
       #MainMenu {visibility: hidden; }
       footer {visibility: hidden;}
       #GithubIcon {visibility: hidden;}
       </style>
       """

st.markdown(hide_default_format, unsafe_allow_html=True)
st.sidebar.image("https://fr8technologies.com/wp-content/uploads/2023/02/Recurso-2@2x-1024x186-1.png", use_column_width=True)
st.title("Fr8Tech :chart_with_upwards_trend: ")
st.caption(':turtle: V1.01')
st.subheader('Audit Fr8Tech SEC Filing: 20-F')
st.write('	Annual and transition report of foreign private issuers [Sections 13 or 15(d)]')
st.divider()
st.sidebar.title(f'Sample Questions')
st.sidebar.write('what is Fr8Tech strategy?')
st.sidebar.write('What is the Fr8Tech capital structure')
st.sidebar.write('How can Fr8Tech become finantially healthy')
st.sidebar.write('What are the financial challenges for Fr8Tech')
st.sidebar.write('What are the risk of Fr8Tech')
st.sidebar.caption('Powered by Polímata.AI')
