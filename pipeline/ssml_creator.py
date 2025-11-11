# pipeline/ssml_creator.py
import os
from pipeline.config import Config
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()

if Config.GOOGLE_API_KEY:
    genai.configure(api_key=Config.GOOGLE_API_KEY)

MODEL_NAME = "models/gemini-2.5-flash"  # change if you have another model

def build_prompt(article_text: str,
                 voice1: str = "en-IN-NeerjaNeural",
                 voice2: str = "en-IN-PrabhatNeural",
                 pacing: str = "medium") -> str:
    prompt = f"""
You are an SSML generator that turns a news article into a podcast-style script with two speakers.

Output MUST:
- Be valid SSML enclosed in <speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xml:lang="en-IN"> ... </speak>.
- Alternate between these two voices exactly: <voice name="{voice1}"> and <voice name="{voice2}">.
- Use expressive tags like <emphasis>, <break>, and <prosody rate="{pacing}">.
- Small pauses (200ms–400ms) between sentences and longer ones (600ms–900ms) between sections.
- No commentary, markdown, or explanation — only raw SSML.

Article:
\"\"\"{article_text}\"\"\"
"""
    return prompt

def call_llm_to_ssml(prompt: str) -> str:
    if not Config.GOOGLE_API_KEY:
        raise RuntimeError("GOOGLE_API_KEY not set.")
    model = genai.GenerativeModel(MODEL_NAME)
    response = model.generate_content(prompt)
    return response.text.strip()

def article_to_double_ssml(article_text: str,
                           voice1: str = "en-IN-NeerjaNeural",
                           voice2: str = "en-IN-PrabhatNeural",
                           pacing: str = "medium") -> str:
    prompt = build_prompt(article_text, voice1, voice2, pacing)
    ssml = call_llm_to_ssml(prompt)
    return ssml

print("SSML Creator module loaded.")
