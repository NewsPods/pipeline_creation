# pipeline/ssml_creator.py
import os
import re
import xml.etree.ElementTree as ET

from pipeline.config import Config
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()

if Config.GOOGLE_API_KEY:
    genai.configure(api_key=Config.GOOGLE_API_KEY)

MODEL_NAME = "models/gemini-2.5-flash"  # change if you have another model


def build_prompt(
    article_text: str,
    voice1: str = "en-IN-NeerjaNeural",
    voice2: str = "en-IN-PrabhatNeural",
    pacing: str = "medium",
) -> str:
    """
    Build a strict prompt so the model stops generating SSML that Azure rejects.
    """
    prompt = f"""
You are an SSML generator that turns a news article into a podcast-style script with two speakers.

Your response MUST obey ALL of the following:

1) Output ONLY valid SSML. No markdown, no backticks, no commentary.
2) Wrap the entire response in a single root element:
   <speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xml:lang="en-IN"> ... </speak>
3) Use EXACTLY these two voices, alternating between them:
   - <voice name="{voice1}"> ... </voice>
   - <voice name="{voice2}"> ... </voice>
4) All <break> tags MUST be inside a <voice> block (ideally inside <prosody> or <p>).
   - NEVER put <break> directly under <speak>.
5) Use <prosody rate="{pacing}"> around the spoken text inside each <voice>.
6) Use <emphasis> for key phrases, and <break time="200ms"/>–<break time="400ms"/> between sentences,
   and <break time="600ms"/>–<break time="900ms"/> between sections.
7) Ensure ALL XML tags are properly closed:
   - Every <voice> has a matching </voice>
   - Every <prosody>, <emphasis>, <p>, etc. is properly closed
8) The script should sound conversational and be a summary of the article, not a verbatim readout.

Return ONLY the SSML.

Article:
\"\"\"{article_text}\"\"\"
"""
    return prompt


def clean_ssml(raw: str) -> str:
    """
    Post-process the model output so Azure doesn't die on stupid formatting issues.
    - Strip markdown fences like ```xml ... ```
    - Keep only the <speak>...</speak> block
    - Trim leading/trailing whitespace
    """
    text = raw.strip()

    # Strip ```...``` wrappers if the model ignores the "no markdown" instruction
    if text.startswith("```"):
        # Remove leading ```something\n and trailing ```
        text = re.sub(r"^```[a-zA-Z0-9]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text).strip()

    # Keep only the first <speak ...> ... </speak> block if there's junk around it
    start = text.find("<speak")
    end = text.rfind("</speak>")
    if start != -1 and end != -1:
        text = text[start : end + len("</speak>")]

    return text.strip()


def validate_ssml(ssml: str) -> None:
    """
    Basic XML validation so we fail fast if the SSML is malformed.
    This won't catch Azure-specific semantics, but it will catch broken tags.
    """
    try:
        ET.fromstring(ssml)
    except ET.ParseError as e:
        # You can log the SSML here if you want, but don't silently continue.
        raise RuntimeError(f"Invalid SSML XML produced by LLM: {e}")


def call_llm_to_ssml(prompt: str) -> str:
    if not Config.GOOGLE_API_KEY:
        raise RuntimeError("GOOGLE_API_KEY not set.")

    model = genai.GenerativeModel(MODEL_NAME)
    response = model.generate_content(prompt)

    if not response.text:
        raise RuntimeError("LLM returned empty SSML response.")

    raw_ssml = response.text
    ssml = clean_ssml(raw_ssml)
    validate_ssml(ssml)  # will raise if badly formed XML

    return ssml


def article_to_double_ssml(
    article_text: str,
    voice1: str = "en-IN-NeerjaNeural",
    voice2: str = "en-IN-PrabhatNeural",
    pacing: str = "medium",
) -> str:
    prompt = build_prompt(article_text, voice1, voice2, pacing)
    ssml = call_llm_to_ssml(prompt)
    return ssml


print("SSML Creator module loaded.")
