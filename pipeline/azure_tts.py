from dotenv import load_dotenv
load_dotenv()
# pipeline/azure_tts.py
import os
import uuid
import pathlib
from pipeline.config import Config
import azure.cognitiveservices.speech as speechsdk

# def synthesize_ssml_to_file(ssml: str, out_path: str):
#     key = Config.AZURE_SPEECH_KEY
#     region = Config.AZURE_SPEECH_REGION
#     if not key or not region:
#         raise RuntimeError("Azure TTS credentials missing in environment.")
#     speech_config = speechsdk.SpeechConfig(subscription=key, region=region)
#     speech_config.set_speech_synthesis_output_format(
#         speechsdk.SpeechSynthesisOutputFormat.Audio48Khz192KBitRateMonoMp3
#     )
#     audio_config = speechsdk.audio.AudioConfig(filename=out_path)
#     synthesizer = speechsdk.SpeechSynthesizer(speech_config=speech_config, audio_config=audio_config)
#     result = synthesizer.speak_ssml_async(ssml).get()
#     if result.reason == speechsdk.ResultReason.SynthesizingAudioCompleted:
#         return out_path
#     else:
#         err = result.error_details if hasattr(result, "error_details") else str(result.reason)
#         raise RuntimeError(f"Azure synthesis failed: {err}")

def synthesize_ssml_to_file(ssml: str, out_path: str):
    key = Config.AZURE_SPEECH_KEY
    region = Config.AZURE_SPEECH_REGION
    if not key or not region:
        raise RuntimeError("Azure TTS credentials missing in environment.")
    
    # 🧠 DEBUG: Print or save SSML to inspect
    print("\n🗣 [DEBUG] Generated SSML snippet:")
    print(ssml[:500])  # first 500 chars (to avoid full dump)
    print("-" * 60)

    # Optionally save to a temp file for manual inspection
    with open("debug_ssml_preview.xml", "w", encoding="utf-8") as f:
        f.write(ssml)

    speech_config = speechsdk.SpeechConfig(subscription=key, region=region)
    speech_config.set_speech_synthesis_output_format(
        speechsdk.SpeechSynthesisOutputFormat.Audio48Khz192KBitRateMonoMp3
    )
    audio_config = speechsdk.audio.AudioConfig(filename=out_path)
    synthesizer = speechsdk.SpeechSynthesizer(speech_config=speech_config, audio_config=audio_config)
    result = synthesizer.speak_ssml_async(ssml).get()

    if result.reason == speechsdk.ResultReason.SynthesizingAudioCompleted:
        return out_path
    else:
        err = result.error_details if hasattr(result, "error_details") else str(result.reason)
        raise RuntimeError(f"Azure synthesis failed: {err}")


def synthesize_ssml_to_tempfile(ssml: str, prefix: str = "news_", ext: str = ".mp3"):
    pathlib.Path(Config.OUTPUT_AUDIO_DIR).mkdir(parents=True, exist_ok=True)
    filename = f"{prefix}{uuid.uuid4().hex}{ext}"
    out_path = str(pathlib.Path(Config.OUTPUT_AUDIO_DIR) / filename)
    return synthesize_ssml_to_file(ssml, out_path)

print("Azure TTS module loaded.")