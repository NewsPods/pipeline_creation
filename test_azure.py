from dotenv import load_dotenv
load_dotenv()

import azure.cognitiveservices.speech as speechsdk
import os

key = os.getenv("AZURE_SPEECH_KEY")
region = os.getenv("AZURE_SPEECH_REGION")

speech_config = speechsdk.SpeechConfig(subscription=key, region=region)
speech_config.speech_synthesis_voice_name = "en-US-JennyNeural"

synthesizer = speechsdk.SpeechSynthesizer(speech_config=speech_config)

print("Testing Azure TTS...")

result = synthesizer.speak_text_async("Hello Tarnveer, Azure TTS is working correctly.").get()

if result.reason == speechsdk.ResultReason.SynthesizingAudioCompleted:
    print("✅ Azure TTS test succeeded.")
else:
    print("❌ Azure TTS test failed:", result.reason)
    if result.cancellation_details:
        print("Details:", result.cancellation_details.reason)
        print("Error details:", result.cancellation_details.error_details)
