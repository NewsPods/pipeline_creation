# # pipeline/b2_uploader.py
# import b2sdk.v2 as b2
# from pipeline.config import Config
# import os

# def authorize_b2():
#     if not Config.B2_KEY_ID or not Config.B2_APP_KEY:
#         raise RuntimeError("B2 credentials missing.")
#     info = b2.InMemoryAccountInfo()
#     api = b2.B2Api(info)
#     api.authorize_account("production", Config.B2_KEY_ID, Config.B2_APP_KEY)
#     return api

# def upload_file(local_path: str, object_name: str):
#     api = authorize_b2()
#     bucket = api.get_bucket_by_name(Config.B2_BUCKET_NAME)
#     file_info = {"Content-Type": "audio/mpeg"}
#     res = bucket.upload_local_file(local_file=local_path, file_name=object_name, file_infos=file_info)
#     # The download URL depends on your B2 settings; return the file key and info
#     return {
#         "file_name": res.file_name,
#         "file_id": res.file_id_,
#         "object_name": object_name
#     }

# pipeline/b2_uploader.py
import os
import b2sdk.v2 as b2
from pipeline.config import Config

def authorize_b2():
    """Authorize and return a B2 API client."""
    if not Config.B2_KEY_ID or not Config.B2_APP_KEY:
        raise RuntimeError("B2 credentials missing.")
    info = b2.InMemoryAccountInfo()
    api = b2.B2Api(info)
    api.authorize_account("production", Config.B2_KEY_ID, Config.B2_APP_KEY)
    return api


def check_audio_file(local_path: str):
    """Check if the Azure TTS audio file exists before upload."""
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"⚠️ Audio file not found: {local_path}")
    size_kb = os.path.getsize(local_path) / 1024
    print(f"🎧 Audio file ready: {local_path} ({size_kb:.2f} KB)")
    print("👉 You can open and play this file manually before uploading.")


def upload_file(local_path: str, object_name: str):
    """Upload a verified audio file to B2."""
    check_audio_file(local_path)  # ensure file exists first

    api = authorize_b2()
    bucket = api.get_bucket_by_name(Config.B2_BUCKET_NAME)
    file_info = {"Content-Type": "audio/mpeg"}

    res = bucket.upload_local_file(
        local_file=local_path,
        file_name=object_name,
        file_infos=file_info
    )

    # Fix: new SDK attribute names
    file_id = getattr(res, "id_", None) or getattr(res, "file_id", None)

    print(f"✅ Uploaded '{object_name}' successfully to B2.")

    return {
        "file_name": res.file_name,
        "file_id": file_id,
        "object_name": object_name
    }

