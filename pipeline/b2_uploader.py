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
import subprocess  # Added for running FFmpeg
import tempfile  # Added for creating a temp directory
import pathlib  # Added for easier file path handling
from pipeline.config import Config


# --- Your Existing Functions (Unchanged) ---

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


def upload_file(local_path: str, object_name: str):
    """Upload a verified audio file to B2."""
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"⚠️ Internal Error: File not found: {local_path}")

    api = authorize_b2()
    bucket = api.get_bucket_by_name(Config.B2_BUCKET_NAME)

    # Determine content type based on file extension
    ext = os.path.splitext(object_name)[1].lower()
    if ext == ".aac":
        content_type = "audio/aac"
    elif ext == ".m3u8":
        content_type = "application/vnd.apple.mpegurl"
    elif ext == ".mp3":
        content_type = "audio/mpeg"
    else:
        content_type = "b2/x-auto"  # let B2 guess

    res = bucket.upload_local_file(
        local_file=local_path,
        file_name=object_name,
        content_type=content_type,   # <-- correct usage
        # file_info={}  # optional if you want custom metadata
    )

    file_id = getattr(res, "id_", None) or getattr(res, "file_id", None)

    return {
        "file_name": res.file_name,
        "file_id": file_id,
        "object_name": object_name,
    }



# --- New HLS Orchestrator Function ---

def upload_as_hls(local_mp3_path: str, b2_object_prefix: str):
    """
    Converts a local MP3 file to HLS and uploads all segments to B2.

    Args:
        local_mp3_path (str): The path to the source MP3 file.
        b2_object_prefix (str): The "folder" on B2 to upload to.
                                  e.g., "audio/hls/article_123"
    """
    print(f"\n--- Starting HLS Conversion for {local_mp3_path} ---")

    # 1. Check if source MP3 exists
    check_audio_file(local_mp3_path)

    # 2. Create a temporary directory to store HLS segments
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Working in temporary directory: {temp_dir}")

        # Define HLS output files
        playlist_path = os.path.join(temp_dir, "index.m3u8")
        segment_filename = os.path.join(temp_dir, "seg_%03d.aac")

        # 3. Run FFmpeg command
        # -i: input file
        # -vn: no video
        # -acodec aac: convert audio to AAC (standard for HLS)
        # -hls_time 4: create 4-second segments
        # -hls_playlist_type vod: create a "Video on Demand" playlist (all segments listed)
        # -hls_segment_filename: pattern for segment files
        # index.m3u8: name of the master playlist
        ffmpeg_command = [
            "ffmpeg",
            "-i", local_mp3_path,
            "-vn",
            "-acodec", "aac",
            "-hls_time", "4",
            "-hls_playlist_type", "vod",
            "-hls_segment_filename", segment_filename,
            playlist_path
        ]

        try:
            print("🏃 Running FFmpeg...")
            subprocess.run(ffmpeg_command, check=True, capture_output=True, text=True)
            print("✅ FFmpeg conversion successful.")
        except subprocess.CalledProcessError as e:
            print(f"❌ FFmpeg Error:")
            print(e.stderr)
            raise RuntimeError("FFmpeg conversion failed.")
        except FileNotFoundError:
            print("❌ FFmpeg Error: 'ffmpeg' command not found.")
            print("Please ensure FFmpeg is installed and in your system's PATH.")
            raise

        # 4. Loop through generated files and upload them
        print(f"🚀 Uploading HLS segments to B2 folder: {b2_object_prefix}/")

        # Use pathlib to find all generated HLS files
        temp_dir_path = pathlib.Path(temp_dir)
        hls_files = [f for f in temp_dir_path.glob('*') if f.name.endswith('.m3u8') or f.name.endswith('.aac')]

        if not hls_files:
            raise RuntimeError("HLS conversion produced no files.")

        uploaded_files = []
        for file_path in hls_files:
            object_name = f"{b2_object_prefix}/{file_path.name}"
            print(f"  > Uploading {file_path.name} to {object_name}...")

            result = upload_file(
                local_path=str(file_path),
                object_name=object_name
            )
            uploaded_files.append(result)

        print(f"--- ✅ Successfully uploaded {len(uploaded_files)} HLS files for {local_mp3_path} ---")
        return uploaded_files
