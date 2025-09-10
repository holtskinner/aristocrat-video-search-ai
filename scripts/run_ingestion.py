# scripts/run_ingestion.py

import argparse
import json
import os
import sys
import tempfile
import time
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

import moviepy.editor as mp
from google.api_core import exceptions
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import speech_v2, storage
from google.cloud import videointelligence_v1p3beta1 as videointelligence

from .config import RECOGNIZER_ID, SUPPORTED_VIDEO_FORMATS
from .path_utils import get_derived_paths, parse_gcs_uri


# The following functions are correct and unchanged.
def extract_audio_from_video(
    video_uri: str, audio_uri: str, project_id: str, force_reextract: bool = False
) -> str:
    print(f"🎵 Extracting audio from video: {video_uri}")
    storage_client = storage.Client(project=project_id)
    bucket_name, video_blob_name = parse_gcs_uri(video_uri)
    _, audio_blob_name = parse_gcs_uri(audio_uri)
    bucket = storage_client.bucket(bucket_name)
    video_ext = f".{video_blob_name.split('.')[-1].lower()}"
    print(f"   Video format: {video_ext}")
    if video_ext not in SUPPORTED_VIDEO_FORMATS:
        raise ValueError(
            f"Video format '{video_ext}' is not in the supported list in config.py."
        )
    audio_blob = bucket.blob(audio_blob_name)
    if audio_blob.exists() and not force_reextract:
        print(f"✅ Audio file already exists: {audio_uri}")
        audio_blob.reload()
        print(f"   Size: {audio_blob.size / (1024 * 1024):.2f} MB")
        return audio_uri
    video_blob = bucket.blob(video_blob_name)
    if not video_blob.exists():
        raise FileNotFoundError(f"Video file not found: {video_uri}")
    video_blob.reload()
    video_size_mb = video_blob.size / (1024 * 1024) if video_blob.size else 0
    print("📥 Downloading video for audio extraction...")
    print(f"   Video size: {video_size_mb:.2f} MB")
    with tempfile.NamedTemporaryFile(suffix=video_ext, delete=False) as temp_video:
        start_time = time.time()
        video_blob.download_to_filename(temp_video.name)
        download_time = time.time() - start_time
        temp_video_path = temp_video.name
        print(f"   Downloaded in {download_time:.1f} seconds")
    temp_audio_fd, temp_audio_path = tempfile.mkstemp(suffix=".wav")
    os.close(temp_audio_fd)
    try:
        print("🔧 Extracting audio with moviepy...")
        video = mp.VideoFileClip(temp_video_path)
        if video.audio is None:
            raise ValueError(f"No audio track found in video: {video_uri}")
        print("   Converting to 16kHz mono WAV...")
        start_time = time.time()
        video.audio.write_audiofile(
            temp_audio_path,
            fps=16000,
            nbytes=2,
            codec="pcm_s16le",
            ffmpeg_params=["-ac", "1"],
            logger=None,
        )
        extraction_time = time.time() - start_time
        video.close()
        audio_size = os.path.getsize(temp_audio_path)
        print(
            f"   Audio extracted: {audio_size / (1024 * 1024):.2f} MB in {extraction_time:.1f} seconds"
        )
        if audio_size == 0:
            raise ValueError("Extracted audio file is empty")
        print("📤 Uploading audio to GCS...")
        start_time = time.time()
        audio_blob.upload_from_filename(temp_audio_path)
        upload_time = time.time() - start_time
        print(f"✅ Audio uploaded to: {audio_uri} in {upload_time:.1f} seconds")
    except Exception as e:
        print(f"❌ Error during audio extraction: {e}")
        raise
    finally:
        if "temp_video_path" in locals() and os.path.exists(temp_video_path):
            os.remove(temp_video_path)
            print("   Cleaned up temp video file")
        if os.path.exists(temp_audio_path):
            os.remove(temp_audio_path)
            print("   Cleaned up temp audio file")
    return audio_uri


def ensure_recognizer_exists(project_id: str, location: str = "global") -> str:
    print(f"🔍 Checking for recognizer in {location}...")
    client = (
        speech_v2.SpeechClient()
        if location == "global"
        else speech_v2.SpeechClient(
            client_options={"api_endpoint": f"{location}-speech.googleapis.com"}
        )
    )
    recognizer_name = (
        f"projects/{project_id}/locations/{location}/recognizers/{RECOGNIZER_ID}"
    )
    try:
        existing_recognizer = client.get_recognizer(name=recognizer_name)
        print(f"✅ Using existing recognizer: {RECOGNIZER_ID}")
        return recognizer_name
    except exceptions.NotFound:
        print(f"🆕 Recognizer '{RECOGNIZER_ID}' not found. Creating a new one...")
    recognizer = speech_v2.Recognizer(model="latest_long", language_codes=["en-US"])
    try:
        operation = client.create_recognizer(
            parent=f"projects/{project_id}/locations/{location}",
            recognizer=recognizer,
            recognizer_id=RECOGNIZER_ID,
        )
        created_recognizer = operation.result(timeout=60)
        print(f"✅ Created new recognizer: {created_recognizer.name}")
        return created_recognizer.name
    except GoogleAPICallError as e:
        print(f"❌ Error creating recognizer: {e}")
        raise


def _read_transcription_results_from_gcs(gcs_uri: str) -> list:
    storage_client = storage.Client()
    print(f"📖 Looking for results at: {gcs_uri}")
    if gcs_uri.endswith(".json"):
        bucket_name, blob_name = parse_gcs_uri(gcs_uri)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if blob.exists():
            content = blob.download_as_text()
            data = json.loads(content)
            return data.get("results", [])
        return []
    if not gcs_uri.endswith("/"):
        gcs_uri += "/"
    bucket_name, prefix = parse_gcs_uri(gcs_uri)
    bucket = storage_client.bucket(bucket_name)
    all_results = []
    for blob in bucket.list_blobs(prefix=prefix):
        if blob.name.endswith(".json") and blob.size > 0:
            try:
                data = json.loads(blob.download_as_text())
                if "results" in data:
                    all_results.extend(data.get("results", []))
            except Exception as e:
                print(f"   ❌ Error reading {blob.name}: {e}")
    return all_results


def transcribe_audio(
    recognizer_name: str, audio_uri: str, temp_output_uri: str
) -> list:
    print("🎙️ Starting transcription...")
    location = recognizer_name.split("/")[3]
    client = (
        speech_v2.SpeechClient()
        if location == "global"
        else speech_v2.SpeechClient(
            client_options={"api_endpoint": f"{location}-speech.googleapis.com"}
        )
    )
    config = speech_v2.RecognitionConfig(
        explicit_decoding_config=speech_v2.ExplicitDecodingConfig(
            encoding=speech_v2.ExplicitDecodingConfig.AudioEncoding.LINEAR16,
            sample_rate_hertz=16000,
            audio_channel_count=1,
        ),
        language_codes=["en-US"],
        model="latest_long",
        features=speech_v2.RecognitionFeatures(
            enable_word_time_offsets=True, enable_automatic_punctuation=True
        ),
    )
    request = speech_v2.BatchRecognizeRequest(
        recognizer=recognizer_name,
        config=config,
        files=[speech_v2.BatchRecognizeFileMetadata(uri=audio_uri)],
        recognition_output_config=speech_v2.RecognitionOutputConfig(
            gcs_output_config=speech_v2.GcsOutputConfig(uri=temp_output_uri)
        ),
    )
    try:
        operation = client.batch_recognize(request=request)
        print(f"\n📝 Transcription operation started: {operation.operation.name}")
        print(
            "\n⏳ Waiting for transcription to complete... Progress: ",
            end="",
            flush=True,
        )
        start_time, dots, last_update = time.time(), 0, time.time()
        while not operation.done():
            if time.time() - last_update > 30:
                print(
                    f"\n   Still processing... ({(time.time() - start_time) / 60:.1f} minutes elapsed)"
                )
                print("   Progress: ", end="", flush=True)
                dots = 0
                last_update = time.time()
            else:
                print(".", end="", flush=True)
                dots += 1
            time.sleep(10)

        print(
            f"\n\n✅ Transcription completed in {(time.time() - start_time) / 60:.1f} minutes"
        )
        if operation.exception():
            raise operation.exception()

        response = operation.result()
        if not response:
            return []

        all_results = []
        for file_uri, result in response.results.items():
            if result.uri:
                all_results.extend(_read_transcription_results_from_gcs(result.uri))
            elif result.error:
                print(f"❌ Error for file {file_uri}: {result.error.message}")
        return all_results
    except Exception as e:
        print(f"\n❌ Transcription error: {e}")
        raise


def extract_text_from_frames(video_uri: str) -> list:
    print("\n🔍 Starting OCR text extraction...")
    client = videointelligence.VideoIntelligenceServiceClient()
    request = videointelligence.AnnotateVideoRequest(
        input_uri=video_uri, features=[videointelligence.Feature.TEXT_DETECTION]
    )
    try:
        operation = client.annotate_video(request=request)
        print(f"   Operation started: {operation.operation.name}")
        print(
            "   Waiting for video annotation to complete... Progress: ",
            end="",
            flush=True,
        )
        start_time, dots, last_update = time.time(), 0, time.time()
        while not operation.done():
            if time.time() - last_update > 30:
                print(
                    f"\n   Still processing... ({(time.time() - start_time) / 60:.1f} minutes elapsed)"
                )
                print("   Progress: ", end="", flush=True)
                dots = 0
                last_update = time.time()
            else:
                print(".", end="", flush=True)
                dots += 1
            time.sleep(10)
        print(
            f"\n✅ Video annotation finished in {(time.time() - start_time) / 60:.1f} minutes"
        )
        annotations = operation.result().annotation_results
        all_text = [ann for res in annotations for ann in res.text_annotations]
        print(f"   Found {len(all_text)} text annotations")
        return all_text
    except Exception as e:
        print(f"❌ Video annotation error: {e}")
        raise


# ---- THIS IS THE CORRECTED FUNCTION ----
def consolidate_data(
    transcription_results: list, ocr_results: list, video_uri: str
) -> dict:
    print("\n🔧 Consolidating data into segments...")
    video_title = os.path.basename(video_uri)
    final_segments = []
    if not transcription_results:
        return {"video_title": video_title, "segments": []}

    for res in transcription_results:
        alt = res.get("alternatives", [{}])[0]
        transcript = alt.get("transcript", "").strip()
        words = alt.get("words", [])

        if not transcript or not words:
            continue

        # Use the correct v2 API keys: 'startOffset' and 'endOffset'
        start_time = float(words[0]["startOffset"].rstrip("s"))
        end_time = float(words[-1]["endOffset"].rstrip("s"))

        final_segments.append(
            {
                "speaker_tag": 0,
                "start_time_seconds": start_time,
                "end_time_seconds": end_time,
                "transcript": transcript,
                "slide_text": "",
            }
        )

    if ocr_results:
        for seg in final_segments:
            ocr_texts = {
                ocr.text
                for ocr in ocr_results
                if any(
                    s.segment.start_time_offset.total_seconds()
                    <= seg["end_time_seconds"]
                    and s.segment.end_time_offset.total_seconds()
                    >= seg["start_time_seconds"]
                    for s in getattr(ocr, "segments", [])
                )
            }
            seg["slide_text"] = " ".join(ocr_texts)

    final_segments.sort(key=lambda s: s["start_time_seconds"])
    print(f"✅ Created {len(final_segments)} segments")
    return {"video_title": video_title, "segments": final_segments}


# ---- END OF CORRECTED FUNCTION ----


def save_to_gcs(data: dict, output_uri: str):
    print(f"\n💾 Saving processed data to {output_uri}")
    bucket_name, blob_name = parse_gcs_uri(output_uri)
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(blob_name)
    json_content = json.dumps(data, indent=2)
    blob.upload_from_string(json_content, content_type="application/json")
    print("✅ Saved successfully.")


def main():
    parser = argparse.ArgumentParser(
        description="Process a video for the Video Search AI system."
    )
    parser.add_argument(
        "--video_uri", required=True, help="GCS URI of the raw video file."
    )
    parser.add_argument(
        "--gcp_project_id", required=True, help="Google Cloud Project ID."
    )
    parser.add_argument(
        "--gcp_location", default="global", help="GCP location for Speech-to-Text."
    )
    parser.add_argument("--skip_ocr", action="store_true", help="Skip OCR extraction.")
    parser.add_argument(
        "--skip_audio_extraction", action="store_true", help="Skip audio extraction."
    )
    parser.add_argument(
        "--force_reprocess",
        action="store_true",
        help="Force reprocessing of existing files.",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60 + "\n🎬 VIDEO SEARCH AI - INGESTION PIPELINE\n" + "=" * 60)

    try:
        paths = get_derived_paths(args.video_uri)
        output_uri = paths["json_uri"]
    except ValueError as e:
        print(f"❌ Invalid Video URI: {e}")
        sys.exit(1)

    print(f"📹 Video: {os.path.basename(paths['video_uri'])}")
    print(f"🎵 Audio Target: {paths['audio_uri']}")
    print(f"📄 Output Target: {output_uri}")
    print(f"🏢 Project: {args.gcp_project_id}, Location: {args.gcp_location}")
    print(f"🔍 OCR: {'❌ Disabled' if args.skip_ocr else '✅ Enabled'}")
    print("=" * 60 + "\n")

    pipeline_start = time.time()
    try:
        recognizer_name = ensure_recognizer_exists(
            args.gcp_project_id, args.gcp_location
        )

        audio_uri = paths["audio_uri"]
        if not args.skip_audio_extraction:
            audio_uri = extract_audio_from_video(
                paths["video_uri"],
                paths["audio_uri"],
                args.gcp_project_id,
                args.force_reprocess,
            )

        temp_transcription_uri = (
            f"gs://{paths['bucket_name']}/tmp/transcription/{paths['base_filename']}/"
        )
        transcription_results = transcribe_audio(
            recognizer_name, audio_uri, temp_transcription_uri
        )

        ocr_results = []
        if not args.skip_ocr:
            try:
                ocr_results = extract_text_from_frames(args.video_uri)
            except Exception as e:
                print(f"⚠️ OCR extraction failed: {e}. Continuing without OCR.")

        consolidated_data = consolidate_data(
            transcription_results, ocr_results, args.video_uri
        )
        save_to_gcs(consolidated_data, output_uri)

        pipeline_time = (time.time() - pipeline_start) / 60
        print("\n" + "=" * 60 + "\n✅ VIDEO INGESTION COMPLETE!\n" + "=" * 60)
        print(f"⏱️ Total processing time: {pipeline_time:.1f} minutes")
        print(f"📊 Results: {len(consolidated_data.get('segments', []))} segments")
        print(f"📄 Output saved to: {output_uri}\n" + "=" * 60 + "\n")

    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
