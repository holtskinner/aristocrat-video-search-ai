# scripts/batch_ingestion.py

import argparse
import os
import subprocess
import sys

from google.cloud import storage

from scripts.config import SUPPORTED_VIDEO_FORMATS as VIDEO_EXTENSIONS
from scripts.path_utils import get_derived_paths


def get_unprocessed_videos(bucket_name: str) -> tuple:
    # This function is correct and unchanged
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    print(f"\n🔍 Scanning gs://{bucket_name}/raw/ for video files...")
    print(f"   Supported formats: {', '.join(VIDEO_EXTENSIONS)}")
    raw_videos = [
        b.name
        for b in bucket.list_blobs(prefix="raw/")
        if any(b.name.lower().endswith(e) for e in VIDEO_EXTENSIONS)
    ]
    if not raw_videos:
        print("   ⚠️ No video files found.")
    print(
        f"\n🔍 Checking for already processed files in gs://{bucket_name}/processed_json/..."
    )
    processed_jsons = {
        os.path.splitext(os.path.basename(b.name))[0]
        for b in bucket.list_blobs(prefix="processed_json/")
        if b.name.endswith(".json")
    }
    if not processed_jsons:
        print("   No processed files found.")
    unprocessed_paths = []
    print("\n📝 Comparing raw videos against processed JSONs...")
    for video_path in raw_videos:
        video_uri = f"gs://{bucket_name}/{video_path}"
        paths = get_derived_paths(video_uri)
        json_base_name = os.path.splitext(os.path.basename(paths["json_uri"]))[0]
        if json_base_name not in processed_jsons:
            unprocessed_paths.append(video_path)
            print(f"   ➕ To be processed: {os.path.basename(video_path)}")
        else:
            print(f"   ➖ Already processed: {os.path.basename(video_path)}")
    return unprocessed_paths, raw_videos


def list_bucket_contents(bucket_name: str):
    # This function is correct and unchanged
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    print(f"\n📦 Complete contents of gs://{bucket_name}/:\n" + "-" * 60)
    all_blobs = list(bucket.list_blobs())
    if not all_blobs:
        print("   ⚠️ Bucket is empty!")
    else:
        for blob in all_blobs:
            print(f"   {blob.name} ({(blob.size or 0) / (1024 * 1024):.2f} MB)")
    print("-" * 60)


def process_video(
    video_path: str, bucket_name: str, project_id: str, location: str, skip_ocr: bool
):
    """Process a single video by calling the run_ingestion.py script."""
    video_uri = f"gs://{bucket_name}/{video_path}"
    paths = get_derived_paths(video_uri)

    print(f"\n{'=' * 60}")
    print(f"📹 Processing: {paths['base_filename']}")
    print(f"   Input:  {paths['video_uri']}")
    print(f"{'=' * 60}")

    # --- MODIFICATION: The --output_uri argument is REMOVED from the command ---
    cmd = [
        sys.executable,
        "-m",
        "scripts.run_ingestion",
        "--video_uri",
        paths["video_uri"],
        "--gcp_project_id",
        project_id,
        "--gcp_location",
        location,
    ]
    if skip_ocr:
        cmd.append("--skip_ocr")

    try:
        with subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
        ) as proc:
            for line in proc.stdout:
                print(line, end="")
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, cmd)
        print(f"\n✅ Successfully processed: {paths['base_filename']}")
        return True
    except subprocess.CalledProcessError as e:
        print(
            f"\n❌ Failed to process {paths['base_filename']} with exit code {e.returncode}"
        )
        return False


# The main() function is correct and remains the same as the previous version.
def main():
    parser = argparse.ArgumentParser(
        description="Batch process all videos in a GCS bucket /raw folder."
    )
    parser.add_argument("--bucket_name", required=True, help="GCS bucket name.")
    parser.add_argument("--project_id", required=True, help="GCP Project ID.")
    parser.add_argument(
        "--location", default="global", help="GCP location for Speech-to-Text."
    )
    parser.add_argument(
        "--skip_ocr", action="store_true", help="Skip OCR extraction on all videos."
    )
    parser.add_argument(
        "--force_all",
        action="store_true",
        help="Process all videos, even if already processed.",
    )
    parser.add_argument(
        "--specific_video",
        help="Process only a specific video file name (e.g., 'my_video.mp4').",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Show debug information about bucket contents.",
    )
    args = parser.parse_args()
    print("\n" + "=" * 60 + "\n🎬 BATCH VIDEO INGESTION\n" + "=" * 60)
    print(f"📦 Bucket: {args.bucket_name}, 🏢 Project: {args.project_id}")
    print(f"🔍 OCR: {'Disabled' if args.skip_ocr else 'Enabled'}")
    print("=" * 60)
    try:
        if not storage.Client().bucket(args.bucket_name).exists():
            print(f"❌ Bucket {args.bucket_name} does not exist!")
            return
    except Exception as e:
        print(f"❌ Error accessing bucket {args.bucket_name}: {e}")
        return
    if args.debug:
        list_bucket_contents(args.bucket_name)
    if args.specific_video:
        video_path = f"raw/{args.specific_video}"
        if not storage.Client().bucket(args.bucket_name).blob(video_path).exists():
            print(f"❌ Video '{video_path}' not found in gs://{args.bucket_name}/")
            return
        videos_to_process = [video_path]
    else:
        if args.force_all:
            bucket = storage.Client().bucket(args.bucket_name)
            videos_to_process = [
                b.name
                for b in bucket.list_blobs(prefix="raw/")
                if any(b.name.lower().endswith(e) for e in VIDEO_EXTENSIONS)
            ]
            total_videos = videos_to_process
        else:
            videos_to_process, total_videos = get_unprocessed_videos(args.bucket_name)
        print(
            f"\n📊 Summary: Found {len(total_videos)} total videos. Need to process {len(videos_to_process)}."
        )
        if not videos_to_process:
            print("✅ All videos are already processed!")
            return
    print("\nVideos to process:")
    for video in videos_to_process:
        print(f"  - {os.path.basename(video)}")
    if (
        not args.specific_video
        and input("\nProceed with processing? (y/n): ").lower() != "y"
    ):
        print("Cancelled.")
        return
    successful, failed = 0, 0
    for i, video_path in enumerate(videos_to_process, 1):
        print(f"\n--- Processing video {i}/{len(videos_to_process)} ---")
        if process_video(
            video_path, args.bucket_name, args.project_id, args.location, args.skip_ocr
        ):
            successful += 1
        else:
            failed += 1
    print("\n" + "=" * 60 + "\n📊 BATCH PROCESSING COMPLETE\n" + "=" * 60)
    print(
        f"✅ Successful: {successful}, ❌ Failed: {failed}, 📁 Total: {len(videos_to_process)}"
    )
    print("=" * 60)


if __name__ == "__main__":
    main()
