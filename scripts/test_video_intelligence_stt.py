# scripts/test_video_intelligence_stt.py
import argparse
from google.cloud import videointelligence

def track_people_and_transcribe(video_uri: str):
    """
    Performs person tracking and speech transcription on a video
    stored on GCS using the Video Intelligence API.

    Args:
        video_uri (str): The GCS URI of the video file.
    """
    print("\n" + "="*60)
    print("🎬 VIDEO INTELLIGENCE API - PERSON TRACKING & TRANSCRIPTION")
    print("="*60)
    print(f"📹 Video URI: {video_uri}")
    print("="*60 + "\n")

    try:
        video_client = videointelligence.VideoIntelligenceServiceClient()

        # --- MODIFIED: Added PERSON_DETECTION to the list of features ---
        features = [
            videointelligence.Feature.SPEECH_TRANSCRIPTION,
            videointelligence.Feature.PERSON_DETECTION,
        ]

        # Configure the speech transcription specific settings
        speech_config = videointelligence.SpeechTranscriptionConfig(
            language_code="en-US",
            enable_automatic_punctuation=True,
            # --- MODIFIED: Enabled speaker diarization to distinguish speakers ---
            enable_speaker_diarization=True,
            # You can optionally specify the number of speakers
            # diarization_speaker_count=2,
        )

        # --- ADDED: Configure the person detection feature ---
        person_config = videointelligence.PersonDetectionConfig(
            include_bounding_boxes=True,
            include_attributes=False,
            include_pose_landmarks=False,
        )

        # --- MODIFIED: Added person detection config to the video context ---
        video_context = videointelligence.VideoContext(
            speech_transcription_config=speech_config,
            person_detection_config=person_config,
        )

        print("🚀 Submitting video annotation request...")
        print("   (This can take a significant amount of time depending on video length)")

        # Start the annotation process
        operation = video_client.annotate_video(
            request={
                "features": features,
                "input_uri": video_uri,
                "video_context": video_context,
            }
        )

        print(f"   Operation started: {operation.operation.name}")
        print("⏳ Waiting for analysis to complete...")

        # Wait for the operation to complete
        result = operation.result(timeout=3600) # 1 hour timeout

        print("\n" + "="*60)
        print("✅ ANALYSIS COMPLETE!")
        print("="*60)

        # A single video is processed, so there's generally one result.
        annotation_results = result.annotation_results[0]

        # --- Process and Print Person Tracking Results ---
        if not annotation_results.person_detection_annotations:
            print("\n⚠️ No people detected in the video.")
        else:
            print("\n🧑‍🤝‍🧑 PERSON TRACKING RESULTS")
            print("-" * 30)
            person_annotations = annotation_results.person_detection_annotations
            for annotation in person_annotations:
                print(f"\nTrack ID: {annotation.track_id}")
                for tracked_object in annotation.tracks:
                    # Each track has multiple timestamped objects
                    for i, timestamped_object in enumerate(tracked_object.timestamped_objects):
                        if i > 4: # Limit output to first 5 sightings per track for brevity
                            print("  ...")
                            break
                        box = timestamped_object.normalized_bounding_box
                        time_offset = timestamped_object.time_offset.total_seconds()
                        print(
                            f"  - Time: {time_offset:.2f}s | "
                            f"Box: [L:{box.left:.2f}, T:{box.top:.2f}, R:{box.right:.2f}, B:{box.bottom:.2f}]"
                        )
            print("-" * 30)


        # --- Process and Print Speech Transcription Results ---
        if not annotation_results.speech_transcriptions:
            print("\n⚠️ No speech transcriptions found in the video.")
        else:
            print("\n🎙️ SPEECH TRANSCRIPTION RESULTS")
            print("-" * 30)
            for speech_transcription in annotation_results.speech_transcriptions:
                for i, alternative in enumerate(speech_transcription.alternatives):
                    print(f"\n--- TRANSCRIPT ALTERNATIVE #{i+1} (Confidence: {alternative.confidence:.2%}) ---")
                    print(f"Transcript: {alternative.transcript}\n")

                    print("Word-level details (with speaker tags):")
                    if not alternative.words:
                        print("  (No word-level information available)")
                        continue
                    
                    for word_info in alternative.words:
                        word = word_info.word
                        start_time = word_info.start_time.total_seconds()
                        end_time = word_info.end_time.total_seconds()
                        # --- MODIFIED: Added speaker_tag to the output ---
                        speaker_tag = word_info.speaker_tag
                        print(f"  {start_time:>7.2f}s - {end_time:>7.2f}s | Speaker #{speaker_tag}: {word}")
            print("-" * 30)


    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function to parse arguments."""
    parser = argparse.ArgumentParser(
        description="Test person tracking and speech transcription using the Video Intelligence API."
    )
    parser.add_argument(
        "--video_uri",
        type=str,
        required=True,
        help="The GCS URI of the video file (e.g., gs://my-bucket/raw/video.mp4).",
    )
    args = parser.parse_args()

    track_people_and_transcribe(args.video_uri)

if __name__ == "__main__":
    main()
