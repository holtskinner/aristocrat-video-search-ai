# scripts/test_video_intelligence_stt.py
import argparse

from google.cloud import videointelligence


def transcribe_with_video_intelligence(video_uri: str):
    """Transcribes speech from a video stored on GCS using the
    Video Intelligence API.

    Args:
        video_uri (str): The GCS URI of the video file.
    """
    print("\n" + "=" * 60)
    print("🎬 VIDEO INTELLIGENCE API - SPEECH TRANSCRIPTION TEST")
    print("=" * 60)
    print(f"📹 Video URI: {video_uri}")
    print("=" * 60 + "\n")

    try:
        video_client = videointelligence.VideoIntelligenceServiceClient()

        # Configure the transcription feature
        features = [videointelligence.Feature.SPEECH_TRANSCRIPTION]

        # Configure the speech transcription specific settings
        config = videointelligence.SpeechTranscriptionConfig(
            language_code="en-US",
            enable_automatic_punctuation=True,
            # Note: Diarization can also be enabled here if needed
            # enable_speaker_diarization=True,
            # diarization_speaker_count=2,
        )

        # Set the transcription config in the video context
        video_context = videointelligence.VideoContext(
            speech_transcription_config=config
        )

        print("🚀 Submitting video annotation request...")
        print(
            "   (This can take a significant amount of time, similar to the other method)"
        )

        # Start the annotation process
        operation = video_client.annotate_video(
            request={
                "features": features,
                "input_uri": video_uri,
                "video_context": video_context,
            }
        )

        print(f"   Operation started: {operation.operation.name}")
        print("⏳ Waiting for transcription to complete...")

        # Wait for the operation to complete
        result = operation.result(timeout=3600)  # 1 hour timeout

        print("\n" + "=" * 60)
        print("✅ TRANSCRIPTION COMPLETE!")
        print("=" * 60)

        # A single video is processed, so there's generally one result.
        annotation_results = result.annotation_results[0]

        if not annotation_results.speech_transcriptions:
            print("\n⚠️ No speech transcriptions found in the video.")
            return

        # Print the results
        for speech_transcription in annotation_results.speech_transcriptions:
            # Each transcription may have multiple alternatives.
            for i, alternative in enumerate(speech_transcription.alternatives):
                print(f"\n--- TRANSCRIPT ALTERNATIVE #{i + 1} ---")
                print(f"Confidence: {alternative.confidence:.2%}")
                print(f"Transcript: {alternative.transcript}\n")

                print("Word-level details:")
                if not alternative.words:
                    print(
                        "  (No word-level information available for this alternative)"
                    )
                    continue

                for word_info in alternative.words:
                    word = word_info.word
                    start_time = word_info.start_time.total_seconds()
                    end_time = word_info.end_time.total_seconds()
                    print(f"  {start_time:>7.2f}s - {end_time:>7.2f}s: {word}")

    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
        import traceback

        traceback.print_exc()


def main():
    """Main function to parse arguments."""
    parser = argparse.ArgumentParser(
        description="Test speech transcription using the Video Intelligence API."
    )
    parser.add_argument(
        "--video_uri",
        type=str,
        required=True,
        help="The GCS URI of the video file (e.g., gs://my-bucket/raw/video.mp4).",
    )
    args = parser.parse_args()

    transcribe_with_video_intelligence(args.video_uri)


if __name__ == "__main__":
    main()
