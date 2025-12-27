package upload

import (
	"context"
	"fmt"
	"io"
	"kendb/db"
	"log/slog"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	video "cloud.google.com/go/videointelligence/apiv1"
	videopb "cloud.google.com/go/videointelligence/apiv1/videointelligencepb"
)

func UploadVideo(ctx context.Context,  userId string, videoName string, storageClient *storage.Client) (*db.Table, error) {
	fp := fmt.Sprintf("external/%s", videoName)
	f, err := os.Open(fp)
	if err != nil {
		slog.Error("Could not open file", "filepath", fp)
		return nil, err
	}
	defer f.Close()

	// use a 50 second timeout to not have long hanging requests
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	// TODO: add vidz-v1 storage bucket to gcs
	wc := storageClient.Bucket("vidz-v1").Object(fmt.Sprintf("%s/%s", userId, videoName)).NewWriter(ctx)
	if _, err := io.Copy(wc, f); err != nil {
		slog.Error("Could not write file to cloud storage", "filepath", fp, "gcs bucket", "vidz-v1")
		return nil, err
	}

	// close the writer or return an error
	if err := wc.Close(); err != nil {
		slog.Error("Could not close GCS storage writer")
		return nil, err
	}

	// once successfully written to gcs - we populate our DB
	db, err := db.InitDB(fmt.Sprintf("%s.ken", userId))
	if err != nil {
		return nil, err
	}

	// each column in a transcript embeddings table represents the sentence by sentence transcript
	// embeddings the video name that is the column name
	transcipts, ok := db.GetTableByName(fmt.Sprintf("%s_transcriptEmbeddings", userId))

	if !ok {
		// NOTE: arbritrarily setting 10 video max for videos per table
		transcipts, err = db.AddTable(fmt.Sprintf("%s_transcriptEmbeddings", userId), 10)
		if err != nil {
			return nil, err
		}
	}

	uri := fmt.Sprintf("gs://vidz-v1/%s/%s", userId, videoName)

	
}

func generateTrasncription(uri string, ctx context.Context) error {
	client, err := video.NewClient(ctx)
	if err != nil {
		slog.Error("Unable to instantiate video intelligence client")
		return err
	}
	defer client.Close()

	op, err := client.AnnotateVideo(ctx, &videopb.AnnotateVideoRequest{
		Features: []videopb.Feature{
			videopb.Feature_SPEECH_TRANSCRIPTION,
		},
		VideoContext: &videopb.VideoContext{
			SpeechTranscriptionConfig: &videopb.SpeechTranscriptionConfig{
				LanguageCode:               "en-US",
				EnableAutomaticPunctuation: true,
				MaxAlternatives: 1,
			},
		},
		InputUri: uri,
	})

	if err != nil {
		return err
	}
	resp, err := op.Wait(ctx)
	if err != nil {
		return err
	}

	// A single video was processed. Get the first result.
	result := resp.AnnotationResults[0]

	for _, transcription := range result.SpeechTranscriptions {
		// each transcription represents a block of the full video
		alt := transcription.GetAlternatives()[0] // NOTE: we set max alternatives to zero
		wordIdx := 0
		for _, sentence := range splitSentences(alt.Transcript) {
			wordCount := len(strings.Fields(sentence))
			if wordIdx+wordCount > len(alt.Words) {
				slog.Warn("Adding this sentence would take me past word count", "sentence", sentence)
				break
			}
			// TODO: align sentence with start+end time, embed and add to db
			// TODO: maybe add start and end time to db? (instead of just start time?)

		}
		
	}
}