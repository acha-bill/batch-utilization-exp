package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const baseURL = "http://localhost:1635"

type Batch struct {
	BatchID     string `json:"batchID"`
	Utilization int    `json:"utilization"`
	Expired     bool   `json:"expired"`
	Usable      bool   `json:"usable"`
}

func generateFile(size int) ([]byte, error) {
	b := make([]byte, size)
	_, err := rand.Read(b)
	return b, err
}

func prettyByteSize(b int) string {
	bf := float64(b)
	for _, unit := range []string{"", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"} {
		if math.Abs(bf) < 1024.0 {
			return fmt.Sprintf("%3.1f%sB", bf, unit)
		}
		bf /= 1024.0
	}
	return fmt.Sprintf("%.1fYiB", bf)
}

func log(f io.Writer, m ...any) {
	_, _ = fmt.Fprintln(f, time.Now().Format(time.RFC3339), fmt.Sprint(m...))
}

func getStamp(batchID string) (*Batch, error) {
	client := &http.Client{}
	req, err := http.NewRequest(http.MethodGet, baseURL+"/stamps/"+batchID, nil)
	if err != nil {
		return nil, err
	}
	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	var batch Batch
	err = json.Unmarshal(body, &batch)
	if err != nil {
		return nil, err
	}
	return &batch, nil
}

func uploadData(size int, batchID string, encrypt bool, deferred bool) error {
	b, err := generateFile(size)
	payload := bytes.NewReader(b)
	client := &http.Client{}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/bytes", payload)
	if err != nil {
		return err
	}
	req.Header.Add("Swarm-Postage-Batch-Id", batchID)
	req.Header.Add("Content-Type", "application/octet-stream")
	req.Header.Add("Swarm-Deferred-Upload", strconv.FormatBool(deferred))
	req.Header.Add("Swarm-Encrypt", strconv.FormatBool(encrypt))

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	type uploadResponse struct {
		Reference string `json:"reference"`
	}
	var upload uploadResponse
	err = json.Unmarshal(body, &upload)
	if err != nil {
		return err
	}
	return nil
}

func run(name string, batchID string, stop <-chan error, encrypt, deferred bool) error {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer f.Close()

	const dataSize = 5 * 1024 * 1024

	batch := &Batch{
		BatchID: batchID,
		Usable:  false,
	}
	log(f, "batchID=", batch.BatchID)
	for !batch.Usable {
		log(f, "waiting for stamp to be usable")
		batch, err = getStamp(batch.BatchID)
		if err != nil {
			return fmt.Errorf("get stamp: %w", err)
		}
		time.Sleep(5 * time.Second)
	}

	totalUploaded := 0
	for {
		select {
		case v := <-stop:
			log(f, "stopping", v)
			return nil
		default:
			err = uploadData(dataSize, batch.BatchID, encrypt, deferred)
			if err != nil {
				return fmt.Errorf("upload data: %w", err)
			}

			batch, err = getStamp(batch.BatchID)
			if err != nil {
				return fmt.Errorf("get stamp: %w", err)
			}
			totalUploaded += dataSize
			log(f, "totalUploaded=", prettyByteSize(totalUploaded), " utilization=", batch.Utilization)
			if batch.Expired {
				log(f, "batch expired")
				return nil
			}
			if batch.Utilization == 16 {
				log(f, "batch full")
				return nil
			}
		}
	}
}

func main() {
	var wg sync.WaitGroup
	wg.Add(2)

	// stop both goroutines if one of them returns an error
	stop := make(chan error, 2)
	go func() {
		defer wg.Done()
		err := run("encrypted.log", "33061094e7281dbc29baf3b825d219d39c6999c8a11572863656225ad9bd287e", stop, true, false)
		if err != nil {
			stop <- fmt.Errorf("encrypted: %w", err)
			fmt.Println("encrypted err", err)
		}
	}()

	go func() {
		defer wg.Done()
		err := run("non-encrypted.log", "b7f8691f430db68104e5c92b8aaf2041bd99749fc1aeba44db77ab0a014b614b", stop, false, false)
		if err != nil {
			stop <- fmt.Errorf("non-encrypted: %w", err)
			fmt.Println("non-encrypted err", err)
		}
	}()

	wg.Wait()
}
