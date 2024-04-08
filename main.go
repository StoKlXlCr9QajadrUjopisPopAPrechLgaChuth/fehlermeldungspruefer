package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var inputFile = "./csv/input.csv"
var outputFile = "./csv/output.csv"
var domain = "dx.doi.org/"
var threads = 250

func main() {
	fmt.Println("Starting CSV check")

	altData, err := os.ReadFile(inputFile)
	errorCheck(err)

	altDataArray := strings.Split(string(altData), "\r\n")

	var wg sync.WaitGroup
	wg.Add(threads)

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	outputFileMutex := sync.Mutex{} // Mutex for file writes

	for i := 0; i < threads; i++ {
		go func(workerID int) {
			defer wg.Done()
			Runtime(altDataArray, client, workerID, threads, outputFile, &outputFileMutex)
		}(i)
	}

	wg.Wait()
	fmt.Println("Finished CSV check")
}

func errorCheck(err error) {
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func Runtime(altDataArray []string, client *http.Client, workerID int, step int, outputFile string, outputFileMutex *sync.Mutex) {
	for i := workerID; i < len(altDataArray); i += step {
		fmt.Printf("Worker %d: Checking row %d of %d\n", workerID, i+1, len(altDataArray))

		if i >= len(altDataArray) {
			break // Exit the loop if index exceeds array length
		}

		buildedURL := "https://" + domain + strings.Split(altDataArray[i], ";")[4]
		fmt.Printf("Worker %d: Built URL: %s\n", workerID, buildedURL)

		request, err := http.NewRequest("GET", buildedURL, nil)
		if err != nil {
			fmt.Printf("Worker %d: Error creating request: %v\n", workerID, err)
			continue
		}

		response, err := client.Do(request)
		if err != nil {
			fmt.Printf("Worker %d: Error sending request: %v\n", workerID, err)
			continue
		}

		defer response.Body.Close()

		var finalStatusCode int
		var finalRedirectURL string

		if response.StatusCode == http.StatusMovedPermanently || response.StatusCode == http.StatusFound {
			finalRedirectURL = response.Header.Get("Location")
			fmt.Printf("Worker %d: Initial Redirect URL: %s\n", workerID, finalRedirectURL)

			// Follow redirect to get final status code
			if finalRedirectURL != "" {
				finalResponse, err := client.Get(finalRedirectURL)
				if err != nil {
					fmt.Printf("Worker %d: Error getting final redirect: %v\n", workerID, err)
					finalStatusCode = http.StatusInternalServerError // Default error status
				} else {
					defer finalResponse.Body.Close()
					finalStatusCode = finalResponse.StatusCode
				}
			} else {
				finalStatusCode = http.StatusInternalServerError // Default error status
			}
		} else {
			finalStatusCode = response.StatusCode
		}


		// Write to output file

		if strings.Contains(buildedURL, ";") {
			buildedURL = "\"" + buildedURL + "\"\"\""
		}

		if strings.Contains(finalRedirectURL, ";") {
			finalRedirectURL = "\"" + finalRedirectURL + "\"\"\""
		}

		outputLine := fmt.Sprintf("%s;%d;%s;%d\n", buildedURL, response.StatusCode, finalRedirectURL, finalStatusCode)
		outputFileMutex.Lock()
		file, err := os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Printf("Worker %d: Error opening output file: %v\n", workerID, err)
			outputFileMutex.Unlock()
			continue
		}
		defer file.Close()

		if _, err := file.WriteString(outputLine); err != nil {
			fmt.Printf("Worker %d: Error writing to file: %v\n", workerID, err)
		}
		outputFileMutex.Unlock()

		fmt.Printf("Worker %d: Checked and wrote result\n", workerID)

		time.Sleep(3 * time.Second) // Sleep for rate limiting
	}
}