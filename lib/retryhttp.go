package lib

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"golang.org/x/sync/semaphore"
)

type RetryHttpClient struct {
	backoffMultiplier int
	c                 *retryablehttp.Client
	sem               *semaphore.Weighted
}

func NewRetryClient(log *slog.Logger) *RetryHttpClient {
	hc := &RetryHttpClient{
		backoffMultiplier: 0,
	}

	retryClient := retryablehttp.NewClient()
	retryClient.HTTPClient.Timeout = time.Second * 2
	retryClient.RetryWaitMin = 50 * time.Millisecond
	retryClient.RetryWaitMax = 5 * time.Second
	retryClient.RetryMax = 3 // Number of retries
	retryClient.Logger = log
	retryClient.RequestLogHook = func(l retryablehttp.Logger, req *http.Request, attemptNumber int) {
		// attemptNumber is 0 for the initial request, 1 for the first retry, etc.
		if attemptNumber > 0 {
			if attemptNumber == retryClient.RetryMax {
				// this conrols the delay for http retries
				hc.backoffMultiplier++
			}
			l.Printf("Retry attempt #%d for %s", attemptNumber, req.URL)
		} else {
			if hc.backoffMultiplier > 0 {
				hc.backoffMultiplier--
			}
			l.Printf("First attempt to %s", req.URL)
		}
	}

	hc.c = retryClient
	hc.sem = semaphore.NewWeighted(1)

	// Add a hook to log the response after each attempt
	retryClient.ResponseLogHook = func(logger retryablehttp.Logger, resp *http.Response) {
		if resp != nil {
			logger.Printf("ResponseLogHook: received status %d from %s",
				resp.StatusCode, resp.Request.URL.String())
		} else {
			logger.Printf("ResponseLogHook: received nil response")
		}
	}

	return hc
}

// isPdf looks ahead at a link, doing a request, and first checking the headers, then the first 4 bytes,
// to see if it is a pdf. If it is, it adds it to the list of found pdf's
func (r *RetryHttpClient) IsPdf(currentUrl, newUrl string) (bool, http.Header, error) {
	// this handles looking ahead at links that are not on the same basepath, we can do this a lot quicker than the base path
	if err := r.sem.Acquire(context.Background(), 1); err != nil {
		return false, nil, err
	}

	// Ensure the semaphore is released when we're done
	defer r.sem.Release(1)

	// Introduce a random delay before making the request, this backoffMultiplier is incremented or decremented based on failed/ passed requests
	delay := time.Duration(r.backoffMultiplier) * time.Second
	time.Sleep(delay)

	// Create a GET request with a Range header asking for the first 4 bytes
	req, err := retryablehttp.NewRequest("GET", newUrl, nil)
	if err != nil {
		return false, nil, err
	}

	// attempt to give the server a range of bytes to respond with
	req.Header.Set("Range", "bytes=0-3")

	// add headers to attempt to bypass bot protection
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Referer", "https://www.google.com/")
	req.Header.Set("DNT", "1")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("Cache-Control", "max-age=0")
	req.Header.Set("Sec-Ch-Ua", `"Google Chrome";v="121", "Chromium";v="121", "Not A(Brand";v="99"`)
	req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
	req.Header.Set("Sec-Ch-Ua-Platform", `"Windows"`)

	resp, err := r.c.Do(req)
	if err != nil {
		return false, nil, err
	}

	// close the body the moment we are done reading to free memory
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	// before reading the body, check if its a pdf from the headers
	if resp.Header.Get("Content-Type") == "application/pdf" {
		fmt.Println("found from headers")
		return true, resp.Header, nil
	}

	// fairly often, headers are not correct, but the file is still a pdf, here, if the
	// headers don't return application/pdf we check the first 4 bytes to see if it's a pdf
	buf := make([]byte, 4)
	n, err := io.ReadFull(resp.Body, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return false, resp.Header, err
	}
	buf = buf[:n]

	// check if it begins with "%PDF", if so return true
	if bytes.HasPrefix(buf, []byte("%PDF")) {
		fmt.Println("found from magic bytes")
		return true, resp.Header, nil
	}

	// otherwise no error and not pdf
	return false, resp.Header, nil
}
