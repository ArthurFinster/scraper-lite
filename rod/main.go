package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"scraper-test/lib"
	"strings"
	"sync"
	"time"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/launcher"
	"github.com/go-rod/rod/lib/proto"
)

var (
	maxOpenPages = 10
)

// var urls = "https://www.nestle.com/investors;https://www.sap.com/investors/en.html;https://www.ubs.com/global/en/investor-relations.html"
var urls = "https://www.nestle.com/investors"

func main() {
	for _, url := range strings.Split(urls, ";") {
		SlowScrape(url)
	}
}

func SlowScrape(startUrl string) {
	// For more info: https://pkg.go.dev/github.com/go-rod/rod/lib/launcher
	// u := l.MustLaunch()

	browser := rod.New().ControlURL(
		launcher.New().Headless(false).MustLaunch(),
	).MustConnect()

	timeoutContext, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	browser = browser.Context(timeoutContext)
	defer browser.Close()

	// open url
	// get all href tags and store in array
	// go through one by one
	semaphore := make(chan struct{}, maxOpenPages)
	wg := &sync.WaitGroup{}

	visitedMap := lib.NewVisitedMap()
	collected := &SafeArray{
		mu:  sync.Mutex{},
		arr: []lib.Url{},
	}
	wg.Add(1)
	ctx := browser.GetContext()
	visitedMap.Visited(startUrl)
	if err := ScrapePage(ctx, true, 2*time.Second, startUrl, startUrl, collected, visitedMap, wg, browser, semaphore); err != nil {
		fmt.Println(err)
	}

	fmt.Println("wait")
	<-ctx.Done()
	fmt.Println("write")
	if err := lib.WriteJsonToFile(collected.arr, startUrl); err != nil {
		fmt.Println("Error opening/creating file", slog.Any("error", err))
		return
	}
}

func ScrapePage(ctx context.Context, firstVisit bool, waitTime time.Duration, startUrl, url string, collected *SafeArray, visitedMap *lib.VisitedMap, wg *sync.WaitGroup, browser *rod.Browser, semaphore chan struct{}) error {
	defer wg.Done() // Notify the WaitGroup when the function is done
	// Use the semaphore to limit the number of concurrent goroutines
	semaphore <- struct{}{} // Acquire a slot in the semaphore (this will block if the channel is full)
	defer func() {
		<-semaphore
	}() // Release the slot in the semaphore when done

	var e proto.NetworkResponseReceived

	p, err := browser.Page(proto.TargetCreateTarget{
		URL: url,
	})
	if err != nil {
		fmt.Println("error-1", err)
		return nil
	}

	defer func() {
		if err := p.Close(); err != nil {
			fmt.Println("error closing page:", err)
		}
	}()

	wait := p.WaitEvent(&e)
	wait()

	p.WaitStable(waitTime)

	links, _ := p.Elements("*[href]")

	if firstVisit {
		checkBox, _ := p.Elements("input[type='checkbox']")
		links = append(checkBox, links...)
	}

	for _, link := range links {
		fmt.Println(link.String())
		fmt.Println(link.Text())
		attr, _ := link.Attribute("href")
		if attr == nil {
			fmt.Println("error0", err)
			continue
		}
		newLink, err := fixLink(startUrl, *attr)
		if err != nil {
			fmt.Println("error1", err)
			continue
		}
		if len(newLink) == 0 {
			fmt.Println("error2", err)
			continue
		}

		// check ahead, this is less overhead than opening a whole new page
		{
			c := http.Client{
				Timeout: 2 * time.Second,
			}
			res, err := c.Head(newLink)
			if err != nil {
				if !errors.Is(err, context.DeadlineExceeded) {
					fmt.Println("error during head request", err)
				}

				// important to break here as if the error is deadline exceeded, we don't want to check return headers as there are none
				break
			}
			contentType := res.Header.Get("Content-Type")
			// if it's a pdf, add it to the list of found pdf's
			if contentType == "application/pdf" {
				fmt.Println(newLink)
				collected.Append(lib.Url{
					ParentUrl:      url,
					PdfUrl:         newLink,
					LastModified:   res.Header.Get("Last-Modified"),
					Etag:           res.Header.Get("ETag"),
					ExtractionTime: time.Now(),
				})
				fmt.Println("checking ahead found pdf, skipping")
				visitedMap.Visited(newLink)
			}
		}

		if !visitedMap.WasVisited(newLink) && isValidLink(newLink) {
			if lib.IsSameBaseDomain(startUrl, newLink) {
				wg.Add(1)
				// fmt.Println(newLink)

				select {
				case <-ctx.Done():
					fmt.Println("erro3", err)
					break
				default:
					visitedMap.Visited(newLink)
					go ScrapePage(ctx, false, 50*time.Millisecond, startUrl, newLink, collected, visitedMap, wg, browser, semaphore)
				}
			}
		}
	}

	return nil
}

func fixLink(baseUrl, relative string) (string, error) {
	if strings.HasPrefix(relative, "#") {
		return "", nil
	}
	base, err := url.Parse(baseUrl)
	if err != nil {
		return "", err
	}

	absURL, err := base.Parse(relative)
	if err != nil {
		return "", err
	}
	absURL.Fragment = ""

	if absURL.Scheme != "https" {
		return "", nil
	}
	// if absURL.Scheme == "//" {
	// 	absURL.Scheme = r.URL.Scheme
	// }

	return absURL.String(), nil
}

func isValidLink(l string) bool {
	if strings.Contains(l, ".ico") || strings.Contains(l, ".css") {
		return false
	}
	return true
}

type SafeArray struct {
	mu  sync.Mutex
	arr []lib.Url
}

func (s *SafeArray) Append(a lib.Url) {
	s.mu.Lock()
	s.arr = append(s.arr, a)
	s.mu.Unlock()
}
