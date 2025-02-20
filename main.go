package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"scraper-test/lib"
	"slices"

	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/extensions"
	"github.com/gocolly/colly/v2/queue"
	"github.com/hashicorp/go-retryablehttp"
	"golang.org/x/net/http2"
	"golang.org/x/net/publicsuffix"
	"golang.org/x/sync/semaphore"
)

type collectorConf struct {
	log           *slog.Logger
	maxDelay      time.Duration
	maxRetries    float64
	safeCounter   *lib.SiteCounter
	scrapeTimeout time.Duration
	searchDepth   int
	startUrl      string
}

type finsterHttpClient struct {
	c                 *retryablehttp.Client
	backoffMultiplier int
	sem               *semaphore.Weighted
}

type finsterCollector struct {
	baseUrl           string
	c                 *colly.Collector
	collected         []lib.Url
	log               *slog.Logger
	maxRetries        float64
	backoffMultiplier int
	mu                *sync.Mutex
	q                 *queue.Queue
	safeCounter       *lib.SiteCounter
	httpClient        *finsterHttpClient
	scrapeTimeout     time.Duration
	startUrl          *url.URL
	visitedMap        *lib.VisitedMap
}

func NewFinsterCollector(conf collectorConf) (*finsterCollector, error) {
	c := colly.NewCollector(
		// colly.Async(conf.async),
		colly.TraceHTTP(),
		colly.URLFilters(
			// allow any pdf to be scraped even if not on company domain
			regexp.MustCompile(".*.pdf"),
		),
	)

	retryClient := retryablehttp.NewClient()
	retryClient.RetryWaitMin = 50 * time.Millisecond
	retryClient.RetryWaitMax = 5 * time.Second
	retryClient.RetryMax = 3 // Number of retries
	retryClient.Logger = conf.log
	retryClient.RequestLogHook = func(l retryablehttp.Logger, req *http.Request, attemptNumber int) {
		// attemptNumber is 0 for the initial request, 1 for the first retry, etc.
		if attemptNumber > 0 {
			if attemptNumber == retryClient.RetryMax {
				// here we want to update the backoff for for the next request
			}
			l.Printf("Retry attempt #%d for %s", attemptNumber, req.URL)
		} else {
			l.Printf("First attempt to %s", req.URL)
		}
	}

	// Add a hook to log the response after each attempt
	retryClient.ResponseLogHook = func(logger retryablehttp.Logger, resp *http.Response) {
		if resp != nil {
			logger.Printf("ResponseLogHook: received status %d from %s",
				resp.StatusCode, resp.Request.URL.String())
		} else {
			logger.Printf("ResponseLogHook: received nil response")
		}
	}

	// Create a custom HTTP transport
	transport := &http.Transport{
		// You can add additional settings here if needed.
		// TLSClientConfig: &tls.Config{...},
	}
	// Explicitly enable HTTP/2 on the custom transport.
	if err := http2.ConfigureTransport(transport); err != nil {
		log.Fatalf("Error configuring HTTP/2: %v", err)
	}

	// timout at 2 seconds, the most common thing to slow this scraper down is a slow website
	c.SetRequestTimeout(5 * time.Second)
	parsedUrl, err := url.Parse(conf.startUrl)
	if err != nil {
		return nil, fmt.Errorf("invalid starturl %w", err)
	}

	host, err := publicsuffix.EffectiveTLDPlusOne(parsedUrl.Hostname())
	if err != nil {
		log.Fatal(err)
	}
	var re *regexp.Regexp
	// Split by "." and get the first part (e.g., "novonordisk")
	parts := strings.Split(host, ".")
	var baseUrl string
	if len(parts) > 0 {
		baseUrl = parts[0]
		// allow (anything .) || (//) company base url (.anything)
		re, err = regexp.Compile(fmt.Sprintf(`.*(\.|\/\/)%s.*`, parts[0]))
		if err != nil {
			return nil, fmt.Errorf("error setting up regex for url before scrape: %w", err)
		}
	} else {
		return nil, errors.New("could not extract name from host")
	}

	c.URLFilters = append(c.URLFilters, re)

	// this essentially adds a delay after a request
	c.Limit(&colly.LimitRule{
		// DomainGlob:  "*",
		RandomDelay: conf.maxDelay,
	})

	// create a request queue with 2 consumer threads
	q, _ := queue.New(
		1, // Number of consumer threads
		&queue.InMemoryQueueStorage{MaxSize: 10000},
	)

	c.MaxDepth = conf.searchDepth

	// use random user agent on each website, will change on each scrape
	extensions.RandomUserAgent(c)

	hc := &finsterHttpClient{
		c:                 retryClient,
		sem:               semaphore.NewWeighted(1),
		backoffMultiplier: 0,
	}

	fc := &finsterCollector{
		mu:                &sync.Mutex{},
		c:                 c,
		log:               conf.log.With(slog.String("startUrl", conf.startUrl), slog.String("baseUrl", baseUrl)),
		scrapeTimeout:     conf.scrapeTimeout,
		startUrl:          parsedUrl,
		baseUrl:           baseUrl,
		backoffMultiplier: 2,
		safeCounter:       conf.safeCounter,
		maxRetries:        conf.maxRetries,
		visitedMap:        lib.NewVisitedMap(),
		httpClient:        hc,
		collected:         []lib.Url{},
		q:                 q,
	}

	fc.handleCallbacks()
	// Add to the WaitGroup before starting a goroutine
	return fc, nil
}

func main() {
	profileMem()

	startTime := time.Now()
	var (
		maxConcurrentScrapes int
		searchDepth          int
		maxDelay             time.Duration
		scrapeTimeout        time.Duration
		maxRetries           float64
		urlList              string
		filePath             string
		env                  string
	)
	flag.IntVar(&maxConcurrentScrapes, "max_concurrent_scrape", 4, "how many sites to scrape at once")
	flag.IntVar(&searchDepth, "depth", 0, "how many layers of links to search through, if 0 will search entire site")
	flag.Float64Var(&maxRetries, "max_retries", 3, "maximum request retries")
	flag.DurationVar(&maxDelay, "max_delay", 2*time.Second, "maximum value for random request delay in millisecods")
	flag.DurationVar(&scrapeTimeout, "site_scrape_timeout", 25*time.Minute, "amount of time in minutes to scrape a site for before writing to file")
	flag.StringVar(&urlList, "url_list", "", "a semicolon delimited list of site urls, if empty, will try to find list file")
	flag.StringVar(&filePath, "file_path", "./company_IR_urls.txt", "path to file containing semicolon delimited list of site urls")
	flag.StringVar(&env, "env", "local", "environment local or prod/dev")

	flag.Parse()

	var logger *slog.Logger
	if env == "local" {
		// explicitly allow debug level logs in local
		// logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
		logger = slog.New(slog.NewTextHandler(os.Stdout, nil))
	} else {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}

	// create a safeCounter to track sites visited and sites blocked
	counter := lib.NewSiteCounter()

	if maxConcurrentScrapes == 0 {
		logger.Error("max_concurrent_scrape value must be above 0")
		return
	}

	var urlStr string
	if urlList != "" {
		urlStr = urlList
	} else {
		file, err := os.ReadFile(filePath)
		if err != nil {
			logger.Error("error reading file", slog.Any("error", err))
			return
		}
		urlStr = string(file)
	}

	urls := strings.Split(urlStr, ";")
	slices.Reverse(urls)
	logger.Info("total sites to scrape", slog.Int("total", len(urls)))

	if len(urlStr) < 1 {
		logger.Error("no urls provided")
		return
	}

	// Use a WaitGroup to wait for all goroutines to finish
	wg := &sync.WaitGroup{}

	// Semaphore to limit the number of concurrent sites being scraped, for the fast scraper
	concurrencyLimit := maxConcurrentScrapes
	semaphore := make(chan struct{}, concurrencyLimit)

	// Channel to signal when WaitGroup is done
	done := make(chan struct{})

	// Start a goroutine that waits for the WaitGroup
	go func() {
		wg.Wait()
		close(done) // Signal that all goroutines are done
	}()

	for _, url := range urls {
		fc, err := NewFinsterCollector(collectorConf{
			log:           logger,
			maxDelay:      maxDelay,
			maxRetries:    maxRetries,
			safeCounter:   counter,
			scrapeTimeout: scrapeTimeout,
			searchDepth:   searchDepth,
			startUrl:      url,
		})
		if err != nil {
			logger.Error("error setting up collector", slog.String("baseUrl", url))
			continue
		}

		wg.Add(1)
		go fc.run(wg, semaphore) // Run scrapeSite in a goroutine
	}

	// Wait for all the goroutines to finish or timeout to finish
	wg.Wait()

	logger.Info("scrape finished", slog.Duration("total_time", time.Since(startTime)), slog.Int("scaped_html", counter.TotalCount()-counter.TotalBlocked()), slog.Int("scaped_headless", counter.TotalBlocked()))
}

func (fc *finsterCollector) handleCallbacks() {
	// on every html element that has an href
	fc.c.OnHTML("*[href]", func(e *colly.HTMLElement) {
		// get the link from the element
		link := e.Attr("href")

		newUrl, err := url.Parse(e.Request.AbsoluteURL(link))
		if err != nil {
			fc.log.Error("error parsing new url", slog.Any("error", err))
			return
		}

		currentUrl, err := url.Parse(e.Request.URL.String())
		if err != nil {
			fc.log.Error("error parsing current url", slog.Any("error", err))
			return
		}

		// if we have not previously visited the link
		if !fc.visitedMap.WasVisited(newUrl.String()) {
			rel := e.Attr("rel")

			// skip stylesheets, loads of pages to be avoided with this
			if strings.Contains(rel, "stylesheet") || strings.Contains(rel, "icon") || strings.Contains(rel, "dns-prefetch") || strings.Contains(rel, "preconnect") || strings.Contains(rel, "preload") {
				return
			}

			// check its not an image type by extension
			if strings.HasSuffix(newUrl.String(), ".css") || strings.HasSuffix(newUrl.String(), ".js") || strings.HasSuffix(newUrl.String(), ".ico") ||
				strings.HasSuffix(newUrl.String(), ".png") || strings.HasSuffix(newUrl.String(), ".svg") || strings.HasSuffix(newUrl.String(), ".jpg") ||
				strings.HasSuffix(newUrl.String(), ".jpeg") || strings.HasSuffix(newUrl.String(), ".gif") || strings.HasSuffix(newUrl.String(), ".bmp") ||
				strings.HasSuffix(newUrl.String(), ".webp") || strings.HasSuffix(newUrl.String(), ".tiff") || strings.HasSuffix(newUrl.String(), ".avif") {
				return
			}

			if newUrl.Scheme != "https" && newUrl.Scheme != "http" {
				if newUrl.String() != "" {
					fc.log.Debug("sheme: ", slog.String("scheme", newUrl.Scheme), slog.String("currentUrl", currentUrl.String()), slog.String("url", newUrl.String()))
				}
				fc.visitedMap.Visited(newUrl.String())
				return
			}

			social, err := lib.IsSocialDomain(newUrl.String())
			if err != nil {
				fc.log.Error("error checking if new url is social link", slog.String("newUrl", newUrl.String()))
				fc.visitedMap.Visited(newUrl.String())
				return
			}
			if social {
				fc.visitedMap.Visited(newUrl.String())
				return
			}

			// if it's a pdf, add it to the list of found pdf's
			pdf, headers, err := fc.isPdf(newUrl.String())
			if err != nil {
				fc.log.Error("error checking if link is a pdf", slog.String("newUrl", newUrl.String()), slog.Any("error", err))
				if !errors.Is(err, io.EOF) && !pdf {
					fc.visitedMap.Visited(newUrl.String())
					return
				}
			}
			if pdf {
				fc.SafeAppend(lib.Url{
					ParentUrl:      e.Request.URL.String(),
					PdfUrl:         newUrl.String(),
					LastModified:   headers.Get("Last-Modified"),
					Etag:           headers.Get("ETag"),
					ExtractionTime: time.Now(),
				})
				// fc.log.Info("read ahead pdf found", slog.String("pdfUrl", newUrl.String()))
				fc.visitedMap.Visited(newUrl.String())
				return
			}

			// if it's a different subdomain
			same, err := lib.IsSameBaseDomain(newUrl.String(), fc.startUrl.String())
			if err != nil {
				return
			}
			if !same {
				// // if it's a pdf, add it to the list of found pdf's
				// pdf, headers, err := fc.isPdf(newUrl.String())
				// if err != nil {
				// 	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, io.EOF) {
				// 		fc.log.Error("error checking if link is a pdf", slog.String("newUrl", newUrl.String()), slog.Any("error", err))
				// 	}
				// 	fc.visitedMap.Visited(newUrl.String())
				// 	return
				// }
				// if pdf {
				// 	fc.SafeAppend(lib.Url{
				// 		ParentUrl:      e.Request.URL.String(),
				// 		PdfUrl:         newUrl.String(),
				// 		LastModified:   headers.Get("Last-Modified"),
				// 		Etag:           headers.Get("ETag"),
				// 		ExtractionTime: time.Now(),
				// 	})
				// 	// fc.log.Info("read ahead pdf found", slog.String("pdfUrl", newUrl.String()))
				// 	fc.visitedMap.Visited(newUrl.String())
				// 	return
				// } else {
				fc.visitedMap.Visited(newUrl.String())
				return
				// }
			}

			// s, _ := fc.q.Size()
			// fmt.Println("adding", newUrl.String(), s)

			if err := fc.addRequest(e.Request.URL, newUrl); err != nil {
				if !errors.Is(err, colly.ErrQueueFull) {
					fc.log.Error("error adding request", slog.Any("error", err))
				}
				return
			}

			fc.visitedMap.Visited(newUrl.String())
		}
	})

	fc.c.OnResponse(func(r *colly.Response) {
		// if we get a response straigt away, speed up the backoff
		retries := r.Ctx.GetAny("retriesLeft")
		if retries != nil {
			retriesLeft, ok := r.Ctx.GetAny("retriesLeft").(float64)
			if !ok {
				fc.log.Error("retries not float var")
			} else {
				if fc.backoffMultiplier > 2 && retriesLeft == fc.maxRetries {
					fc.backoffMultiplier--
					fmt.Println("speed up backoff", fc.backoffMultiplier)
				}
			}
		}
		if r.Headers.Get("Content-Type") == "application/pdf" {
			// fc.log.Info("pdf found", slog.String("parentUrl", r.Ctx.Get("parent")), slog.String("pdfUrl", r.Request.URL.String()))
			fc.collected = append(fc.collected, lib.Url{
				ParentUrl:      r.Ctx.Get("parent"),
				PdfUrl:         r.Request.URL.String(),
				LastModified:   r.Headers.Get("Last-Modified"),
				Etag:           r.Headers.Get("ETag"),
				ExtractionTime: time.Now(),
			})
		}
	})

	fc.c.OnError(func(r *colly.Response, err error) {
		firstRun := r.Request.URL.String() == fc.startUrl.String()
		if r.Headers != nil {
			// sometimes we get a pdf, that automatically downloads, this causes some errors, by checking and exiting out if a pdf is found we reduce this
			if r.Headers.Get("Content-Type") == "application/pdf" {
				// fc.log.Info("pdf found", slog.String("parentUrl", r.Ctx.Get("parent")), slog.String("pdfUrl", r.Request.URL.String()))
				fc.collected = append(fc.collected, lib.Url{
					ParentUrl:      r.Ctx.Get("parent"),
					PdfUrl:         r.Request.URL.String(),
					LastModified:   r.Headers.Get("Last-Modified"),
					Etag:           r.Headers.Get("ETag"),
					ExtractionTime: time.Now(),
				})
				fc.visitedMap.Visited(r.Request.URL.String())
				return
			} else if !strings.Contains(r.Headers.Get("Content-Type"), "text/html") {
				// if the next link is not an html response
				fc.log.Info("skipping retry", slog.String("retryUrl", r.Request.URL.String()))
				return
			}
		}

		if r.StatusCode != http.StatusNotFound && r.StatusCode != http.StatusBadRequest {
			retriesLeft, ok := r.Ctx.GetAny("retriesLeft").(float64)
			if !ok {
				fc.log.Error("retries not float var")
				return
			}

			if retriesLeft > 0 {
				delay := time.Duration(math.Pow(float64(fc.backoffMultiplier), float64(fc.maxRetries-retriesLeft))) * time.Second
				fc.log.Debug("Retrying request", slog.String("retry", r.Request.URL.String()), slog.Duration("delay", delay), slog.Float64("retriesLeft", retriesLeft), slog.Any("error", err))
				time.Sleep(delay)
				if firstRun && retriesLeft == 1 {
					fmt.Println("big retry")
					time.Sleep(time.Minute)
				}
				r.Ctx.Put("retriesLeft", retriesLeft-1)
				r.Request.Retry()
			} else {
				// backoff multiplier ++
				if fc.backoffMultiplier < 5 {
					fc.backoffMultiplier++
					fmt.Println("overall backoff increased:", fc.backoffMultiplier)
				}
				fc.log.Error("Error after retries", slog.Int("statusCode", r.StatusCode), slog.Any("headers", r.Headers), slog.String("reqUrl", r.Request.URL.String()), slog.Float64("retriesLeft", retriesLeft), slog.Any("error", err))
			}
		}
	})
}

func (fc *finsterCollector) run(wg *sync.WaitGroup, semaphore chan struct{}) {
	defer wg.Done() // Notify the WaitGroup when the function is done
	// Use the semaphore to limit the number of concurrent goroutines
	semaphore <- struct{}{}        // Acquire a slot in the semaphore (this will block if the channel is full)
	defer func() { <-semaphore }() // Release the slot in the semaphore when done

	if err := fc.addRequest(fc.startUrl, fc.startUrl); err != nil {
		fc.log.Error("error adding request", slog.Any("error", err))
		return
	}
	// start visiting sites through the queue

	// Channel to signal when WaitGroup is done
	done := make(chan struct{}) // Set your desired timeout
	fc.log.Info("scraping started")

	go func() {
		if err := fc.q.Run(fc.c); err != nil {
			// normally, on an error here, it means we were blocked by some form of bot detection
			// in that case I increment the blocked counter
			fc.log.Error("error on first call")

			// slowScrapeSemaphore <- struct{}{}        // Acquire a slot in the semaphore (this will block if the channel is full)
			// defer func() { <-slowScrapeSemaphore }() // Release the slot in the semaphore when done
			// fc.log.Info("starting slow scrape", slog.String("requestUrl", fc.startUrl), slog.Any("error", err))
			// finrod.SlowScrape(fc.startUrl)
		}

		// if we have no more to process in the queue, signal that we are done
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("All goroutines finished before timeout")
	case <-time.After(fc.scrapeTimeout):
		fmt.Println("Timeout reached, exiting")
	}

	if len(fc.collected) < 1 {
		// send to slow scraper
		fc.log.Error("rejected off rip")
		fc.safeCounter.Blocked()
		fc.log.Info("blocked counter", slog.Int("blocked", fc.safeCounter.TotalBlocked()))
		return
	} else {
		if err := lib.WriteJsonToFile(fc.collected, fc.baseUrl); err != nil {
			fc.log.Error("Error opening/creating file", slog.Any("error", err))
			return
		}
	}

	fc.log.Info("site scraped")

	// if we managed to get through to a site without getting blocked, increment
	fc.safeCounter.Increment()
}

func (fc *finsterCollector) SafeAppend(u lib.Url) {
	fc.mu.Lock()
	fc.collected = append(fc.collected, u)
	fc.mu.Unlock()
}

func (fc *finsterCollector) addRequest(currentUrl, newLink *url.URL) error {
	ctx := colly.NewContext()
	ctx.Put("parent", currentUrl.String())
	ctx.Put("retriesLeft", fc.maxRetries)
	h := &http.Header{}

	h.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")

	return fc.q.AddRequest(&colly.Request{
		URL:     newLink,
		Ctx:     ctx,
		Headers: h,
	})
}

func (fc *finsterCollector) isPdf(url string) (bool, http.Header, error) {
	// this handles looking ahead at links that are not on the same basepath, we can do this a lot quicker than the base path
	if err := fc.httpClient.sem.Acquire(context.Background(), 1); err != nil {
		// fmt.Printf("[Goroutine %d] failed to acquire semaphore: %v\n", id, err)
		return false, nil, err
	}
	// Ensure the semaphore is released when we're done
	defer fc.httpClient.sem.Release(1)

	// Introduce a random delay before making the request
	delay := time.Duration(fc.httpClient.backoffMultiplier) * time.Second
	fmt.Println("delay amount", delay)
	time.Sleep(delay)

	// Create a GET request with a Range header asking for the first 4 bytes
	req, err := retryablehttp.NewRequest("GET", url, nil)
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

	resp, err := fc.httpClient.c.Do(req)
	if err != nil {
		return false, nil, err
	}

	// close the body the moment we are done reading to free memory
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	// Either way, I only want to read up to 4 bytes from the response
	buf := make([]byte, 4)
	n, err := io.ReadFull(resp.Body, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return false, nil, err
	}
	buf = buf[:n]

	// Example check if it begins with "%PDF"
	if resp.Header.Get("Content-Type") == "application/pdf" {
		fmt.Println("found from headers")
		return true, resp.Header, err
	} else if bytes.HasPrefix(buf, []byte("%PDF")) {
		fmt.Println("found from magic bytes")
		return true, resp.Header, err
	}

	return false, resp.Header, nil
}
