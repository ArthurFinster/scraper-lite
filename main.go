package main

import (
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

	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/extensions"
	"github.com/gocolly/colly/v2/queue"
	"golang.org/x/net/publicsuffix"
)

type collectorConf struct {
	log           *slog.Logger
	maxDelay      time.Duration
	maxRetries    float64
	minCollected  int
	safeCounter   *lib.SiteCounter
	scrapeTimeout time.Duration
	httpTimeout   time.Duration
	collyTimeout  time.Duration
	searchDepth   int
}

type finsterCollector struct {
	backoffMultiplier int
	baseUrl           string
	c                 *colly.Collector
	retryClient       *lib.RetryHttpClient
	log               *slog.Logger
	maxRetries        float64
	minCollected      int
	q                 *queue.Queue
	safeCounter       *lib.SiteCounter
	scrapeTimeout     time.Duration
	startUrl          *url.URL
	siteMap           *lib.SiteMap
}

func main() {
	var (
		maxConcurrentScrapes int
		minCollected         int
		searchDepth          int
		maxDelay             time.Duration
		scrapeTimeout        time.Duration
		httpTimeout          time.Duration
		collyTimeout         time.Duration
		maxRetries           float64
		urlList              string
		filePath             string
		env                  string
	)
	flag.IntVar(&maxConcurrentScrapes, "max_concurrent_scrape", 4, "how many sites to scrape at once")
	flag.IntVar(&minCollected, "min_collected", 1, "minimum pdfs accepted, any amount below this number will be sent to the headless scraper")
	flag.IntVar(&searchDepth, "depth", 0, "how many layers of links to search through, if 0 will search entire site")
	flag.Float64Var(&maxRetries, "max_retries", 3, "maximum request retries")
	flag.DurationVar(&httpTimeout, "http_timeout", 3*time.Second, "timeout for requests made by the http client")
	flag.DurationVar(&collyTimeout, "colly_timeout", 2*time.Second, "timeout for requests made by the colly client")
	flag.DurationVar(&maxDelay, "max_delay", 2*time.Second, "maximum value for random request delay in millisecods")
	flag.DurationVar(&scrapeTimeout, "site_scrape_timeout", 25*time.Minute, "amount of time in minutes to scrape a site for before writing to file")
	flag.StringVar(&urlList, "url_list", "", "a semicolon delimited list of site urls, if empty, will try to find list file")
	flag.StringVar(&filePath, "file_path", "./company_IR_urls.txt", "path to file containing semicolon delimited list of site urls")
	flag.StringVar(&env, "env", "local", "environment local or prod/dev")

	flag.Parse()

	startTime := time.Now()
	var logger *slog.Logger

	if env == "local" {
		// explicitly allow debug level logs in local
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	} else {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	// we have to at least have one go routine running scraping sites
	if maxConcurrentScrapes == 0 {
		logger.Error("max_concurrent_scrape value must be above 0")
		return
	}

	// either use list provided, or file provided by user
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

	safeCounter := lib.NewSiteCounter()

	conf := collectorConf{
		log:           logger,
		maxDelay:      maxDelay,
		maxRetries:    maxRetries,
		minCollected:  minCollected,
		safeCounter:   safeCounter,
		scrapeTimeout: scrapeTimeout,
		searchDepth:   searchDepth,
	}

	for _, url := range urls {
		fc, err := NewFinsterCollector(url, conf)
		if err != nil {
			logger.Error("error setting up collector", slog.String("baseUrl", url))
			continue
		}

		wg.Add(1)
		go fc.run(wg, semaphore) // Run scrapeSite in a goroutine
	}

	// Wait for all the goroutines to finish or timeout to finish
	wg.Wait()

	logger.Info("scrape finished", slog.Duration("total_time", time.Since(startTime)), slog.Int("scaped_html", safeCounter.GetTotalViewed()-safeCounter.GetTotalBlocked()), slog.Int("scaped_headless", safeCounter.GetTotalBlocked()))
}

func NewFinsterCollector(startUrl string, conf collectorConf) (*finsterCollector, error) {
	c := colly.NewCollector(
		// colly.Async(conf.async),
		colly.URLFilters(
			// allow any pdf to be scraped even if not on company domain
			regexp.MustCompile(".*.pdf"),
		),
		colly.MaxDepth(conf.searchDepth),
	)

	// timout at 2 seconds, the most common thing to slow this scraper down is a slow website
	c.SetRequestTimeout(conf.collyTimeout)
	parsedUrl, err := url.Parse(startUrl)
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

	// use random user agent on each website, will change on each scrape
	extensions.RandomUserAgent(c)

	fc := &finsterCollector{
		baseUrl:           baseUrl,
		startUrl:          parsedUrl,
		log:               conf.log.With(slog.String("startUrl", startUrl), slog.String("baseUrl", baseUrl)),
		scrapeTimeout:     conf.scrapeTimeout,
		minCollected:      conf.minCollected,
		siteMap:           lib.NewSiteMap(),
		retryClient:       lib.NewRetryClient(conf.log, conf.httpTimeout),
		backoffMultiplier: 2,
		c:                 c,
		safeCounter:       conf.safeCounter,
		maxRetries:        conf.maxRetries,
		q:                 q,
	}

	fc.handleCallbacks()
	// Add to the WaitGroup before starting a goroutine
	return fc, nil
}

func (fc *finsterCollector) handleCallbacks() {
	// on every html element that has an href
	fc.c.OnHTML("*[href]", func(e *colly.HTMLElement) {
		// get the link from the element
		link := e.Attr("href")

		// parse the current link and the new link we found in the current link's html
		currentUrl, err := url.Parse(e.Request.URL.String())
		if err != nil {
			fc.log.Error("error parsing current url", slog.Any("error", err))
			return
		}

		newUrl, err := url.Parse(e.Request.AbsoluteURL(link))
		if err != nil {
			// using a warn here as this is fairly likely since we have not cleaned the link at this point
			fc.log.Warn("error parsing new url", slog.Any("error", err))
			return
		}

		// if we have not previously visited the link
		if !fc.siteMap.WasVisited(newUrl.String()) {
			rel := e.Attr("rel")
			// skip stylesheets, loads of pages to be avoided with this
			if !lib.IsValidRelTag(rel) {
				return
			}

			// check its not an image type by extension
			if !lib.IsValidFileExtension(newUrl.String()) {
				return
			}

			// check the scheme is http and not something like mailto
			if !lib.IsValidScheme(newUrl.Scheme) {
				return
			}

			// if the link is a social media link, we can assume it is not a pdf, or a link worth checking
			social, err := lib.IsSocialDomain(newUrl.String())
			if err != nil {
				fc.log.Error("error checking if new url is social link", slog.String("newUrl", newUrl.String()))
				fc.siteMap.Visited(newUrl.String())
				return
			}
			if social {
				fc.siteMap.Visited(newUrl.String())
				return
			}

			// here we perform a quick request using a normal retryable http client, this is quicker, and lighter than letting colly do a request and waiting for a full response
			// + html parse of the link. This link might not on the same base path, which is common for pdf's stored in some other location (cdn's etc)
			pdf, headers, err := fc.retryClient.IsPdf(currentUrl.String(), newUrl.String())
			if err != nil {
				fc.log.Error("error checking if link is a pdf", slog.String("newUrl", newUrl.String()), slog.Any("error", err))
				// isPdf can have an eof error but have still found out it's a pdf so we only want to say we've visited this link if the
				// error is not EOF and it's not a pdf
				if !errors.Is(err, io.EOF) && !pdf {
					fc.siteMap.Visited(newUrl.String())
					return
				}
			}
			if pdf {
				fc.log.Debug("read ahead pdf found", slog.String("pdfUrl", newUrl.String()))
				fc.siteMap.AppendPdf(lib.Url{
					ParentUrl:      currentUrl.String(),
					PdfUrl:         newUrl.String(),
					LastModified:   headers.Get("Last-Modified"),
					Etag:           headers.Get("ETag"),
					ExtractionTime: time.Now(),
				})
				return
			}

			// once we have checked the new link on a few simple cases, and validated that its not a pdf, we check the base domain
			// https://reports.companytoscrape.com == https://companytoscrape.com
			// https://someothercompany.com != https://companytoscrape.com
			same, err := lib.IsSameBaseDomain(newUrl.String(), fc.startUrl.String())
			if err != nil || !same {
				fc.siteMap.Visited(newUrl.String())
				return
			}

			// if the next link is good, on the same base domain, and is not a pdf, we add it to the queue for colly to process next
			if err := fc.addRequest(currentUrl, newUrl); err != nil {
				// this should not happen to often, as we prune the new links well
				if !errors.Is(err, colly.ErrQueueFull) {
					fc.log.Error("error adding request", slog.Any("error", err))
				}
				return
			}

			// if we added the request to the queue, add that this new link found has been visited
			fc.siteMap.Visited(newUrl.String())
		}
	})

	fc.c.OnResponse(func(r *colly.Response) {
		// if we get a response straight away, reduce the backoff multiplier, this speeds new requests up
		retries := r.Ctx.GetAny("retriesLeft")
		if retries != nil {
			retriesLeft, ok := r.Ctx.GetAny("retriesLeft").(float64)
			if !ok {
				fc.log.Error("retries not float var")
			} else {
				if fc.backoffMultiplier > 2 && retriesLeft == fc.maxRetries {
					fc.backoffMultiplier--
					fc.log.Debug("reducing backoff multiplier", slog.Int("backoff", fc.backoffMultiplier))
				}
			}
		}

		if r.Headers.Get("Content-Type") == "application/pdf" {
			// fc.log.Info("pdf found", slog.String("parentUrl", r.Ctx.Get("parent")), slog.String("pdfUrl", r.Request.URL.String()))
			fc.siteMap.AppendPdf(lib.Url{
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
				fc.siteMap.AppendPdf(lib.Url{
					ParentUrl:      r.Ctx.Get("parent"),
					PdfUrl:         r.Request.URL.String(),
					LastModified:   r.Headers.Get("Last-Modified"),
					Etag:           r.Headers.Get("ETag"),
					ExtractionTime: time.Now(),
				})
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
					time.Sleep(time.Minute * 2)
				}
				r.Ctx.Put("retriesLeft", retriesLeft-1)
				r.Request.Retry()
			} else {
				if fc.backoffMultiplier < 5 {
					fc.backoffMultiplier++
					fc.log.Debug("increasing backoff multiplier", slog.Int("backoff", fc.backoffMultiplier))
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
			fc.log.Error("error starting queue")
		}

		// if we have no more to process in the queue, signal that we are done
		close(done)
	}()

	// wait for either queue to be empty, or timeout
	select {
	case <-done:
		fc.log.Debug("All goroutines finished before timeout, exiting")
	case <-time.After(fc.scrapeTimeout):
		fc.log.Debug("Timeout reached, exiting")
	}

	pdfsFound := fc.siteMap.GetLinks()
	// if we collected less than the accepted minimum pdfs, increment blocked counter
	if len(pdfsFound) < fc.minCollected {
		fc.safeCounter.Blocked()
		fc.log.Info("blocked counter", slog.Int("blocked", fc.safeCounter.GetTotalBlocked()))
		return
	} else {
		// otherwise write to a file
		if err := lib.WriteJsonToFile(pdfsFound, fc.baseUrl); err != nil {
			fc.log.Error("Error opening/creating file", slog.Any("error", err))
			return
		}
	}

	fc.log.Info("site scraped")

	// if we managed to get through to a site without getting blocked, increment
	fc.safeCounter.Increment()
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
