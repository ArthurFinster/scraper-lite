package lib

import (
	"net/url"
	"strings"

	"golang.org/x/net/publicsuffix"
)

func IsValidRelTag(rel string) bool {
	if strings.Contains(rel, "stylesheet") || strings.Contains(rel, "icon") || strings.Contains(rel, "dns-prefetch") || strings.Contains(rel, "preconnect") || strings.Contains(rel, "preload") {
		return false
	}
	return true
}

func IsValidFileExtension(url string) bool {
	if strings.HasSuffix(url, ".css") || strings.HasSuffix(url, ".js") || strings.HasSuffix(url, ".ico") ||
		strings.HasSuffix(url, ".png") || strings.HasSuffix(url, ".svg") || strings.HasSuffix(url, ".jpg") ||
		strings.HasSuffix(url, ".jpeg") || strings.HasSuffix(url, ".gif") || strings.HasSuffix(url, ".bmp") ||
		strings.HasSuffix(url, ".webp") || strings.HasSuffix(url, ".tiff") || strings.HasSuffix(url, ".avif") {
		return false
	}
	return true
}

func IsValidScheme(scheme string) bool {
	return strings.Contains(scheme, "http")
}

func IsSameBaseDomain(u1, u2 string) (bool, error) {
	pu1, err := url.Parse(u1)
	if err != nil {
		return false, err
	}
	pu2, err := url.Parse(u2)
	if err != nil {
		return false, err
	}

	// Extract the effective top-level domain plus one (eTLD+1)
	domain1, err := publicsuffix.EffectiveTLDPlusOne(pu1.Hostname())
	if err != nil {
		return false, err
	}
	domain2, err := publicsuffix.EffectiveTLDPlusOne(pu2.Hostname())
	if err != nil {
		return false, err
	}

	domain1 = strings.Split(domain1, ".")[0]
	domain2 = strings.Split(domain2, ".")[0]

	if domain1 == domain2 {
		return true, nil
	}
	return false, nil
}

func IsSocialDomain(u1 string) (bool, error) {
	socialDomains := []string{"twitter", "facebook", "linkedin", ".x.com", "youtube", "instagram"}
	pu1, err := url.Parse(u1)
	if err != nil {
		return false, err
	}

	// Extract the effective top-level domain plus one (eTLD+1)
	domain1, err := publicsuffix.EffectiveTLDPlusOne(pu1.Hostname())
	if err != nil {
		return false, err
	}

	for _, social := range socialDomains {
		if strings.Contains(domain1, social) {
			return true, nil
		}
	}

	return false, nil
}
