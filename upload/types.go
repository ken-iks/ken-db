package upload

import (
	"regexp"
	"strings"
)

func splitSentences(text string) []string {
    re := regexp.MustCompile(`[.!?]+\s*`)
    sentences := re.Split(text, -1)
    // Filter empty strings
    var result []string
    for _, s := range sentences {
        if strings.TrimSpace(s) != "" {
            result = append(result, strings.TrimSpace(s))
        }
    }
    return result
}