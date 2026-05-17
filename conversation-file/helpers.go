// Package conversationfile is a file-backed Conversation implementation.
//
// Ported from exoclaw-plugins/packages/exoclaw-conversation. File layout
// mirrors the Python package one-to-one.
package conversationfile

import (
	"os"
	"regexp"
	"strings"
)

// Ported from exoclaw_conversation/helpers.py.

// EnsureDir ensures the directory exists, returning its path.
func EnsureDir(path string) (string, error) {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return "", err
	}
	return path, nil
}

var unsafeChars = regexp.MustCompile(`[<>:"/\\|?*]`)

// SafeFilename replaces unsafe path characters with underscores and trims
// leading/trailing whitespace.
func SafeFilename(name string) string {
	return strings.TrimSpace(unsafeChars.ReplaceAllString(name, "_"))
}

// DetectImageMIME detects an image MIME type from magic bytes, ignoring the
// file extension. Returns "" when no signature matches.
func DetectImageMIME(data []byte) string {
	if len(data) >= 8 && string(data[:8]) == "\x89PNG\r\n\x1a\n" {
		return "image/png"
	}
	if len(data) >= 3 && string(data[:3]) == "\xff\xd8\xff" {
		return "image/jpeg"
	}
	if len(data) >= 6 && (string(data[:6]) == "GIF87a" || string(data[:6]) == "GIF89a") {
		return "image/gif"
	}
	if len(data) >= 12 && string(data[:4]) == "RIFF" && string(data[8:12]) == "WEBP" {
		return "image/webp"
	}
	return ""
}
