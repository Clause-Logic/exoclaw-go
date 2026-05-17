package conversationfile

import (
	"os"
	"path/filepath"
	"testing"
)

// Ported from tests/test_conversation.py::{TestEnsureDir,TestSafeFilename,TestDetectImageMime}.

func TestEnsureDirCreatesAndReturns(t *testing.T) {
	tmp := t.TempDir()
	p := filepath.Join(tmp, "a", "b", "c")
	got, err := EnsureDir(p)
	if err != nil {
		t.Fatal(err)
	}
	if got != p {
		t.Fatalf("returned %q want %q", got, p)
	}
	info, err := os.Stat(p)
	if err != nil || !info.IsDir() {
		t.Fatalf("not a dir: %v %v", info, err)
	}
}

func TestSafeFilenameReplacesUnsafeChars(t *testing.T) {
	cases := map[string]string{
		"foo/bar":         "foo_bar",
		"a<b>c:d\"e/f\\g": "a_b_c_d_e_f_g",
		"  trim me  ":     "trim me",
		"clean":           "clean",
	}
	for in, want := range cases {
		if got := SafeFilename(in); got != want {
			t.Errorf("SafeFilename(%q) = %q want %q", in, got, want)
		}
	}
}

func TestDetectImageMime(t *testing.T) {
	cases := map[string][]byte{
		"image/png":  {0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n', 1, 2, 3},
		"image/jpeg": {0xff, 0xd8, 0xff, 0xe0, 0, 0x10},
		"image/gif":  []byte("GIF89a..."),
		"image/webp": append([]byte("RIFF"), append([]byte{0, 0, 0, 0}, []byte("WEBP")...)...),
	}
	for want, data := range cases {
		if got := DetectImageMIME(data); got != want {
			t.Errorf("DetectImageMIME for %s = %q", want, got)
		}
	}
	if got := DetectImageMIME([]byte("plain text")); got != "" {
		t.Fatalf("non-image got %q", got)
	}
}
