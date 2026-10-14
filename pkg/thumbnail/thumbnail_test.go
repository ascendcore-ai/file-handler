package thumbnail

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"testing"
)

func TestComputeThumbnailPath(t *testing.T) {
	cases := []struct {
		original string
		folder   string
		expected string
	}{
		{"sunset.jpg", "thumbnails", "thumbnails/sunset.jpg"},
		{"sunset.jpg", "", "thumbnails/sunset.jpg"},
		{"photos/2026/sunset.jpg", "thumbnails", "photos/2026/thumbnails/sunset.jpg"},
		{"a/b/c/d/photo.png", "thumbs", "a/b/c/d/thumbs/photo.png"},
		{"documents/invoice.pdf", "thumbnails", "documents/thumbnails/invoice.pdf"},
	}

	for _, tc := range cases {
		got := ComputeThumbnailPath(tc.original, tc.folder)
		if got != tc.expected {
			t.Errorf("ComputeThumbnailPath(%q, %q) = %q; expected %q", tc.original, tc.folder, got, tc.expected)
		}
	}
}

func TestIsSupportedImage(t *testing.T) {
	if !IsSupportedImage("photo.jpg") {
		t.Fatal("expected .jpg to be supported")
	}
	if !IsSupportedImage("photo.JPEG") {
		t.Fatal("expected .JPEG to be supported")
	}
	if !IsSupportedImage("logo.png") {
		t.Fatal("expected .png to be supported")
	}
	if !IsSupportedImage("anim.gif") {
		t.Fatal("expected .gif to be supported")
	}
	if IsSupportedImage("document.pdf") {
		t.Fatal("expected .pdf to NOT be supported")
	}
	if IsSupportedImage("archive.zip") {
		t.Fatal("expected .zip to NOT be supported")
	}
}

func TestGenerateThumbnailJPEG(t *testing.T) {
	// Create an 800x600 test image
	srcImg := image.NewRGBA(image.Rect(0, 0, 800, 600))
	for y := 0; y < 600; y++ {
		for x := 0; x < 800; x++ {
			srcImg.Set(x, y, color.RGBA{R: uint8(x % 256), G: uint8(y % 256), B: 128, A: 255})
		}
	}

	buf := &bytes.Buffer{}
	if err := jpeg.Encode(buf, srcImg, nil); err != nil {
		t.Fatalf("failed to encode test image: %v", err)
	}

	// Generate thumbnail with maxDim 200
	thumbReader, w, h, format, err := Generate(context.Background(), buf, 200)
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	if format != "jpeg" {
		t.Fatalf("expected format jpeg, got %s", format)
	}
	if w != 200 {
		t.Fatalf("expected width 200, got %d", w)
	}
	if h != 150 { // 600 * 200 / 800 = 150
		t.Fatalf("expected height 150, got %d", h)
	}

	// Verify generated image is decodable
	decoded, _, err := image.Decode(thumbReader)
	if err != nil {
		t.Fatalf("failed to decode generated thumbnail: %v", err)
	}
	if decoded.Bounds().Dx() != 200 || decoded.Bounds().Dy() != 150 {
		t.Fatalf("decoded dimensions mismatch: %dx%d", decoded.Bounds().Dx(), decoded.Bounds().Dy())
	}
}

func TestGenerateThumbnailPNG(t *testing.T) {
	// Create a 300x600 test portrait image
	srcImg := image.NewRGBA(image.Rect(0, 0, 300, 600))
	for y := 0; y < 600; y++ {
		for x := 0; x < 300; x++ {
			srcImg.Set(x, y, color.RGBA{R: 50, G: 100, B: 200, A: 255})
		}
	}

	buf := &bytes.Buffer{}
	if err := png.Encode(buf, srcImg); err != nil {
		t.Fatalf("failed to encode test png: %v", err)
	}

	thumbReader, w, h, format, err := Generate(context.Background(), buf, 200)
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	if format != "png" {
		t.Fatalf("expected format png, got %s", format)
	}
	if h != 200 {
		t.Fatalf("expected height 200 for portrait image, got %d", h)
	}
	if w != 100 { // 300 * 200 / 600 = 100
		t.Fatalf("expected width 100 for portrait image, got %d", w)
	}

	decoded, _, err := image.Decode(thumbReader)
	if err != nil {
		t.Fatalf("failed to decode thumbnail: %v", err)
	}
	if decoded.Bounds().Dx() != 100 || decoded.Bounds().Dy() != 200 {
		t.Fatalf("decoded dimensions mismatch: %dx%d", decoded.Bounds().Dx(), decoded.Bounds().Dy())
	}
}
