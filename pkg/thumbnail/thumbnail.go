package thumbnail

import (
	"context"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"path"
	"path/filepath"
	"strings"
)

// DefaultThumbnailFolder is the standard subfolder name for generated thumbnails.
const DefaultThumbnailFolder = "thumbnails"

// DefaultMaxDimension is the standard max width/height for thumbnails in pixels.
const DefaultMaxDimension = 200

// MaxImageDimensionLimit protects against decompression bombs (pixel bombs).
const MaxImageDimensionLimit = 16384

// Concurrency limiter to prevent memory spikes during simultaneous image resizing.
var resizeSem = make(chan struct{}, 4)

// Result contains metadata for a generated thumbnail.
type Result struct {
	Path     string `json:"path"`
	Filename string `json:"filename"`
	Bytes    int64  `json:"bytes"`
	Width    int    `json:"width"`
	Height   int    `json:"height"`
	Format   string `json:"format"`
}

// IsSupportedImage returns true if the filename extension corresponds to a decodable image format.
func IsSupportedImage(filename string) bool {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".jpg", ".jpeg", ".png", ".gif":
		return true
	default:
		return false
	}
}

// ComputeThumbnailPath calculates the target relative path for a thumbnail.
// For example, given originalPath "photos/2026/sunset.jpg" and folder "thumbnails",
// it produces "photos/2026/thumbnails/sunset.jpg".
// For a root file "sunset.jpg", it produces "thumbnails/sunset.jpg".
func ComputeThumbnailPath(originalPath string, thumbFolder string) string {
	if strings.TrimSpace(thumbFolder) == "" {
		thumbFolder = DefaultThumbnailFolder
	}
	dir := path.Dir(originalPath)
	base := path.Base(originalPath)

	if dir == "." || dir == "/" || dir == "" {
		return path.Join(thumbFolder, base)
	}
	return path.Join(dir, thumbFolder, base)
}

// Generate resizes an image stream to fit within maxDim × maxDim, preserving aspect ratio.
// Encodes the resulting thumbnail into pipeWriter without whole-file memory buffering.
func Generate(ctx context.Context, src io.Reader, maxDim int) (thumbReader io.Reader, width int, height int, format string, err error) {
	if maxDim <= 0 {
		maxDim = DefaultMaxDimension
	}

	// Acquire semaphore to strictly bound concurrent image decodings in RAM
	select {
	case resizeSem <- struct{}{}:
		defer func() { <-resizeSem }()
	case <-ctx.Done():
		return nil, 0, 0, "", ctx.Err()
	}

	// Decode source image (streams directly from io.Reader, zero io.ReadAll)
	img, detectedFormat, err := image.Decode(src)
	if err != nil {
		return nil, 0, 0, "", fmt.Errorf("failed to decode image: %w", err)
	}

	bounds := img.Bounds()
	origW := bounds.Dx()
	origH := bounds.Dy()

	if origW <= 0 || origH <= 0 {
		return nil, 0, 0, "", errors.New("invalid image dimensions")
	}

	if origW > MaxImageDimensionLimit || origH > MaxImageDimensionLimit {
		return nil, 0, 0, "", fmt.Errorf("image dimensions (%dx%d) exceed safe limit (%d)",
			origW, origH, MaxImageDimensionLimit)
	}

	targetW, targetH := calculateDimensions(origW, origH, maxDim)
	resized := ResizeBilinear(img, targetW, targetH)

	// Stream encoded output via io.Pipe
	pr, pw := io.Pipe()

	go func() {
		var encErr error
		switch strings.ToLower(detectedFormat) {
		case "png":
			encErr = png.Encode(pw, resized)
		case "gif":
			encErr = gif.Encode(pw, resized, nil)
		default:
			encErr = jpeg.Encode(pw, resized, &jpeg.Options{Quality: 85})
		}
		_ = pw.CloseWithError(encErr)
	}()

	return pr, targetW, targetH, detectedFormat, nil
}

// calculateDimensions scales width and height to fit within maxDim preserving aspect ratio.
func calculateDimensions(origW, origH, maxDim int) (int, int) {
	if origW <= maxDim && origH <= maxDim {
		return origW, origH
	}

	if origW >= origH {
		newW := maxDim
		newH := int(float64(origH) * float64(maxDim) / float64(origW))
		if newH < 1 {
			newH = 1
		}
		return newW, newH
	}

	newH := maxDim
	newW := int(float64(origW) * float64(maxDim) / float64(origH))
	if newW < 1 {
		newW = 1
	}
	return newW, newH
}

// ResizeBilinear resizes an image using high-quality bilinear interpolation with zero external dependencies.
func ResizeBilinear(src image.Image, newW, newH int) *image.RGBA {
	dst := image.NewRGBA(image.Rect(0, 0, newW, newH))
	bounds := src.Bounds()
	oldW := bounds.Dx()
	oldH := bounds.Dy()

	if oldW == 0 || oldH == 0 || newW == 0 || newH == 0 {
		return dst
	}

	xRatio := float64(oldW) / float64(newW)
	yRatio := float64(oldH) / float64(newH)

	for y := 0; y < newH; y++ {
		srcY := float64(y) * yRatio
		y1 := int(srcY)
		y2 := y1 + 1
		if y2 >= oldH {
			y2 = oldH - 1
		}
		yWeight := srcY - float64(y1)

		for x := 0; x < newW; x++ {
			srcX := float64(x) * xRatio
			x1 := int(srcX)
			x2 := x1 + 1
			if x2 >= oldW {
				x2 = oldW - 1
			}
			xWeight := srcX - float64(x1)

			c11 := src.At(bounds.Min.X+x1, bounds.Min.Y+y1)
			c21 := src.At(bounds.Min.X+x2, bounds.Min.Y+y1)
			c12 := src.At(bounds.Min.X+x1, bounds.Min.Y+y2)
			c22 := src.At(bounds.Min.X+x2, bounds.Min.Y+y2)

			r11, g11, b11, a11 := c11.RGBA()
			r21, g21, b21, a21 := c21.RGBA()
			r12, g12, b12, a12 := c12.RGBA()
			r22, g22, b22, a22 := c22.RGBA()

			r := (1-xWeight)*(1-yWeight)*float64(r11) + xWeight*(1-yWeight)*float64(r21) +
				(1-xWeight)*yWeight*float64(r12) + xWeight*yWeight*float64(r22)
			g := (1-xWeight)*(1-yWeight)*float64(g11) + xWeight*(1-yWeight)*float64(g21) +
				(1-xWeight)*yWeight*float64(g12) + xWeight*yWeight*float64(g22)
			b := (1-xWeight)*(1-yWeight)*float64(b11) + xWeight*(1-yWeight)*float64(b21) +
				(1-xWeight)*yWeight*float64(b12) + xWeight*yWeight*float64(b22)
			a := (1-xWeight)*(1-yWeight)*float64(a11) + xWeight*(1-yWeight)*float64(a21) +
				(1-xWeight)*yWeight*float64(a12) + xWeight*yWeight*float64(a22)

			dst.SetRGBA(x, y, color.RGBA{
				R: uint8(uint32(r) >> 8),
				G: uint8(uint32(g) >> 8),
				B: uint8(uint32(b) >> 8),
				A: uint8(uint32(a) >> 8),
			})
		}
	}

	return dst
}
