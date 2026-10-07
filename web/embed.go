// Package web embeds the dashboard so the API binary serves it with no extra files.
package web

import "embed"

// FS holds the dashboard files.
//
//go:embed index.html app.css app.js
var FS embed.FS
