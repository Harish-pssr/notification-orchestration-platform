// Package migrations embeds the SQL migration files into the binaries, so a
// container needs nothing but its executable to migrate a database.
package migrations

import "embed"

// FS contains platform/*.sql and provider/*.sql.
// (go:embed paths must be inside this package's directory, which is why this
// file lives next to the SQL instead of under internal/.)
//
//go:embed platform/*.sql provider/*.sql
var FS embed.FS
