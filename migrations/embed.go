// Package migrations embeds the SQL schema migrations into the binary so the
// service can migrate itself without shipping extra files.
//
// Files follow goose's naming convention: <version>_<description>.sql where
// version is a sequential, zero-padded number. Create new files with
// `make migration name=add_something`.
package migrations

import "embed"

// FS contains every *.sql migration in this directory.
//
//go:embed *.sql
var FS embed.FS
