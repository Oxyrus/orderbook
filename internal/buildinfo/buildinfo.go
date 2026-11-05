// Package buildinfo exposes version metadata injected at build time via
// -ldflags, falling back to the information embedded by the Go toolchain.
package buildinfo

import "runtime/debug"

// Overridden at build time, e.g.:
//
//	go build -ldflags "-X github.com/Oxyrus/orderbook/internal/buildinfo.Version=v1.2.3"
var (
	Version   = "dev"
	Commit    = ""
	BuildTime = ""
)

func init() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			if Commit == "" {
				Commit = s.Value
			}
		case "vcs.time":
			if BuildTime == "" {
				BuildTime = s.Value
			}
		}
	}
}

// Info is a serialisable snapshot of the build metadata.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit,omitempty"`
	BuildTime string `json:"build_time,omitempty"`
}

// Get returns the current build metadata.
func Get() Info { return Info{Version: Version, Commit: Commit, BuildTime: BuildTime} }
