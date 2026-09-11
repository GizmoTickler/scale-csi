package main

import (
	"golang.org/x/tools/go/analysis/multichecker"
)

func main() {
	multichecker.Main(
		HardenedExec,
		BlkidFlag,
		MetricCardinality,
		WedgedGuard,
		DebugConfigEmbed,
	)
}
