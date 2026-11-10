module github.com/exergy-dev/twine/examples/tunsink

go 1.26.0

require (
	github.com/exergy-dev/twine v0.0.0
	github.com/twmb/franz-go v1.21.1
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20260504210720-963be6fd5989
	golang.org/x/sys v0.50.0
)

require (
	github.com/klauspost/compress v1.20.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	golang.org/x/crypto v0.50.0 // indirect
)

replace github.com/exergy-dev/twine => ../..
