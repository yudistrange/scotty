package beam

import (
	"fmt"
	beam "github.com/apache/beam/sdks/v2/go/pkg/beam"
)

func Initialize() {
	fmt.Println("Initializing Beam")
	beam.Init()
	beam.NewPipelineWithRoot()
}
