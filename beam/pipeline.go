package beam

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"flag"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/direct"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
)

var (
	input  = flag.String("input", "./assets/shakespeare.txt", "Input URI for the pipeline")
	output = flag.String("output", "", "Output file for writing the pipeline's result")
)

var (
	wordRE = regexp.MustCompile(`[a-zA-Z]+`)
)

func Initialize() {
	fmt.Println("Initializing Beam")
	flag.Parse()

	beam.Init()
	pipeline := beam.NewPipeline()
	scope := pipeline.Root()

	linesPCol := textio.Read(scope, *input)

	words := beam.ParDo(scope, func(line string, emit func(string)) {
		for _, word := range wordRE.FindAllString(line, -1) {
			emit(word)
		}
	}, linesPCol)

	formatted_words := beam.ParDo(scope, func(word string, emit func(string)) {
		formatted_word := strings.ToLower(word)
		emit(formatted_word)
	}, words)

	counted := stats.Count(scope, formatted_words)

	listings := beam.ParDo(scope, func(w string, c int) string {
		return fmt.Sprintf("%s: %v", w, c)
	}, counted)

	textio.Write(scope, *output, listings)

	direct.Execute(context.Background(), pipeline)
}
