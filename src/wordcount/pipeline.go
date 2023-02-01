package wordcount

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

func initialize() {
	fmt.Println("Parsing flags")
	flag.Parse()
	fmt.Println(fmt.Sprintf("input: %s", *input))
	fmt.Println(fmt.Sprintf("output: %s", *output))
	fmt.Println("Initializing Beam")
	beam.Init()
}

func createPipeline() *beam.Pipeline {
	return beam.NewPipeline()
}

func createScope(pipeline *beam.Pipeline) beam.Scope {
	return pipeline.Root()
}

func readInput(scope beam.Scope) beam.PCollection {
	return textio.Read(scope, *input)
}

func parseWords(scope beam.Scope, lines beam.PCollection) beam.PCollection {
	return beam.ParDo(scope, func(line string, emit func(string)) {
		for _, word := range wordRE.FindAllString(line, -1) {
			emit(word)
		}
	}, lines)
}

func lowerCaseWords(scope beam.Scope, words beam.PCollection) beam.PCollection {
	return beam.ParDo(scope, func(word string, emit func(string)) {
		formatted_word := strings.ToLower(word)
		emit(formatted_word)
	}, words)
}

func countWordOccurance(scope beam.Scope, words beam.PCollection) beam.PCollection {
	return stats.Count(scope, words)
}

func formatOutput(scope beam.Scope, wordCounts beam.PCollection) beam.PCollection {
	return beam.ParDo(scope, func(w string, c int) string {
		return fmt.Sprintf("%s: %v", w, c)
	}, wordCounts)
}

func writeOutput(scope beam.Scope, wordListing beam.PCollection) {
	textio.Write(scope, *output, wordListing)
}

func Run() {
	initialize()
	pipeline := createPipeline()
	scope := createScope(pipeline)
	input := readInput(scope)
	words := parseWords(scope, input)
	formattedWords := lowerCaseWords(scope, words)
	wordListings := countWordOccurance(scope, formattedWords)
	formattedOutput := formatOutput(scope, wordListings)
	writeOutput(scope, formattedOutput)
	direct.Execute(context.Background(), pipeline)
}
