// Package kafka extracts Kafka topology — topics, producers, consumers —
// from a repository by scanning source files for the canonical client-library
// patterns of each supported language. Topic names are extracted from string
// literals adjacent to producer/consumer constructors. The resulting fragment
// emits one KafkaTopic node per unique topic, plus PRODUCES/CONSUMES edges
// from the repository to each topic.
//
// This is a strictly heuristic extractor — Kafka clients pass topic names
// through many indirection layers (config files, env vars, constants).
// Confidence on every edge is INFERRED, and the fragment carries a topic
// list operators can cross-reference against runtime metrics.
package kafka

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"graph-platform/internal/extract"
)

type Extractor struct {
	MaxFileBytes int64
}

func New() *Extractor { return &Extractor{MaxFileBytes: 2 * 1024 * 1024} }

func (e *Extractor) Name() string { return "kafka" }

// patternSet groups the regexes for one language. Each entry's "Role" is
// PRODUCES, CONSUMES, or TOPIC (topic-list reference, treated as both unless
// adjacent producer/consumer context resolves it).
type patternSet struct {
	produces []*regexp.Regexp
	consumes []*regexp.Regexp
	topics   []*regexp.Regexp // any string-literal topic reference
}

var (
	// Go: sarama, segmentio/kafka-go, confluent-kafka-go.
	goPatterns = patternSet{
		produces: []*regexp.Regexp{
			regexp.MustCompile(`(?:sarama\.NewSyncProducer|sarama\.NewAsyncProducer|kafka\.Writer|NewProducer)\b`),
			regexp.MustCompile(`ProducerMessage\s*\{[^}]*Topic\s*:\s*"([^"]+)"`),
			regexp.MustCompile(`Writer\s*\{[^}]*Topic\s*:\s*"([^"]+)"`),
		},
		consumes: []*regexp.Regexp{
			regexp.MustCompile(`(?:sarama\.NewConsumer|sarama\.NewConsumerGroup|kafka\.NewReader|NewConsumer)\b`),
			// segmentio/kafka-go: kafka.NewReader(kafka.ReaderConfig{Topic: "..."}) — the
			// pattern matches both ReaderConfig{} and bare Reader{} initializers.
			regexp.MustCompile(`Reader(?:Config)?\s*\{[^}]*Topic\s*:\s*"([^"]+)"`),
			regexp.MustCompile(`ConsumeClaim\s*\([^)]*"([^"]+)"`),
		},
		topics: []*regexp.Regexp{
			regexp.MustCompile(`"([a-z0-9][a-z0-9._\-]+_(?:events|topic|requests|responses|stream))"`),
		},
	}
	// Java/Kotlin/Scala: KafkaProducer/KafkaConsumer, Spring Kafka @KafkaListener.
	jvmPatterns = patternSet{
		produces: []*regexp.Regexp{
			regexp.MustCompile(`new\s+KafkaProducer\b`),
			regexp.MustCompile(`KafkaTemplate\b`),
			regexp.MustCompile(`\.send\s*\(\s*new\s+ProducerRecord\s*<[^>]*>\s*\(\s*"([^"]+)"`),
			regexp.MustCompile(`\.send\s*\(\s*"([^"]+)"`),
		},
		consumes: []*regexp.Regexp{
			regexp.MustCompile(`new\s+KafkaConsumer\b`),
			regexp.MustCompile(`@KafkaListener\s*\(\s*topics?\s*=\s*[\{"]?([^,)}]+)`),
			regexp.MustCompile(`\.subscribe\s*\(\s*(?:Collections\.singletonList|Arrays\.asList|List\.of)\s*\(\s*"([^"]+)"`),
			regexp.MustCompile(`\.subscribe\s*\(\s*"([^"]+)"`),
		},
	}
	// Node/TS: kafkajs (producer/consumer) and node-rdkafka.
	jsPatterns = patternSet{
		produces: []*regexp.Regexp{
			regexp.MustCompile(`\.producer\s*\(\s*\)`),
			regexp.MustCompile(`\.send\s*\(\s*\{[^}]*topic\s*:\s*['"]([^'"]+)['"]`),
		},
		consumes: []*regexp.Regexp{
			regexp.MustCompile(`\.consumer\s*\(\s*\{[^}]*groupId`),
			regexp.MustCompile(`\.subscribe\s*\(\s*\{[^}]*topic\s*:\s*['"]([^'"]+)['"]`),
			regexp.MustCompile(`topics\s*:\s*\[\s*['"]([^'"]+)['"]`),
		},
	}
	// Python: confluent-kafka, kafka-python, aiokafka.
	pyPatterns = patternSet{
		produces: []*regexp.Regexp{
			regexp.MustCompile(`Producer\s*\(`),
			regexp.MustCompile(`KafkaProducer\s*\(`),
			regexp.MustCompile(`AIOKafkaProducer\s*\(`),
			regexp.MustCompile(`\.produce\s*\(\s*["']([^"']+)["']`),
			regexp.MustCompile(`\.send\s*\(\s*["']([^"']+)["']`),
		},
		consumes: []*regexp.Regexp{
			regexp.MustCompile(`Consumer\s*\(`),
			regexp.MustCompile(`KafkaConsumer\s*\(\s*["']([^"']+)["']`),
			regexp.MustCompile(`AIOKafkaConsumer\s*\(\s*["']([^"']+)["']`),
			regexp.MustCompile(`\.subscribe\s*\(\s*\[\s*["']([^"']+)["']`),
		},
	}
)

var languageDispatch = map[string]patternSet{
	".go":    goPatterns,
	".java":  jvmPatterns,
	".kt":    jvmPatterns,
	".kts":   jvmPatterns,
	".scala": jvmPatterns,
	".js":    jsPatterns,
	".jsx":   jsPatterns,
	".ts":    jsPatterns,
	".tsx":   jsPatterns,
	".mjs":   jsPatterns,
	".py":    pyPatterns,
}

func (e *Extractor) Extract(ctx context.Context, repoPath, repoName string) (*extract.Fragment, error) {
	frag := extract.NewFragment(e.Name())
	repoNodeID := "repo::" + repoName

	maxBytes := e.MaxFileBytes
	if maxBytes <= 0 {
		maxBytes = 2 * 1024 * 1024
	}

	// Aggregate topics per role to avoid duplicate edges.
	produced := map[string]occurrence{}
	consumed := map[string]occurrence{}

	walk := func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			if d != nil && d.IsDir() && shouldSkipDir(d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ext := strings.ToLower(filepath.Ext(path))
		ps, ok := languageDispatch[ext]
		if !ok {
			return nil
		}
		info, statErr := d.Info()
		if statErr != nil || info.Size() > maxBytes {
			return nil
		}
		rel, _ := filepath.Rel(repoPath, path)
		rel = filepath.ToSlash(rel)

		f, ferr := os.Open(path)
		if ferr != nil {
			frag.Warn(fmt.Sprintf("%s: %v", rel, ferr))
			return nil
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		lineNum := 0
		for scanner.Scan() {
			lineNum++
			line := scanner.Text()
			for _, re := range ps.produces {
				for _, m := range re.FindAllStringSubmatch(line, -1) {
					if len(m) >= 2 && m[1] != "" {
						register(produced, m[1], rel, lineNum)
					}
				}
			}
			for _, re := range ps.consumes {
				for _, m := range re.FindAllStringSubmatch(line, -1) {
					if len(m) >= 2 && m[1] != "" {
						register(consumed, m[1], rel, lineNum)
					}
				}
			}
		}
		_ = f.Close()
		return nil
	}

	if err := filepath.WalkDir(repoPath, walk); err != nil {
		return frag, fmt.Errorf("walk repo: %w", err)
	}

	emitTopics(frag, repoNodeID, repoName, "produces", produced)
	emitTopics(frag, repoNodeID, repoName, "consumes", consumed)
	return frag, nil
}

type occurrence struct {
	file string
	line int
}

func register(m map[string]occurrence, topic, file string, line int) {
	topic = strings.TrimSpace(strings.Trim(topic, `"' {}`))
	if topic == "" {
		return
	}
	if _, exists := m[topic]; !exists {
		m[topic] = occurrence{file: file, line: line}
	}
}

func emitTopics(frag *extract.Fragment, repoNodeID, repoName, relation string, topics map[string]occurrence) {
	for topic, occ := range topics {
		id := "topic::" + topic
		frag.AddNode(extract.FragmentNode{
			ID:    id,
			Label: topic,
			Type:  "kafka_topic",
			Metadata: map[string]any{
				"discovered_in_repo": repoName,
			},
		})
		frag.AddEdge(extract.FragmentEdge{
			Source:         repoNodeID,
			Target:         id,
			Relation:       relation,
			Confidence:     extract.ConfidenceInferred,
			SourceFile:     occ.file,
			SourceLocation: fmt.Sprintf("L%d", occ.line),
		})
	}
}

func shouldSkipDir(name string) bool {
	switch name {
	case ".git", "node_modules", "vendor", "target", "build", "dist",
		"__pycache__", ".venv", "venv", ".tox", ".gradle", ".idea",
		".vs", "bin", "obj", ".mvn":
		return true
	}
	return false
}
