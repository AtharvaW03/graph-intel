package kafka

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

type topology struct {
	topics   map[string]bool
	produces map[string]bool
	consumes map[string]bool
	hasHub   bool
}

func runExtract(t *testing.T, files map[string]string) topology {
	t.Helper()
	dir := t.TempDir()
	for name, contents := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	frag, err := New().Extract(context.Background(), dir, "test-repo")
	if err != nil {
		t.Fatal(err)
	}
	out := topology{topics: map[string]bool{}, produces: map[string]bool{}, consumes: map[string]bool{}}
	for _, n := range frag.Nodes {
		if n.Type == "kafka_topic" {
			out.topics[n.Label] = true
		}
		if n.ID == "repo::test-repo" {
			out.hasHub = true
		}
	}
	for _, e := range frag.Edges {
		topic := e.Target[len("topic::"):]
		switch e.Relation {
		case "produces":
			out.produces[topic] = true
		case "consumes":
			out.consumes[topic] = true
		}
	}
	return out
}

func TestGoTopology(t *testing.T) {
	got := runExtract(t, map[string]string{
		"kafka.go": `package main

func setup() {
	w := &kafka.Writer{Addr: addr, Topic: "orders_events", Balancer: b}
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: bs, Topic: "trades_stream"})
	msg := &sarama.ProducerMessage{Topic: "audit_log", Value: v}
}
`,
	})
	if !got.produces["orders_events"] || !got.produces["audit_log"] {
		t.Errorf("produces = %v", got.produces)
	}
	if !got.consumes["trades_stream"] {
		t.Errorf("consumes = %v", got.consumes)
	}
	if !got.hasHub {
		t.Error("repo hub node missing — edges dangle when deps extractor is disabled")
	}
}

func TestJVMTopology(t *testing.T) {
	got := runExtract(t, map[string]string{
		"Listener.java": `public class Listener {
    @KafkaListener(topics = "user_events")
    public void onUser(String msg) {}

    void publish() {
        template.send("notification_requests", payload);
    }
}
`,
	})
	if !got.consumes["user_events"] {
		t.Errorf("consumes = %v", got.consumes)
	}
	if !got.produces["notification_requests"] {
		t.Errorf("produces = %v", got.produces)
	}
}

func TestPythonTopology(t *testing.T) {
	got := runExtract(t, map[string]string{
		"worker.py": `consumer = KafkaConsumer("raw_ticks", bootstrap_servers=servers)
producer.send("clean_ticks", value)
`,
	})
	if !got.consumes["raw_ticks"] || !got.produces["clean_ticks"] {
		t.Errorf("topology = %+v", got)
	}
}

func TestNoKafkaNoNodes(t *testing.T) {
	got := runExtract(t, map[string]string{
		"plain.go": "package main\n\nfunc main() {}\n",
	})
	if len(got.topics) != 0 || got.hasHub {
		t.Errorf("nodes emitted for kafka-free repo: %+v", got)
	}
}
