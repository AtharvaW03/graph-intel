package graphify

type Graph struct {
	Nodes      []Node      `json:"nodes"`
	Links      []Link      `json:"links"`
	HyperEdges []HyperEdge `json:"hyperedges"`
}

type Node struct {
	ID             string         `json:"id"`
	Label          string         `json:"label"`
	NormLabel      string         `json:"norm_label"`
	Origin         string         `json:"_origin"`
	FileType       string         `json:"file_type"`
	Community      int            `json:"community"`
	CommunityName  string         `json:"community_name"`
	SourceFile     string         `json:"source_file"`
	SourceLocation string         `json:"source_location"`
	Type           string         `json:"type"`
	Ecosystem      string         `json:"ecosystem"`
	Metadata       map[string]any `json:"metadata"`
}

// MetaString returns the metadata value for key when it is a string, or ""
// when the key is absent or holds another type. Metadata is an open map
// (graphify's own nodes carry kind/language; platform extractors carry
// version/script/schedule/...), so callers pull the keys they know about.
func (n Node) MetaString(key string) string {
	s, _ := n.Metadata[key].(string)
	return s
}

type Link struct {
	Source          string  `json:"source"`
	Target          string  `json:"target"`
	Relation        string  `json:"relation"`
	Confidence      string  `json:"confidence"`
	ConfidenceScore float64 `json:"confidence_score"`
	Weight          float64 `json:"weight"`
	SourceFile      string  `json:"source_file"`
	SourceLocation  string  `json:"source_location"`
	Context         string  `json:"context"`
}

type HyperEdge map[string]any
