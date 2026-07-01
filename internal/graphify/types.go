package graphify

type Graph struct {
	Nodes      []Node      `json:"nodes"`
	Links      []Link      `json:"links"`
	HyperEdges []HyperEdge `json:"hyperedges"`
}

type NodeMetadata struct {
	Kind     string `json:"kind"`
	Language string `json:"language"`
}

type Node struct {
	ID             string       `json:"id"`
	Label          string       `json:"label"`
	NormLabel      string       `json:"norm_label"`
	Origin         string       `json:"_origin"`
	FileType       string       `json:"file_type"`
	Community      int          `json:"community"`
	CommunityName  string       `json:"community_name"`
	SourceFile     string       `json:"source_file"`
	SourceLocation string       `json:"source_location"`
	Type           string       `json:"type"`
	Ecosystem      string       `json:"ecosystem"`
	Metadata       NodeMetadata `json:"metadata"`
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
