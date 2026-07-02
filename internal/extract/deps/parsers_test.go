package deps

import "testing"

func find(deps []Dep, name string) *Dep {
	for i := range deps {
		if deps[i].Name == name {
			return &deps[i]
		}
	}
	return nil
}

func TestParseGoMod(t *testing.T) {
	deps, err := parseGoMod("go.mod", `module example.com/svc

go 1.22

require (
	github.com/gin-gonic/gin v1.9.1
	golang.org/x/sys v0.15.0 // indirect
)

require github.com/inline/dep v2.0.0
`)
	if err != nil {
		t.Fatal(err)
	}
	if len(deps) != 3 {
		t.Fatalf("deps = %d, want 3: %+v", len(deps), deps)
	}
	gin := find(deps, "github.com/gin-gonic/gin")
	if gin == nil || gin.Version != "v1.9.1" || gin.Scope != "runtime" || gin.Ecosystem != "go" {
		t.Errorf("gin = %+v", gin)
	}
	if sys := find(deps, "golang.org/x/sys"); sys == nil || sys.Scope != "indirect" {
		t.Errorf("indirect scope not detected: %+v", sys)
	}
	if inline := find(deps, "github.com/inline/dep"); inline == nil {
		t.Error("inline require missed")
	}
}

func TestParsePackageJSON(t *testing.T) {
	deps, err := parsePackageJSON("package.json", `{
  "dependencies": {"lodash": "^4.17.21"},
  "devDependencies": {"vitest": "^1.0.0"},
  "peerDependencies": {"react": ">=18"}
}`)
	if err != nil {
		t.Fatal(err)
	}
	if d := find(deps, "lodash"); d == nil || d.Scope != "runtime" || d.Ecosystem != "npm" {
		t.Errorf("lodash = %+v", d)
	}
	if d := find(deps, "vitest"); d == nil || d.Scope != "dev" {
		t.Errorf("vitest = %+v", d)
	}
	if d := find(deps, "react"); d == nil || d.Scope != "peer" {
		t.Errorf("react = %+v", d)
	}
}

func TestParseRequirementsTxt(t *testing.T) {
	deps, err := parseRequirementsTxt("requirements.txt", `# comment
fastapi>=0.110
pydantic==2.6.1 ; python_version > "3.10"
-r other.txt
uvicorn[standard]~=0.27
`)
	if err != nil {
		t.Fatal(err)
	}
	if d := find(deps, "fastapi"); d == nil || d.Version != ">=0.110" {
		t.Errorf("fastapi = %+v", d)
	}
	if d := find(deps, "uvicorn"); d == nil {
		t.Errorf("extras not stripped: %+v", deps)
	}
	if find(deps, "-r") != nil || find(deps, "other.txt") != nil {
		t.Error("-r reference parsed as a dependency")
	}
}

func TestParseBuildGradle(t *testing.T) {
	deps, err := parseBuildGradle("build.gradle", `dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-web:3.2.0'
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
    api group: 'com.fasterxml.jackson.core', name: 'jackson-databind', version: '2.16.0'
}
`)
	if err != nil {
		t.Fatal(err)
	}
	if d := find(deps, "org.springframework.boot:spring-boot-starter-web"); d == nil || d.Version != "3.2.0" || d.Scope != "implementation" {
		t.Errorf("spring = %+v", d)
	}
	if d := find(deps, "org.junit.jupiter:junit-jupiter"); d == nil || d.Scope != "testImplementation" {
		t.Errorf("junit = %+v", d)
	}
	if d := find(deps, "com.fasterxml.jackson.core:jackson-databind"); d == nil || d.Version != "2.16.0" {
		t.Errorf("jackson long form = %+v", d)
	}
}

func TestParsePomXML(t *testing.T) {
	deps, err := parsePomXML("pom.xml", `<?xml version="1.0"?>
<project>
  <dependencies>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-clients</artifactId>
      <version>3.6.0</version>
      <scope>compile</scope>
    </dependency>
  </dependencies>
</project>`)
	if err != nil {
		t.Fatal(err)
	}
	if d := find(deps, "org.apache.kafka:kafka-clients"); d == nil || d.Version != "3.6.0" || d.Scope != "compile" {
		t.Errorf("kafka-clients = %+v", d)
	}
}

func TestInternalRepoNameFromDep(t *testing.T) {
	prefixes := []string{"github.com/angel-one/", "@angel-one/"}
	cases := []struct {
		dep, want string
	}{
		{"github.com/angel-one/auth-service", "auth-service"},
		{"github.com/angel-one/auth-service/v2", "auth-service"},
		{"@angel-one/ui-kit", "ui-kit"},
		{"github.com/gin-gonic/gin", ""},
	}
	for _, c := range cases {
		if got := InternalRepoNameFromDep(c.dep, prefixes); got != c.want {
			t.Errorf("InternalRepoNameFromDep(%q) = %q, want %q", c.dep, got, c.want)
		}
	}
}

func TestPackageNodeIDIsEcosystemScoped(t *testing.T) {
	goPkg := PackageNodeID(Dep{Name: "Redis", Ecosystem: "go"})
	npmPkg := PackageNodeID(Dep{Name: "redis", Ecosystem: "npm"})
	if goPkg == npmPkg {
		t.Error("same short name in different ecosystems must not collide")
	}
	// Casing variants collapse.
	if PackageNodeID(Dep{Name: "Redis", Ecosystem: "go"}) != PackageNodeID(Dep{Name: "redis", Ecosystem: "go"}) {
		t.Error("case variants should collapse to one node")
	}
}
