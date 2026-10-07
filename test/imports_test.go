package test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

const modulePath = "notifplatform"

// forbidden lists, per package directory prefix, the import prefixes it must not
// use (D2b). One module makes accidental coupling easy; this test is what stops it.
var forbidden = map[string][]string{
	"internal/consumer": {modulePath + "/internal/relay"},
	"cmd/consumer":      {modulePath + "/internal/relay"},
	"internal/relay":    {modulePath + "/internal/consumer"},
	"cmd/relay":         {modulePath + "/internal/consumer"},
}

func TestForbiddenImports(t *testing.T) {
	root := ".." // this test runs with the test/ directory as the working directory
	fset := token.NewFileSet()

	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)

		var banned []string
		for prefix, list := range forbidden {
			if strings.HasPrefix(rel, prefix+"/") {
				banned = append(banned, list...)
			}
		}
		if len(banned) == 0 {
			return nil
		}

		f, perr := parser.ParseFile(fset, p, nil, parser.ImportsOnly)
		if perr != nil {
			return perr
		}
		for _, imp := range f.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			for _, b := range banned {
				if path == b || strings.HasPrefix(path, b+"/") {
					t.Errorf("%s imports %s, which is forbidden by the package rules", rel, path)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
