package connectors

import "testing"

func TestIdentifierRenderersEscapeRawParts(t *testing.T) {
	identifiers := []string{
		"normal", "first name", "customer-id", "O'Reilly", "some`column",
		`some"column`, "O’Rayelly", "O‘Rayelly", "OʼRayelly", "O«Rayelly",
		"O「Rayelly", "日本語「列」", "a.b", "123column", "select",
		`x"; DROP TABLE users; --`,
	}
	renderers := []identifierRenderer{
		postgresIdentifierRenderer, mysqlIdentifierRenderer, mssqlIdentifierRenderer,
		oracleIdentifierRenderer, clickHouseIdentifierRenderer, trinoIdentifierRenderer,
		cassandraIdentifierRenderer,
	}
	for _, renderer := range renderers {
		for _, identifier := range identifiers {
			got, err := renderer.part(identifier)
			if err != nil {
				t.Fatalf("renderer %+v rejected %q: %v", renderer, identifier, err)
			}
			if got == identifier {
				t.Fatalf("renderer %+v did not quote %q", renderer, identifier)
			}
		}
	}
}

func TestIdentifierRendererDistinguishesLiteralDotsFromQualification(t *testing.T) {
	part, err := postgresIdentifierRenderer.part("a.b")
	if err != nil || part != `"a.b"` {
		t.Fatalf("literal dot = %q, %v", part, err)
	}
	qualified, err := postgresIdentifierRenderer.qualified("schema", "table")
	if err != nil || qualified != `"schema"."table"` {
		t.Fatalf("qualified = %q, %v", qualified, err)
	}
}
