package template

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEngine_Render(t *testing.T) {
	e := NewEngine()
	out, err := e.Render("Hello {{.Name}}, order #{{.OrderID}} ready", map[string]any{
		"Name":    "Valentin",
		"OrderID": 42,
	})
	require.NoError(t, err)
	require.Equal(t, "Hello Valentin, order #42 ready", out)
}

func TestEngine_Render_missingVar(t *testing.T) {
	e := NewEngine()
	_, err := e.Render("Hi {{.Missing}}", map[string]any{})
	require.Error(t, err)
}
