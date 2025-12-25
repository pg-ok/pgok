package util

import (
    "fmt"
    "strings"
    "unicode"
)

type OutputFormat string

const (
    OutputFormatTable OutputFormat = "table"
    OutputFormatJson  OutputFormat = "json"
)

func (f *OutputFormat) String() string {
    return string(*f)
}

func (f *OutputFormat) Set(v string) error {
    switch v {
    case "table", "json":
        *f = OutputFormat(v)
        return nil
    default:
        return fmt.Errorf("must be one of 'table' or 'json'")
    }
}

func (f *OutputFormat) Type() string {
    return "OutputFormat"
}

// TrimLeftSpaces removes common indentation from lines, preserving relative formatting.
func TrimLeftSpaces(s string) string {
    lines := strings.Split(s, "\n")

    // 1. Skip initial empty lines
    startIdx := 0
    for startIdx < len(lines) && strings.TrimSpace(lines[startIdx]) == "" {
        startIdx++
    }
    lines = lines[startIdx:]

    if len(lines) == 0 {
        return ""
    }

    // 2. Find minimum indentation among non-empty lines
    minIndent := -1
    for _, line := range lines {
        // Ignore empty lines when calculating indentation
        if strings.TrimSpace(line) == "" {
            continue
        }

        // Count whitespace characters at the beginning (runes)
        indent := 0
        for _, r := range line {
            if unicode.IsSpace(r) {
                indent++
            } else {
                break
            }
        }

        if minIndent == -1 || indent < minIndent {
            minIndent = indent
        }
    }

    if minIndent == -1 {
        minIndent = 0
    }

    // 3. Trim the indentation
    var trimmed []string
    for _, line := range lines {
        // Convert to runes for correct trimming (to match chars().skip() in Rust).
        // This is slightly slower than byte slicing, but safer for UTF-8.
        runes := []rune(line)

        if len(runes) >= minIndent {
            trimmed = append(trimmed, string(runes[minIndent:]))
        } else {
            trimmed = append(trimmed, "")
        }
    }

    return strings.Join(trimmed, "\n")
}

func PrintRunnableSQL(sql string, args []interface{}) {
    fmt.Println("-- Dry Run SQL:")
    fmt.Println(sql)
    fmt.Println("\n-- Parameters:")
    for i, arg := range args {
        fmt.Printf("-- $%d: %v\n", i+1, arg)
    }
}
