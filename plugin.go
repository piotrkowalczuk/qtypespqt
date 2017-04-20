package qtypespqt

import (
	"bytes"
	"fmt"
	"go/types"
	"io"
	"strings"

	"github.com/piotrkowalczuk/pqt"
	"github.com/piotrkowalczuk/pqt/pqtgo"
	"github.com/piotrkowalczuk/qtypes"
)

type Plugin struct {
	Formatter  *pqtgo.Formatter
	Visibility pqtgo.Visibility
}

// PropertyType implements pqtgo Plugin interface.
func (*Plugin) PropertyType(c *pqt.Column, m int32) string {
	switch {
	case useString(c.Type, m):
		return "*qtypes.String"
	case useInt64(c.Type, m):
		return "*qtypes.Int64"
	case useFloat64(c.Type, m):
		return "*qtypes.Float64"
	case useTimestamp(c.Type, m):
		return "*qtypes.Timestamp"
	}
	return ""
}

// ScanClause implements pqtgo Plugin interface.
func (p *Plugin) ScanClause(c *pqt.Column) string {
	return ""
}

// SetClause implements pqtgo Plugin interface.
func (p *Plugin) SetClause(c *pqt.Column) string {
	return ""
}

// WhereClause implements pqtgo Plugin interface.
func (p *Plugin) WhereClause(c *pqt.Column) string {
	opts := "And"
	if c.IsDynamic {
		opts = `&CompositionOpts{Joint: " AND ", IsDynamic: true}`
	}
	switch {
	case useString(c.Type, pqtgo.ModeCriteria):
		return fmt.Sprintf(`
			%s({{ .selector }}, {{ .id }}, {{ .column }}, {{ .composer }}, %s)`, p.Formatter.Identifier("queryStringWhereClause"), opts)
	case useInt64(c.Type, pqtgo.ModeCriteria):
		return fmt.Sprintf(`
			%s({{ .selector }}, {{ .id }}, {{ .column }}, {{ .composer }}, %s)`, p.Formatter.Identifier("queryInt64WhereClause"), opts)
	case useFloat64(c.Type, pqtgo.ModeCriteria):
		return fmt.Sprintf(`
			%s({{ .selector }}, {{ .id }}, {{ .column }}, {{ .composer }}, %s)`, p.Formatter.Identifier("queryFloat64WhereClause"), opts)
	case useTimestamp(c.Type, pqtgo.ModeCriteria):
		return fmt.Sprintf(`
		%s({{ .selector }}, {{ .id }}, {{ .column }}, {{ .composer }}, %s)`, p.Formatter.Identifier("queryTimestampWhereClause"), opts)
	}
	return ""
}

// Static implements pqtgo Plugin interface.
func (p *Plugin) Static(s *pqt.Schema) string {
	buf := bytes.NewBuffer(nil)

	p.whereClause(buf, "Int64")
	p.whereClause(buf, "Float64")
	p.whereClause(buf, "String")
	p.whereClause(buf, "Timestamp")

	return buf.String()
}

func (p *Plugin) whereClause(w io.Writer, n string) error {
	var placeholder string
	switch n {
	case "Int64":
		placeholder = "%d"
	case "Float64":
		placeholder = "%g"
	case "Timestamp":
		placeholder = "%s"
	default:
		placeholder = "%s"
	}

	funcName := p.Formatter.Identifier("query", n, "where", "clause")
	fmt.Fprintf(w, `
	func %s(i *qtypes.%s, id int, sel string, com *Composer, opt *CompositionOpts) (err error) {
		if i == nil || !i.Valid {
			return nil
		}`, funcName, n,
	)
	fmt.Fprint(w, `
		if i.Type == qtypes.QueryType_IN {
			if len(i.Values) == 0 {
				return nil
			}
		}
		if i.Type != qtypes.QueryType_BETWEEN {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}

			if i.Negation {
				switch i.Type {
				case qtypes.QueryType_CONTAINS, qtypes.QueryType_IS_CONTAINED_BY, qtypes.QueryType_OVERLAP, qtypes.QueryType_HAS_ANY_ELEMENT, qtypes.QueryType_HAS_ALL_ELEMENTS, qtypes.QueryType_HAS_ELEMENT:
					if _, err = com.WriteString(" NOT "); err != nil {
						return
					}
				}
			}

			if len(opt.SelectorFuncs) == 0 {
				switch i.Type {
				case qtypes.QueryType_OVERLAP:
					if opt.IsJSON {
						if _, err = com.WriteString("ARRAY(SELECT jsonb_array_elements_text("); err != nil {
							return err
						}
					}
				case qtypes.QueryType_HAS_ANY_ELEMENT, qtypes.QueryType_HAS_ALL_ELEMENTS, qtypes.QueryType_HAS_ELEMENT:
					if !opt.IsJSON {
						if _, err = com.WriteString("ARRAY(SELECT jsonb_array_elements_text("); err != nil {
							return err
						}
					}
				}
			} else {
				for _, sf := range opt.SelectorFuncs {
					if _, err = com.WriteString(sf); err != nil {
						return err
					}
					if _, err = com.WriteString("("); err != nil {
						return err
					}
				}
			}
			if !opt.IsDynamic {
				if err = com.WriteAlias(id); err != nil {
					return err
				}
			}
			if opt.SelectorCast != "" {
				if _, err = com.WriteString("("); err != nil {
					return
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			if opt.SelectorCast != "" {
				if _, err = com.WriteString(")::"); err != nil {
					return
				}
				if _, err = com.WriteString(opt.SelectorCast); err != nil {
					return
				}
			}
			if len(opt.SelectorFuncs) == 0 {
				switch i.Type {
				case qtypes.QueryType_OVERLAP:
					if opt.IsJSON {
						if _, err = com.WriteString("))"); err != nil {
							return err
						}
					}
				}
			} else {
				for range opt.SelectorFuncs {
					if _, err = com.WriteString(")"); err != nil {
						return err
					}
				}
			}
		}
		switch i.Type {`)

	fmt.Fprint(w, `
		case qtypes.QueryType_NULL:
			if i.Negation {
				if _, err = com.WriteString(" IS NOT NULL"); err != nil {
					return
				}
			} else {
				if _, err = com.WriteString(" IS NULL"); err != nil {
					return
				}
			}
			com.Dirty = true
			return nil
			`)
	fmt.Fprint(w, `
		case qtypes.QueryType_EQUAL:
			if i.Negation {
				if _, err = com.WriteString(" <> "); err != nil {
					return
				}
			} else {
				if _, err = com.WriteString(" = "); err != nil {
					return
				}
			}`)
	fmt.Fprint(w, `
		case qtypes.QueryType_GREATER:
			if i.Negation {
				if _, err = com.WriteString(" <= "); err != nil {
					return
				}
			} else {
				if _, err = com.WriteString(" > "); err != nil {
					return
				}
			}`)
	fmt.Fprint(w, `
		case qtypes.QueryType_GREATER_EQUAL:
			if i.Negation {
				if _, err = com.WriteString(" < "); err != nil {
					return
				}
			} else {
				if _, err = com.WriteString(" >= "); err != nil {
					return
				}
			}`)
	fmt.Fprint(w, `
		case qtypes.QueryType_LESS:
			if i.Negation {
				if _, err = com.WriteString(" >= "); err != nil {
					return
				}
			} else {
				if _, err = com.WriteString(" < "); err != nil {
					return
				}
			}`)
	fmt.Fprint(w, `
		case qtypes.QueryType_LESS_EQUAL:
			if i.Negation {
				if _, err = com.WriteString(" > "); err != nil {
					return
				}
			} else {
				if _, err = com.WriteString(" <= "); err != nil {
					return
				}
			}`)
	fmt.Fprint(w, `
		case qtypes.QueryType_CONTAINS:
			if _, err = com.WriteString(" @> "); err != nil {
				return
			}`)
	fmt.Fprint(w, `
		case qtypes.QueryType_IS_CONTAINED_BY:
			if _, err = com.WriteString(" <@ "); err != nil {
				return
			}`)
	fmt.Fprint(w, `
		case qtypes.QueryType_OVERLAP:
			if _, err = com.WriteString(" && "); err != nil {
				return
			}`)
	fmt.Fprint(w, `
		case qtypes.QueryType_HAS_ANY_ELEMENT:
			if _, err = com.WriteString(" ?| "); err != nil {
				return
			}`)
	fmt.Fprint(w, `
		case qtypes.QueryType_HAS_ALL_ELEMENTS:
			if _, err = com.WriteString(" ?& "); err != nil {
				return
			}`)
	fmt.Fprint(w, `
		case qtypes.QueryType_HAS_ELEMENT:
			if _, err = com.WriteString(" ? "); err != nil {
				return
			}`)
	if strings.ToLower(n) == "string" {
		fmt.Fprint(w, `
			case qtypes.QueryType_SUBSTRING, qtypes.QueryType_HAS_PREFIX, qtypes.QueryType_HAS_SUFFIX:
			if i.Negation {
				if _, err = com.WriteString(" NOT"); err != nil {
					return
				}
			}
			if i.Insensitive {
				if _, err = com.WriteString(" ILIKE "); err != nil {
					return
				}
			} else {
				if _, err = com.WriteString(" LIKE "); err != nil {
					return
				}
			}`)
	}
	fmt.Fprintf(w, `
		case qtypes.QueryType_IN:
			if i.Negation {
				if _, err = com.WriteString(" NOT IN ("); err != nil {
					return
				}
			} else {
				if _, err = com.WriteString(" IN ("); err != nil {
					return
				}
			}
			for i, v := range i.Values {
				if i != 0 {
					if _, err = com.WriteString(","); err != nil {
						return
					}
				}
				if err = com.WritePlaceholder(); err != nil {
					return
				}
				%s
				com.Dirty = true
			}
			if _, err = com.WriteString(")"); err != nil {
				return
			}`, add(n, "v"))
	fmt.Fprintf(w, `
		case qtypes.QueryType_BETWEEN:
			cpy := *i
			cpy.Values = i.Values[:1]
			cpy.Type = qtypes.QueryType_GREATER
			if err := %s(&cpy, id, sel, com, opt); err != nil {
				return err
			}
			cpy.Values = i.Values[1:]
			cpy.Type = qtypes.QueryType_LESS
			if err := %s(&cpy, id, sel, com, opt); err != nil {
				return err
			}`, funcName, funcName)

	fmt.Fprint(w, `
			default:
				return
		}
		if i.Type != qtypes.QueryType_BETWEEN && i.Type != qtypes.QueryType_IN {
			for _, pf := range opt.PlaceholderFuncs {
				if _, err := com.WriteString(pf); err != nil {
					return err
				}
				if _, err := com.WriteString("("); err != nil {
					return err
				}
			}
			if err = com.WritePlaceholder(); err != nil {
				return
			}
			for range opt.PlaceholderFuncs {
				if _, err := com.WriteString(")"); err != nil {
					return err
				}
			}
		}
		switch i.Type {
		case qtypes.QueryType_CONTAINS, qtypes.QueryType_IS_CONTAINED_BY, qtypes.QueryType_HAS_ANY_ELEMENT, qtypes.QueryType_HAS_ALL_ELEMENTS:`)
	if strings.ToLower(n) == "timestamp" {
		fmt.Fprint(w, `
			return errors.New("query type not supported for timestamp")`)

	} else {
		fmt.Fprintf(w, `
				switch opt.IsJSON {
				case true:
					com.Add(JSONArray%s(i.Values))
				case false:
					com.Add(pq.%sArray(i.Values))
				}
			case qtypes.QueryType_OVERLAP:
				com.Add(pq.%sArray(i.Values))`, n, n, n)
	}
	fmt.Fprintf(w, `
		case qtypes.QueryType_SUBSTRING:
			com.Add(fmt.Sprintf("%s", i.Value()))
		case qtypes.QueryType_HAS_PREFIX:
			com.Add(fmt.Sprintf("%s", i.Value()))
		case qtypes.QueryType_HAS_SUFFIX:
			com.Add(fmt.Sprintf("%s", i.Value()))
		case qtypes.QueryType_IN:
			// already handled
		case qtypes.QueryType_BETWEEN:
			// already handled by recursive call
		default:
			%s
		}

		com.Dirty = true
		return nil
	}`, "%%"+placeholder+"%%", placeholder+"%%", "%%"+placeholder, add(n, "i.Value()"))
	return nil
}

func useInt64(t pqt.Type, m int32) (use bool) {
	if m != pqtgo.ModeCriteria {
		return false
	}
	switch tt := t.(type) {
	case pqtgo.BuiltinType:
		switch types.BasicKind(tt) {
		case types.Int:
			use = true
		case types.Int8:
			use = true
		case types.Int16:
			use = true
		case types.Int32:
			use = true
		case types.Int64:
			use = true
		}
	case pqt.BaseType:
		switch tt {
		case pqt.TypeIntegerSmall(), pqt.TypeInteger(), pqt.TypeIntegerBig():
			use = true
		case pqt.TypeSerialSmall(), pqt.TypeSerial(), pqt.TypeSerialBig():
			use = true
		default:
			switch {
			case strings.HasPrefix(tt.String(), "INTEGER["):
				use = true
			case strings.HasPrefix(tt.String(), "BIGINT["):
				use = true
			case strings.HasPrefix(tt.String(), "SMALLINT["):
				use = true
			}
		}
	case pqtgo.CustomType:
		if _, ok := tt.ValueOf(m).(*qtypes.Int64); ok {
			return true
		}
	case pqt.MappableType:
		for _, mt := range tt.Mapping {
			if useInt64(mt, m) {
				return true
			}
		}
	}
	return
}

func useFloat64(t pqt.Type, m int32) (use bool) {
	if m != pqtgo.ModeCriteria {
		return false
	}
	switch tt := t.(type) {
	case pqtgo.BuiltinType:
		switch types.BasicKind(tt) {
		case types.Float32:
			use = true
		case types.Float64:
			use = true
		}
	case pqt.BaseType:
		switch tt {
		case pqt.TypeDoublePrecision():
			use = true
		default:
			switch {
			case strings.HasPrefix(tt.String(), "DECIMAL"):
				use = true
			case strings.HasPrefix(tt.String(), "NUMERIC"):
				use = true
			case strings.HasPrefix(tt.String(), "DOUBLE PRECISION["):
				use = true
			}
		}
	case pqtgo.CustomType:
		if _, ok := tt.ValueOf(m).(*qtypes.Float64); ok {
			return true
		}
	case pqt.MappableType:
		for _, mt := range tt.Mapping {
			if useFloat64(mt, m) {
				return true
			}
		}
	}
	return
}

func useString(t pqt.Type, m int32) (use bool) {
	if m != pqtgo.ModeCriteria {
		return false
	}

	switch tt := t.(type) {
	case pqtgo.BuiltinType:
		switch types.BasicKind(tt) {
		case types.String:
			use = true
		}
	case pqt.BaseType:
		switch tt {
		case pqt.TypeText():
			use = true
		case pqt.TypeUUID():
			use = true
		default:
			switch {
			case strings.HasPrefix(tt.String(), "TEXT["):
				use = true
			case strings.HasPrefix(tt.String(), "VARCHAR"), strings.HasPrefix(tt.String(), "CHARACTER["):
				use = true
			}
		}
	case pqtgo.CustomType:
		if _, ok := tt.ValueOf(m).(*qtypes.String); ok {
			return true
		}
	case pqt.MappableType:
		for _, mt := range tt.Mapping {
			if useString(mt, m) {
				return true
			}
		}
	}
	return
}

func useTimestamp(t pqt.Type, m int32) (use bool) {
	if m != pqtgo.ModeCriteria {
		return false
	}
	switch tt := t.(type) {
	case pqt.BaseType:
		switch tt {
		case pqt.TypeTimestamp(), pqt.TypeTimestampTZ():
			use = true
		}
	case pqtgo.CustomType:
		if _, ok := tt.ValueOf(m).(*qtypes.Timestamp); ok {
			return true
		}
	case pqt.MappableType:
		for _, mt := range tt.Mapping {
			if useTimestamp(mt, m) {
				return true
			}
		}
	}
	return
}

func add(n, s string) string {
	if strings.ToLower(n) == "timestamp" {
		return fmt.Sprintf(`
			ts, err := ptypes.Timestamp(%s)
			if err != nil {
				return err
			}
			com.Add(ts)`, s)
	}
	return fmt.Sprintf(`
		com.Add(%s)`, s)
}
