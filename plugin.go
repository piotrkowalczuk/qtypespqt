package qtypespqt

import (
	"fmt"
	"go/types"
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
		opts = `&CompositionOpts{Joint: "And", IsDynamic: true}`
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
	return `
	` + p.numericWhereClause("Int64") + `
	` + p.numericWhereClause("Float64") + `
	func ` + p.Formatter.Identifier("queryTimestampWhereClause") + `(t *qtypes.Timestamp, id int, sel string, com *Composer, opt *CompositionOpts) error {
	if t == nil || !t.Valid {
		return nil
	}
	v := t.Value()
	if v != nil {
		vv1, err := ptypes.Timestamp(v)
		if err != nil {
			return err
		}
		switch t.Type {
		case qtypes.QueryType_NULL:
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return err
				}
			}
			if !opt.IsDynamic {
				if err := com.WriteAlias(id); err != nil {
					return err
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			if t.Negation {
				com.WriteString(" IS NOT NULL ")
			} else {
				com.WriteString(" IS NULL ")
			}
		case qtypes.QueryType_EQUAL:
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return err
				}
			}
			if !opt.IsDynamic {
				if err := com.WriteAlias(id); err != nil {
					return err
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			if t.Negation {
				com.WriteString(" <> ")
			} else {
				com.WriteString(" = ")
			}
			com.WritePlaceholder()
			com.Add(t.Value())
		case qtypes.QueryType_GREATER:
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return err
				}
			}
			if !opt.IsDynamic {
				if err := com.WriteAlias(id); err != nil {
					return err
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			com.WriteString(">")
			com.WritePlaceholder()
			com.Add(t.Value())
		case qtypes.QueryType_GREATER_EQUAL:
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return err
				}
			}
			if err := com.WriteAlias(id); err != nil {
				return err
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			com.WriteString(">=")
			com.WritePlaceholder()
			com.Add(t.Value())
		case qtypes.QueryType_LESS:
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return err
				}
			}
			if !opt.IsDynamic {
				if err := com.WriteAlias(id); err != nil {
					return err
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			com.WriteString(" < ")
			com.WritePlaceholder()
			com.Add(t.Value())
		case qtypes.QueryType_LESS_EQUAL:
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return err
				}
			}
			if !opt.IsDynamic {
				if err := com.WriteAlias(id); err != nil {
					return err
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			com.WriteString(" <= ")
			com.WritePlaceholder()
			com.Add(t.Value())
		case qtypes.QueryType_IN:
			if len(t.Values) >0 {
				if com.Dirty {
					if _, err = com.WriteString(opt.Joint); err != nil {
						return err
					}
				}
				if !opt.IsDynamic {
					if err := com.WriteAlias(id); err != nil {
						return err
					}
				}
				if _, err := com.WriteString(sel); err != nil {
					return err
				}
				com.WriteString(" IN (")
				for i, v := range t.Values {
					if i != 0 {
						com.WriteString(", ")
					}
					com.WritePlaceholder()
					com.Add(v)
				}
				com.WriteString(") ")
			}
		case qtypes.QueryType_BETWEEN:
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return err
				}
			}
			v2 := t.Values[1]
			if v2 != nil {
				vv2, err := ptypes.Timestamp(v2)
				if err != nil {
					return err
				}
				if !opt.IsDynamic {
					if err := com.WriteAlias(id); err != nil {
						return err
					}
				}
				if _, err := com.WriteString(sel); err != nil {
					return err
				}
				com.WriteString(" > ")
				com.WritePlaceholder()
				com.Add(vv1)
				com.WriteString(" AND ")
				if !opt.IsDynamic {
					if err := com.WriteAlias(id); err != nil {
						return err
					}
				}
				if _, err := com.WriteString(sel); err != nil {
					return err
				}
				com.WriteString(" < ")
				com.WritePlaceholder()
				com.Add(vv2)
			}
		}
	}
	return nil
}
func ` + p.Formatter.Identifier("queryStringWhereClause") + `(s *qtypes.String, id int, sel string, com *Composer, opt *CompositionOpts) (err error) {
	if s == nil || !s.Valid {
		return
	}
	switch s.Type {
	case qtypes.QueryType_NULL:
		if com.Dirty {
			if _, err = com.WriteString(opt.Joint); err != nil {
				return
			}
		}
		if !opt.IsDynamic {
			if err := com.WriteAlias(id); err != nil {
				return err
			}
		}
		if _, err := com.WriteString(sel); err != nil {
			return err
		}
		if s.Negation {
			if _, err = com.WriteString(" IS NOT NULL"); err != nil {
				return
			}
		} else {
			if _, err = com.WriteString(" IS NULL"); err != nil {
				return
			}
		}
		com.Dirty = true
		return // cannot be casted so simply return
	case qtypes.QueryType_EQUAL:
		if com.Dirty {
			if _, err = com.WriteString(opt.Joint); err != nil {
				return
			}
		}
		if !opt.IsDynamic {
			if err := com.WriteAlias(id); err != nil {
				return err
			}
		}
		if _, err := com.WriteString(sel); err != nil {
			return err
		}
		if s.Negation {
			if _, err = com.WriteString(" <> "); err != nil {
				return
			}
		} else {
			if _, err = com.WriteString(" = "); err != nil {
				return
			}
		}
		if opt.PlaceholderFunc != "" {
			if _, err = com.WriteString(opt.PlaceholderFunc); err != nil {
				return
			}
			if _, err = com.WriteString("("); err != nil {
				return
			}
		}
		if err = com.WritePlaceholder(); err != nil {
			return
		}
		com.Add(s.Value())
		com.Dirty = true
	case qtypes.QueryType_SUBSTRING:
		if com.Dirty {
			if _, err = com.WriteString(opt.Joint); err != nil {
				return
			}
		}
		if !opt.IsDynamic {
			if err := com.WriteAlias(id); err != nil {
				return err
			}
		}
		if _, err := com.WriteString(sel); err != nil {
			return err
		}
		if s.Negation {
			if _, err = com.WriteString(" NOT "); err != nil {
				return
			}
		}
		if s.Insensitive {
			if _, err = com.WriteString(" ILIKE "); err != nil {
				return
			}
		} else {
			if _, err = com.WriteString(" LIKE "); err != nil {
				return
			}
		}

		if opt.PlaceholderFunc != "" {
			if _, err = com.WriteString(opt.PlaceholderFunc); err != nil {
				return
			}
			if _, err = com.WriteString("("); err != nil {
				return
			}
		}
		if err = com.WritePlaceholder(); err != nil {
			return
		}

		com.Add(fmt.Sprintf("%%%s%%", s.Value()))
		com.Dirty = true
	case qtypes.QueryType_HAS_PREFIX:
		if com.Dirty {
			if _, err = com.WriteString(opt.Joint); err != nil {
				return
			}
		}
		if !opt.IsDynamic {
			if err := com.WriteAlias(id); err != nil {
				return err
			}
		}
		if _, err := com.WriteString(sel); err != nil {
			return err
		}
		if s.Negation {
			if _, err = com.WriteString(" NOT "); err != nil {
				return
			}
		}
		if s.Insensitive {
			if _, err = com.WriteString(" ILIKE "); err != nil {
				return
			}
		} else {
			if _, err = com.WriteString(" LIKE "); err != nil {
				return
			}
		}
		if opt.PlaceholderFunc != "" {
			if _, err = com.WriteString(opt.PlaceholderFunc); err != nil {
				return
			}
			if _, err = com.WriteString("("); err != nil {
				return
			}
		}
		if err = com.WritePlaceholder(); err != nil {
			return
		}

		com.Add(fmt.Sprintf("%s%%", s.Value()))
		com.Dirty = true
	case qtypes.QueryType_HAS_SUFFIX:
		if com.Dirty {
			if _, err = com.WriteString(opt.Joint); err != nil {
				return
			}
		}
		if !opt.IsDynamic {
			if err := com.WriteAlias(id); err != nil {
				return err
			}
		}
		if _, err := com.WriteString(sel); err != nil {
			return err
		}
		if s.Negation {
			if _, err = com.WriteString(" NOT "); err != nil {
				return
			}

		}
		if s.Insensitive {
			if _, err = com.WriteString(" ILIKE "); err != nil {
				return
			}
		} else {
			if _, err = com.WriteString(" LIKE "); err != nil {
				return
			}
		}
		if opt.PlaceholderFunc != "" {
			if _, err = com.WriteString(opt.PlaceholderFunc); err != nil {
				return
			}
			if _, err = com.WriteString("("); err != nil {
				return
			}
		}
		if err = com.WritePlaceholder(); err != nil {
			return
		}

		com.Add(fmt.Sprintf("%%%s", s.Value()))
		com.Dirty = true
	case qtypes.QueryType_CONTAINS:
		if !s.Negation {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}
			if !opt.IsDynamic {
				if err := com.WriteAlias(id); err != nil {
					return err
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			if _, err = com.WriteString(" @> "); err != nil {
				return
			}
			if opt.PlaceholderFunc != "" {
				if _, err = com.WriteString(opt.PlaceholderFunc); err != nil {
					return
				}
				if _, err = com.WriteString("("); err != nil {
					return
				}
			}
			if err = com.WritePlaceholder(); err != nil {
				return
			}

			switch opt.IsJSON {
			case true:
				com.Add(JSONArrayString(s.Values))
			case false:
				com.Add(pq.StringArray(s.Values))
			}
			com.Dirty = true
		}
	case qtypes.QueryType_IS_CONTAINED_BY:
		if !s.Negation {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}
			if !opt.IsDynamic {
				if err := com.WriteAlias(id); err != nil {
					return err
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			if _, err = com.WriteString(" <@ "); err != nil {
				return
			}
			if opt.PlaceholderFunc != "" {
				if _, err = com.WriteString(opt.PlaceholderFunc); err != nil {
					return
				}
				if _, err = com.WriteString("("); err != nil {
					return
				}
			}
			if err = com.WritePlaceholder(); err != nil {
				return
			}

			switch opt.IsJSON {
			case true:
				com.Add(JSONArrayString(s.Values))
			case false:
				com.Add(pq.StringArray(s.Values))
			}
			com.Dirty = true
		}
	case qtypes.QueryType_OVERLAP:
		if !s.Negation {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}
			if !opt.IsDynamic {
				if err := com.WriteAlias(id); err != nil {
					return err
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			if _, err = com.WriteString(" && "); err != nil {
				return
			}
			if opt.PlaceholderFunc != "" {
				if _, err = com.WriteString(opt.PlaceholderFunc); err != nil {
					return
				}
				if _, err = com.WriteString("("); err != nil {
					return
				}
			}
			if err = com.WritePlaceholder(); err != nil {
				return
			}

			switch opt.IsJSON {
			case true:
				com.Add(JSONArrayString(s.Values))
			case false:
				com.Add(pq.StringArray(s.Values))
			}
			com.Dirty = true
		}
	case qtypes.QueryType_HAS_ANY_ELEMENT:
		if !s.Negation {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}
			if !opt.IsDynamic {
				if err := com.WriteAlias(id); err != nil {
					return err
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			if _, err = com.WriteString(" ?| "); err != nil {
				return
			}
			if err = com.WritePlaceholder(); err != nil {
				return
			}
			switch opt.IsJSON {
			case true:
				com.Add(JSONArrayString(s.Values))
			case false:
				com.Add(pq.StringArray(s.Values))
			}
			com.Dirty = true
		}
	case qtypes.QueryType_HAS_ALL_ELEMENTS:
		if !s.Negation {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}
			if !opt.IsDynamic {
				if err := com.WriteAlias(id); err != nil {
					return err
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			if _, err = com.WriteString(" ?& "); err != nil {
				return
			}
			if err = com.WritePlaceholder(); err != nil {
				return
			}
			switch opt.IsJSON {
			case true:
				com.Add(JSONArrayString(s.Values))
			case false:
				com.Add(pq.StringArray(s.Values))
			}
			com.Dirty = true
		}
	case qtypes.QueryType_IN:
		if len(s.Values) > 0 {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}
			if !opt.IsDynamic {
				if err := com.WriteAlias(id); err != nil {
					return err
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			if s.Negation {
				if _, err = com.WriteString(" NOT IN ("); err != nil {
					return
				}
			} else {
				if _, err = com.WriteString(" IN ("); err != nil {
					return
				}
			}
			for i, v := range s.Values {
				if i != 0 {
					if _, err = com.WriteString(","); err != nil {
						return
					}
				}
				if err = com.WritePlaceholder(); err != nil {
					return
				}
				com.Add(v)
				com.Dirty = true
			}
			if _, err = com.WriteString(")"); err != nil {
				return
			}
		}
	default:
		return fmt.Errorf("pqtgo: unknown string query type %s", s.Type.String())
	}

	switch {
	case com.Dirty && opt.Cast != "":
		if _, err = com.WriteString(opt.Cast); err != nil {
			return
		}
	case com.Dirty && opt.PlaceholderFunc != "":
		if _, err = com.WriteString(")"); err != nil {
			return
		}
	case com.Dirty:
	}
	return
}`
}

func (p *Plugin) numericWhereClause(n string) string {
	return `func ` + p.Formatter.Identifier("query", n, "where", "clause") + `(i *qtypes.` + n + `, id int, sel string, com *Composer, opt *CompositionOpts) (err error) {
	if i == nil || !i.Valid {
		return nil
	}
	switch i.Type {
	case qtypes.QueryType_NULL:
		if com.Dirty {
			if _, err = com.WriteString(opt.Joint); err != nil {
				return
			}
		}
		if !opt.IsDynamic {
			if err := com.WriteAlias(id); err != nil {
				return err
			}
		}
		if _, err := com.WriteString(sel); err != nil {
			return err
		}
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
		return
	case qtypes.QueryType_EQUAL:
		if com.Dirty {
			if _, err = com.WriteString(opt.Joint); err != nil {
				return
			}
		}
		if !opt.IsDynamic {
			if err := com.WriteAlias(id); err != nil {
				return err
			}
		}
		if _, err := com.WriteString(sel); err != nil {
			return err
		}
		if i.Negation {
			if _, err = com.WriteString(" <> "); err != nil {
				return
			}
		} else {
			if _, err = com.WriteString(" = "); err != nil {
				return
			}
		}
		if err = com.WritePlaceholder(); err != nil {
			return
		}
		com.Add(i.Value())
		com.Dirty = true
	case qtypes.QueryType_GREATER:
		if com.Dirty {
			if _, err = com.WriteString(opt.Joint); err != nil {
				return
			}
		}
		if !opt.IsDynamic {
			if err := com.WriteAlias(id); err != nil {
				return err
			}
		}
		if _, err := com.WriteString(sel); err != nil {
			return err
		}
		if i.Negation {
			if _, err = com.WriteString(" <= "); err != nil {
				return
			}
		} else {
			if _, err = com.WriteString(" > "); err != nil {
				return
			}
		}
		if err = com.WritePlaceholder(); err != nil {
			return
		}
		com.Add(i.Value())
		com.Dirty = true
	case qtypes.QueryType_GREATER_EQUAL:
		if com.Dirty {
			if _, err = com.WriteString(opt.Joint); err != nil {
				return
			}
		}
		if !opt.IsDynamic {
			if err := com.WriteAlias(id); err != nil {
				return err
			}
		}
		if _, err := com.WriteString(sel); err != nil {
			return err
		}
		if i.Negation {
			if _, err = com.WriteString(" < "); err != nil {
				return
			}
		} else {
			if _, err = com.WriteString(" >= "); err != nil {
				return
			}
		}
		if err = com.WritePlaceholder(); err != nil {
			return
		}
		com.Add(i.Value())
		com.Dirty = true
	case qtypes.QueryType_LESS:
		if com.Dirty {
			if _, err = com.WriteString(opt.Joint); err != nil {
				return
			}
		}
		if !opt.IsDynamic {
			if err := com.WriteAlias(id); err != nil {
				return err
			}
		}
		if _, err := com.WriteString(sel); err != nil {
			return err
		}
		if i.Negation {
			if _, err = com.WriteString(" >= "); err != nil {
				return
			}
		} else {
			if _, err = com.WriteString(" < "); err != nil {
				return
			}
		}
		if err = com.WritePlaceholder(); err != nil {
			return
		}
		com.Add(i.Value())
		com.Dirty = true
	case qtypes.QueryType_LESS_EQUAL:
		if com.Dirty {
			if _, err = com.WriteString(opt.Joint); err != nil {
				return
			}
		}
		if !opt.IsDynamic {
			if err := com.WriteAlias(id); err != nil {
				return err
			}
		}
		if _, err := com.WriteString(sel); err != nil {
			return err
		}
		if i.Negation {
			if _, err = com.WriteString(" > "); err != nil {
				return
			}
		} else {
			if _, err = com.WriteString(" <= "); err != nil {
				return
			}
		}
		if err = com.WritePlaceholder(); err != nil {
			return
		}
		com.Add(i.Value())
		com.Dirty = true
	case qtypes.QueryType_CONTAINS:
		if !i.Negation {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}
			if !opt.IsDynamic {
				if err := com.WriteAlias(id); err != nil {
					return err
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			if _, err = com.WriteString(" @> "); err != nil {
				return
			}
			if err = com.WritePlaceholder(); err != nil {
				return
			}
			switch opt.IsJSON {
			case true:
				com.Add(JSONArray` + n + `(i.Values))
			case false:
				com.Add(pq.` + n + `Array(i.Values))
			}
			com.Dirty = true
		}
	case qtypes.QueryType_IS_CONTAINED_BY:
		if !i.Negation {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}
			if !opt.IsDynamic {
				if err := com.WriteAlias(id); err != nil {
					return err
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			if _, err = com.WriteString(" <@ "); err != nil {
				return
			}
			if err = com.WritePlaceholder(); err != nil {
				return
			}
			switch opt.IsJSON {
			case true:
				com.Add(JSONArray` + n + `(i.Values))
			case false:
				com.Add(pq.` + n + `Array(i.Values))
			}
			com.Dirty = true
		}
	case qtypes.QueryType_OVERLAP:
		if !i.Negation {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}
			if !opt.IsDynamic {
				if err := com.WriteAlias(id); err != nil {
					return err
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			if _, err = com.WriteString(" && "); err != nil {
				return
			}
			if err = com.WritePlaceholder(); err != nil {
				return
			}
			switch opt.IsJSON {
			case true:
				com.Add(JSONArray` + n + `(i.Values))
			case false:
				com.Add(pq.` + n + `Array(i.Values))
			}
			com.Dirty = true
		}
	case qtypes.QueryType_HAS_ANY_ELEMENT:
		if !i.Negation {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}
			if !opt.IsDynamic {
				if err := com.WriteAlias(id); err != nil {
					return err
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			if _, err = com.WriteString(" ?| "); err != nil {
				return
			}
			if err = com.WritePlaceholder(); err != nil {
				return
			}
			switch opt.IsJSON {
			case true:
				com.Add(JSONArray` + n + `(i.Values))
			case false:
				com.Add(pq.` + n + `Array(i.Values))
			}
			com.Dirty = true
		}
	case qtypes.QueryType_HAS_ALL_ELEMENTS:
		if !i.Negation {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}
			if !opt.IsDynamic {
				if err := com.WriteAlias(id); err != nil {
					return err
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			if _, err = com.WriteString(" ?& "); err != nil {
				return
			}
			if err = com.WritePlaceholder(); err != nil {
				return
			}
			switch opt.IsJSON {
			case true:
				com.Add(JSONArray` + n + `(i.Values))
			case false:
				com.Add(pq.` + n + `Array(i.Values))
			}
			com.Dirty = true
		}
	case qtypes.QueryType_HAS_ELEMENT:
		if !i.Negation {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}
			if !opt.IsDynamic {
				if err := com.WriteAlias(id); err != nil {
					return err
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
			if _, err = com.WriteString(" ? "); err != nil {
				return
			}
			if err = com.WritePlaceholder(); err != nil {
				return
			}
			com.Add(i.Value())
			com.Dirty = true
		}
	case qtypes.QueryType_IN:
		if len(i.Values) > 0 {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}
			if !opt.IsDynamic {
				if err := com.WriteAlias(id); err != nil {
					return err
				}
			}
			if _, err := com.WriteString(sel); err != nil {
				return err
			}
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
				com.Add(v)
				com.Dirty = true
			}
			if _, err = com.WriteString(")"); err != nil {
				return
			}
		}
	case qtypes.QueryType_BETWEEN:
		if com.Dirty {
			if _, err = com.WriteString(opt.Joint); err != nil {
				return
			}
		}
		if !opt.IsDynamic {
			if err := com.WriteAlias(id); err != nil {
				return err
			}
		}
		if _, err := com.WriteString(sel); err != nil {
			return err
		}
		if i.Negation {
			if _, err = com.WriteString(" <= "); err != nil {
				return
			}
		} else {
			if _, err = com.WriteString(" > "); err != nil {
				return
			}
		}
		if err = com.WritePlaceholder(); err != nil {
			return
		}
		com.Add(i.Values[0])
		if _, err = com.WriteString(" AND "); err != nil {
			return
		}
		if !opt.IsDynamic {
			if err := com.WriteAlias(id); err != nil {
				return err
			}
		}
		if _, err := com.WriteString(sel); err != nil {
			return err
		}
		if i.Negation {
			if _, err = com.WriteString(" >= "); err != nil {
				return
			}
		} else {
			if _, err = com.WriteString(" < "); err != nil {
				return
			}
		}
		if err = com.WritePlaceholder(); err != nil {
			return
		}
		com.Add(i.Values[1])
		com.Dirty = true
	default:
		return fmt.Errorf("pqtgo: unknown int64 query type %s", i.Type.String())
	}

	if com.Dirty {
		if opt.Cast != "" {
			if _, err = com.WriteString(opt.Cast); err != nil {
				return
			}
		}
	}

	return
}`
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
