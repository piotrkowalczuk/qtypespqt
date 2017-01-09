package qtypespqt

import (
	"fmt"
	"go/types"
	"strings"

	"github.com/piotrkowalczuk/pqt"
	"github.com/piotrkowalczuk/pqt/pqtgo"
)

type Plugin struct {
	Formatter  *pqtgo.Formatter
	Visibility pqtgo.Visibility
}

func (*Plugin) PropertyType(c *pqt.Column, m int32) string {
	switch {
	case useString(c, m):
		return "*qtypes.String"
	case useInt64(c, m):
		return "*qtypes.Int64"
	case useFloat64(c, m):
		return "*qtypes.Float64"
	case useTimestamp(c, m):
		return "*qtypes.Timestamp"
	}
	return ""
}

func (p *Plugin) SetClause(c *pqt.Column) string {
	return ""
}

// WhereClause implements pqtgo Plugin interface.
func (p *Plugin) WhereClause(c *pqt.Column) string {
	switch {
	case useString(c, 3):
		return fmt.Sprintf(`
			%s({{ .selector }}, {{ .column }}, {{ .composer }}, And)`, p.Formatter.Identifier("queryStringWhereClause"))
	case useInt64(c, 3):
		return fmt.Sprintf(`
			%s({{ .selector }}, {{ .column }}, {{ .composer }}, And)`, p.Formatter.Identifier("queryInt64WhereClause"))
	case useFloat64(c, 3):
		return fmt.Sprintf(`
			%s({{ .selector }}, {{ .column }}, {{ .composer }}, And)`, p.Formatter.Identifier("queryFloat64WhereClause"))
	case useTimestamp(c, 3):
		return fmt.Sprintf(`
		%s({{ .selector }}, {{ .column }}, {{ .composer }}, And)`, p.Formatter.Identifier("queryTimestampWhereClause"))
	}
	return ""
}

// GenAfter implements pqtgo Plugin interface.
func (p *Plugin) Static(s *pqt.Schema) string {
	return `
	` + p.numericWhereClause("Int64") + `
	` + p.numericWhereClause("Float64") + `
	func ` + p.Formatter.Identifier("queryTimestampWhereClause") + `(t *qtypes.Timestamp, sel string, com *Composer, opt *CompositionOpts) error {
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
			com.WriteString(sel)
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
			com.WriteString(sel)
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
			com.WriteString(sel)
			com.WriteString(">")
			com.WritePlaceholder()
			com.Add(t.Value())
		case qtypes.QueryType_GREATER_EQUAL:
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return err
				}
			}
			com.WriteString(sel)
			com.WriteString(">=")
			com.WritePlaceholder()
			com.Add(t.Value())
		case qtypes.QueryType_LESS:
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return err
				}
			}
			com.WriteString(sel)
			com.WriteString(" < ")
			com.WritePlaceholder()
			com.Add(t.Value())
		case qtypes.QueryType_LESS_EQUAL:
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return err
				}
			}
			com.WriteString(sel)
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
				com.WriteString(sel)
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
				com.WriteString(sel)
				com.WriteString(" > ")
				com.WritePlaceholder()
				com.Add(vv1)
				com.WriteString(" AND ")
				com.WriteString(sel)
				com.WriteString(" < ")
				com.WritePlaceholder()
				com.Add(vv2)
			}
		}
	}
	return nil
}
func ` + p.Formatter.Identifier("queryStringWhereClause") + `(s *qtypes.String, sel string, com *Composer, opt *CompositionOpts) (err error) {
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
		if _, err = com.WriteString(sel); err != nil {
			return
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
		if _, err = com.WriteString(sel); err != nil {
			return
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
		if _, err = com.WriteString(sel); err != nil {
			return
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
		if _, err = com.WriteString(sel); err != nil {
			return
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
		if _, err = com.WriteString(sel); err != nil {
			return
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
			if _, err = com.WriteString(sel); err != nil {
				return
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

			com.Add(JSONArrayString(s.Values))
			com.Dirty = true
		}
	case qtypes.QueryType_IS_CONTAINED_BY:
		if !s.Negation {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}
			if _, err = com.WriteString(sel); err != nil {
				return
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

			com.Add(s.Value())
			com.Dirty = true
		}
	case qtypes.QueryType_OVERLAP:
		if !s.Negation {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}
			if _, err = com.WriteString(sel); err != nil {
				return
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

			com.Add(s.Value())
			com.Dirty = true
		}
	case qtypes.QueryType_HAS_ANY_ELEMENT:
		if !s.Negation {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}
			if _, err = com.WriteString(sel); err != nil {
				return
			}
			if _, err = com.WriteString(" ?| "); err != nil {
				return
			}
			if err = com.WritePlaceholder(); err != nil {
				return
			}
			com.Add(pq.StringArray(s.Values))
			com.Dirty = true
		}
	case qtypes.QueryType_HAS_ALL_ELEMENTS:
		if !s.Negation {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}
			if _, err = com.WriteString(sel); err != nil {
				return
			}
			if _, err = com.WriteString(" ?& "); err != nil {
				return
			}
			if err = com.WritePlaceholder(); err != nil {
				return
			}
			com.Add(pq.StringArray(s.Values))
			com.Dirty = true
		}
	case qtypes.QueryType_IN:
		if len(s.Values) > 0 {
			if com.Dirty {
				if _, err = com.WriteString(opt.Joint); err != nil {
					return
				}
			}
			if _, err = com.WriteString(sel); err != nil {
				return
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
	return `func ` + p.Formatter.Identifier("query", n, "where", "clause") + `(i *qtypes.` + n + `, sel string, com *Composer, opt *CompositionOpts) (err error) {
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
		com.WriteString(sel)
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
		if _, err = com.WriteString(sel); err != nil {
			return
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
		if _, err = com.WriteString(sel); err != nil {
			return
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
		if _, err = com.WriteString(sel); err != nil {
			return
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
		if _, err = com.WriteString(sel); err != nil {
			return
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
		if _, err = com.WriteString(sel); err != nil {
			return
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
			if _, err = com.WriteString(sel); err != nil {
				return
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
			if _, err = com.WriteString(sel); err != nil {
				return
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
			if _, err = com.WriteString(sel); err != nil {
				return
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
			if _, err = com.WriteString(sel); err != nil {
				return
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
			if _, err = com.WriteString(sel); err != nil {
				return
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
			if _, err = com.WriteString(sel); err != nil {
				return
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
			if _, err = com.WriteString(sel); err != nil {
				return
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
		if _, err = com.WriteString(sel); err != nil {
			return
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
		if _, err = com.WriteString(sel); err != nil {
			return
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

func useInt64(c *pqt.Column, m int32) (use bool) {
	if m != 3 {
		return false
	}
	switch t := c.Type.(type) {
	case pqtgo.BuiltinType:
		switch types.BasicKind(t) {
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
		switch t {
		case pqt.TypeIntegerSmall(), pqt.TypeInteger(), pqt.TypeIntegerBig():
			use = true
		case pqt.TypeSerialSmall(), pqt.TypeSerial(), pqt.TypeSerialBig():
			use = true
		default:
			switch {
			case strings.HasPrefix(t.String(), "INTEGER["):
				use = true
			case strings.HasPrefix(t.String(), "BIGINT["):
				use = true
			case strings.HasPrefix(t.String(), "SMALLINT["):
				use = true
			}
		}
	}
	return
}

func useFloat64(c *pqt.Column, m int32) (use bool) {
	if m != 3 {
		return false
	}
	switch t := c.Type.(type) {
	case pqtgo.BuiltinType:
		switch types.BasicKind(t) {
		case types.Float32:
			use = true
		case types.Float64:
			use = true
		}
	case pqt.BaseType:
		switch t {
		case pqt.TypeDoublePrecision():
			use = true
		default:
			switch {
			case strings.HasPrefix(t.String(), "DECIMAL"):
				use = true
			case strings.HasPrefix(t.String(), "NUMERIC"):
				use = true
			case strings.HasPrefix(t.String(), "DOUBLE PRECISION["):
				use = true
			}
		}
	}
	return
}

func useString(c *pqt.Column, m int32) (use bool) {
	if m != 3 {
		return false
	}

	switch t := c.Type.(type) {
	case pqtgo.BuiltinType:
		switch types.BasicKind(t) {
		case types.String:
			use = true
		}
	case pqt.BaseType:
		switch t {
		case pqt.TypeText():
			use = true
		case pqt.TypeUUID():
			use = true
		default:
			switch {
			case strings.HasPrefix(c.Type.String(), "TEXT["):
				use = true
			case strings.HasPrefix(c.Type.String(), "VARCHAR"), strings.HasPrefix(c.Type.String(), "CHARACTER["):
				use = true
			}
		}
	}
	return
}

func useTimestamp(c *pqt.Column, m int32) (use bool) {
	if m != 3 {
		return false
	}
	switch t := c.Type.(type) {
	case pqt.BaseType:
		switch t {
		case pqt.TypeTimestamp(), pqt.TypeTimestampTZ():
			use = true
		}
	}
	return
}
