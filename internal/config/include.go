package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// ExpandIncludes expands custom !include directives within raw YAML bytes.
// baseDir is used to resolve relative include paths.
func ExpandIncludes(raw []byte, baseDir string) ([]byte, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	if len(doc.Content) == 0 {
		return raw, nil
	}
	seen := map[string]struct{}{}
	root := doc.Content[0]
	if err := expandNode(root, baseDir, seen); err != nil {
		return nil, err
	}
	out, err := yaml.Marshal(root)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func expandNode(n *yaml.Node, baseDir string, seen map[string]struct{}) error {
	if n == nil {
		return nil
	}

	// Handle tag-based include on any node
	if p, ok := parseIncludeTag(n.Tag); ok {
		// If the tag carries a path suffix, use it; otherwise fall back to node content
		if p != "" {
			inc, err := loadIncludedRoot(p, baseDir, seen)
			if err != nil {
				return err
			}
			// Merge/replace depending on node kind
			switch n.Kind {
			case yaml.MappingNode:
				if inc.Kind != yaml.MappingNode {
					return fmt.Errorf("!include tag on mapping must include a mapping")
				}
				// Expand current mapping children before merging
				for i := 1; i < len(n.Content); i += 2 {
					if err := expandNode(n.Content[i], baseDir, seen); err != nil {
						return err
					}
				}
				merged := cloneNodeDeep(inc)
				if err := mergeMap(merged, n); err != nil {
					return err
				}
				*n = *merged
				return nil
			case yaml.SequenceNode:
				if inc.Kind != yaml.SequenceNode {
					return fmt.Errorf("!include tag on sequence must include a sequence")
				}
				for _, c := range n.Content {
					if err := expandNode(c, baseDir, seen); err != nil {
						return err
					}
				}
				n.Content = append(cloneNodesDeep(inc.Content), n.Content...)
				return nil
			default:
				// scalar or alias: replace with included root
				*n = *inc
				return nil
			}
		}
		// Tag present but no path suffix: treat like old behavior (value- or seq-based)
		inc, err := expandIncludeNode(n, baseDir, seen)
		if err != nil {
			return err
		}
		*n = *inc
		return nil
	}

	switch n.Kind {
	case yaml.DocumentNode:
		if len(n.Content) == 0 {
			return nil
		}
		return expandNode(n.Content[0], baseDir, seen)

	case yaml.MappingNode:
		// Build a new mapping with includes merged first, then local keys.
		dst := &yaml.Node{Kind: yaml.MappingNode}

		// First pass: process include keys
		// Content layout: [k1, v1, k2, v2, ...]
		nonIncludePairs := make([]*yaml.Node, 0, len(n.Content))
		for i := 0; i < len(n.Content); i += 2 {
			k := n.Content[i]
			v := n.Content[i+1]
			if k.Tag == "!include" || k.Value == "!include" {
				incNodes, err := includeListFromValue(v, baseDir, seen)
				if err != nil {
					return err
				}
				for _, inc := range incNodes {
					if inc.Kind != yaml.MappingNode {
						return fmt.Errorf("!include at mapping scope must resolve to a mapping, got kind %d", inc.Kind)
					}
					if err := mergeMap(dst, inc); err != nil {
						return err
					}
				}
				continue
			}
			nonIncludePairs = append(nonIncludePairs, k, v)
		}

		// Second pass: process non-include pairs and merge into dst
		for i := 0; i < len(nonIncludePairs); i += 2 {
			k := nonIncludePairs[i]
			v := nonIncludePairs[i+1]
			if err := expandNode(v, baseDir, seen); err != nil {
				return err
			}
			// Find existing entry
			idx := indexOfKey(dst, k.Value)
			if idx == -1 {
				// append as-is
				dst.Content = append(dst.Content, cloneNode(k), v)
				continue
			}
			// Merge with existing value
			existingVal := dst.Content[idx+1]
			merged, err := mergeValue(existingVal, v)
			if err != nil {
				return err
			}
			dst.Content[idx+1] = merged
		}

		*n = *dst
		return nil

	case yaml.SequenceNode:
		out := make([]*yaml.Node, 0, len(n.Content))
		for _, c := range n.Content {
			if c.Tag == "!include" {
				inc, err := expandIncludeNode(c, baseDir, seen)
				if err != nil {
					return err
				}
				if inc.Kind == yaml.SequenceNode {
					// splice children
					for _, gc := range inc.Content {
						if err := expandNode(gc, baseDir, seen); err != nil {
							return err
						}
						out = append(out, gc)
					}
				} else {
					if err := expandNode(inc, baseDir, seen); err != nil {
						return err
					}
					out = append(out, inc)
				}
				continue
			}
			if err := expandNode(c, baseDir, seen); err != nil {
				return err
			}
			out = append(out, c)
		}
		n.Content = out
		return nil

	default:
		// Scalar or alias: nothing to do
		return nil
	}
}

// expandIncludeNode expands a node tagged with !include.
// Supported forms:
// - Scalar: a single path
// - Sequence: a list of paths
func expandIncludeNode(n *yaml.Node, baseDir string, seen map[string]struct{}) (*yaml.Node, error) {
	switch n.Kind {
	case yaml.ScalarNode:
		inc, err := loadIncludedRoot(n.Value, baseDir, seen)
		if err != nil {
			return nil, err
		}
		return inc, nil
	case yaml.SequenceNode:
		// Merge a list of includes into a sequence; flatten nested sequences.
		res := &yaml.Node{Kind: yaml.SequenceNode}
		for _, it := range n.Content {
			if it.Kind != yaml.ScalarNode {
				return nil, errors.New("!include sequence must contain only scalar file paths")
			}
			inc, err := loadIncludedRoot(it.Value, baseDir, seen)
			if err != nil {
				return nil, err
			}
			if inc.Kind == yaml.SequenceNode {
				res.Content = append(res.Content, inc.Content...)
			} else {
				res.Content = append(res.Content, inc)
			}
		}
		return res, nil
	default:
		return nil, fmt.Errorf("unsupported !include node kind: %d", n.Kind)
	}
}

// includeListFromValue expands a value node for a mapping-level !include key
// and returns a list of mapping roots to merge.
func includeListFromValue(v *yaml.Node, baseDir string, seen map[string]struct{}) ([]*yaml.Node, error) {
	// Allow either scalar path or a sequence of paths
	if v.Tag == "!include" {
		vn, err := expandIncludeNode(v, baseDir, seen)
		if err != nil {
			return nil, err
		}
		v = vn
	}
	switch v.Kind {
	case yaml.ScalarNode:
		inc, err := loadIncludedRoot(v.Value, baseDir, seen)
		if err != nil {
			return nil, err
		}
		return []*yaml.Node{inc}, nil
	case yaml.SequenceNode:
		res := make([]*yaml.Node, 0, len(v.Content))
		for _, it := range v.Content {
			if it.Tag == "!include" {
				var err error
				it, err = expandIncludeNode(it, baseDir, seen)
				if err != nil {
					return nil, err
				}
			}
			if it.Kind != yaml.ScalarNode {
				// If a previous expansion produced a non-scalar, accept it directly
				res = append(res, it)
				continue
			}
			inc, err := loadIncludedRoot(it.Value, baseDir, seen)
			if err != nil {
				return nil, err
			}
			res = append(res, inc)
		}
		return res, nil
	default:
		return nil, fmt.Errorf("!include mapping value must be scalar or sequence, got kind %d", v.Kind)
	}
}

// parseIncludeTag returns (path, true) if the tag is of the form "!include" or "!include:<path>"
func parseIncludeTag(tag string) (string, bool) {
	if tag == "!include" {
		return "", true
	}
	const prefix = "!include:"
	if len(tag) > len(prefix) && tag[:len(prefix)] == prefix {
		return tag[len(prefix):], true
	}
	return "", false
}

func loadIncludedRoot(path string, baseDir string, seen map[string]struct{}) (*yaml.Node, error) {
	p := path
	if !filepath.IsAbs(p) {
		p = filepath.Join(baseDir, p)
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return nil, err
	}
	if _, ok := seen[abs]; ok {
		return nil, fmt.Errorf("include cycle detected for %s", abs)
	}
	seen[abs] = struct{}{}
	defer delete(seen, abs)

	b, err := os.ReadFile(abs)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("included file not found: %s", path)
		}
		return nil, err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("invalid YAML in included file %s: %w", abs, err)
	}
	if len(doc.Content) == 0 {
		return &yaml.Node{Kind: yaml.MappingNode}, nil
	}
	root := doc.Content[0]
	// Recurse into the included content with its own baseDir
	if err := expandNode(root, filepath.Dir(abs), seen); err != nil {
		return nil, err
	}
	return root, nil
}

func indexOfKey(m *yaml.Node, key string) int {
	for i := 0; i < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return i
		}
	}
	return -1
}

func mergeMap(dst, src *yaml.Node) error {
	if dst.Kind != yaml.MappingNode || src.Kind != yaml.MappingNode {
		return errors.New("mergeMap expects mapping nodes")
	}
	for i := 0; i < len(src.Content); i += 2 {
		sk := src.Content[i]
		sv := src.Content[i+1]
		// Ensure children are expanded already; callers expand before merging
		idx := indexOfKey(dst, sk.Value)
		if idx == -1 {
			dst.Content = append(dst.Content, cloneNode(sk), cloneNodeDeep(sv))
			continue
		}
		mv := dst.Content[idx+1]
		merged, err := mergeValue(mv, sv)
		if err != nil {
			return err
		}
		dst.Content[idx+1] = merged
	}
	return nil
}

func mergeValue(a, b *yaml.Node) (*yaml.Node, error) {
	// map + map => recursive merge
	if a.Kind == yaml.MappingNode && b.Kind == yaml.MappingNode {
		// Work on a clone to avoid mutating caller's reference besides return
		out := cloneNodeDeep(a)
		if err := mergeMap(out, b); err != nil {
			return nil, err
		}
		return out, nil
	}
	// seq + seq => concat
	if a.Kind == yaml.SequenceNode && b.Kind == yaml.SequenceNode {
		out := &yaml.Node{Kind: yaml.SequenceNode}
		out.Content = append(out.Content, cloneNodesDeep(a.Content)...)
		out.Content = append(out.Content, cloneNodesDeep(b.Content)...)
		return out, nil
	}
	// different kinds => override with b
	return cloneNodeDeep(b), nil
}

func cloneNode(n *yaml.Node) *yaml.Node {
	if n == nil {
		return nil
	}
	c := *n
	c.Content = nil // shallow clone without children
	return &c
}

func cloneNodeDeep(n *yaml.Node) *yaml.Node {
	if n == nil {
		return nil
	}
	c := *n
	if len(n.Content) > 0 {
		c.Content = make([]*yaml.Node, len(n.Content))
		for i, ch := range n.Content {
			c.Content[i] = cloneNodeDeep(ch)
		}
	}
	return &c
}

func cloneNodesDeep(in []*yaml.Node) []*yaml.Node {
	out := make([]*yaml.Node, len(in))
	for i, n := range in {
		out[i] = cloneNodeDeep(n)
	}
	return out
}
