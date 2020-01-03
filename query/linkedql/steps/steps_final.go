package steps

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query"
	"github.com/cayleygraph/cayley/query/linkedql"
	"github.com/cayleygraph/quad/voc"
)

func init() {
	linkedql.Register(&Select{})
	linkedql.Register(&SelectFirst{})
	linkedql.Register(&Value{})
	linkedql.Register(&Documents{})
}

func newTagsIteratorFrom(from linkedql.PathStep, selected []string, qs graph.QuadStore, ns *voc.Namespaces) (*linkedql.TagsIterator, error) {
	fromPath, err := linkedql.BuildFrom(from, qs, ns)
	if err != nil {
		return nil, err
	}
	valueIt := linkedql.NewValueIterator(fromPath, qs)
	return &linkedql.TagsIterator{ValueIt: valueIt, Selected: selected}, nil
}

func singleValueIteratorFrom(from linkedql.PathStep, qs graph.QuadStore, ns *voc.Namespaces) (*linkedql.ValueIterator, error) {
	fromPath, err := linkedql.BuildFrom(from, qs, ns)
	if err != nil {
		return nil, err
	}
	return linkedql.NewValueIterator(fromPath.Limit(1), qs), nil
}

var _ linkedql.IteratorStep = (*Select)(nil)

// Select corresponds to .select().
type Select struct {
	From linkedql.PathStep `json:"from" minCardinality:"0"`
	Tags []string          `json:"tags"`
}

// Description implements Step.
func (s *Select) Description() string {
	return "Select returns flat records of tags matched in the query"
}

// BuildIterator implements IteratorStep
func (s *Select) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return newTagsIteratorFrom(s.From, s.Tags, qs, ns)
}

var _ linkedql.IteratorStep = (*SelectFirst)(nil)

// SelectFirst corresponds to .selectFirst().
type SelectFirst struct {
	From linkedql.PathStep `json:"from" minCardinality:"0"`
	Tags []string          `json:"tags"`
}

// Description implements Step.
func (s *SelectFirst) Description() string {
	return "Like Select but only returns the first result"
}

// BuildIterator implements IteratorStep
func (s *SelectFirst) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	it, err := singleValueIteratorFrom(s.From, qs, ns)
	if err != nil {
		return nil, err
	}
	return &linkedql.TagsIterator{ValueIt: it, Selected: s.Tags}, nil
}

var _ linkedql.IteratorStep = (*Value)(nil)

// Value corresponds to .value().
type Value struct {
	From linkedql.PathStep `json:"from" minCardinality:"0"`
}

// Description implements Step.
func (s *Value) Description() string {
	return "Value returns a single value matched in the query"
}

// BuildIterator implements IteratorStep
func (s *Value) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	return singleValueIteratorFrom(s.From, qs, ns)
}

var _ linkedql.IteratorStep = (*Documents)(nil)

// Documents corresponds to .documents().
type Documents struct {
	From linkedql.PathStep `json:"from" minCardinality:"0"`
}

// Description implements Step.
func (s *Documents) Description() string {
	return "Documents return documents of the tags matched in the query associated with their entity"
}

// BuildIterator implements IteratorStep
func (s *Documents) BuildIterator(qs graph.QuadStore, ns *voc.Namespaces) (query.Iterator, error) {
	tagsIt, err := newTagsIteratorFrom(s.From, nil, qs, ns)
	if err != nil {
		return nil, err
	}
	return linkedql.NewDocumentIterator(tagsIt), nil
}
