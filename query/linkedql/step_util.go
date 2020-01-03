package linkedql

import (
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/query/path"
	"github.com/cayleygraph/quad/voc"
)

// BuildFrom returns for given from step a path to be used by PathStep's BuildPath
func BuildFrom(from PathStep, qs graph.QuadStore, ns *voc.Namespaces) (*path.Path, error) {
	if from == nil {
		return path.StartPath(qs), nil
	}
	return from.BuildPath(qs, ns)
}
