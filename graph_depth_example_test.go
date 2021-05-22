// Copyright ©2020 Dan Kortschak. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gogo_test

import (
	"fmt"
	"strings"

	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/formats/rdf"
	"gonum.org/v1/gonum/graph/traverse"

	"github.com/kortschak/gogo"
)

func ExampleConnectedByAny_depth() {
	// This is an example of obtaining the DAG depth of all GO
	// terms. See the example for Graph for how to load a graph.

	type depth struct {
		level int
		root  rdf.Term
	}
	depths := make(map[rdf.Term]depth)

	// Iterate over all the roots, and walk down the DAG with
	// a breadth first search until exhausted.
	for _, r := range g.Roots(false) {
		depths[r] = depth{level: 0, root: r}
		bf := traverse.BreadthFirst{
			Traverse: func(e graph.Edge) bool {
				// Provide a filter for edges that match our requirement:
				//  - there must be a subClassOf relationship
				//  - the subject must be an obo:GO term (reverse of normal
				//    direction since the traversal is reversed)
				return gogo.ConnectedByAny(e, func(s *rdf.Statement) bool {
					return s.Predicate.Value == "<rdfs:subClassOf>" &&
						strings.HasPrefix(s.Subject.Value, "<obo:GO_")
				})
			},
		}
		bf.Walk(reverse{g}, r, func(n graph.Node, d int) bool {
			depths[n.(rdf.Term)] = depth{level: d, root: r}
			return false
		})
	}

	for t, d := range depths {
		fmt.Printf("depth of %s is %d in %s\n", t.Value, d.level, d.root.Value)
	}
}

// reverse implements the traverse.Graph reversing the direction of edges.
type reverse struct {
	*gogo.Graph
}

func (g reverse) From(id int64) graph.Nodes      { return g.Graph.To(id) }
func (g reverse) Edge(uid, vid int64) graph.Edge { return g.Graph.Edge(vid, uid) }
