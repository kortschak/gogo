// Copyright ©2020 Dan Kortschak. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Copyright ©2014 The Gonum Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gogo

import (
	"fmt"
	"strings"

	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/formats/rdf"
	"gonum.org/v1/gonum/graph/iterator"
	"gonum.org/v1/gonum/graph/multi"
	"gonum.org/v1/gonum/graph/set/uid"
	"gonum.org/v1/gonum/graph/traverse"
)

// Graph implements a Gene Ontology graph.
type Graph struct {
	nodes map[int64]graph.Node
	from  map[int64]map[int64]map[int64]graph.Line
	to    map[int64]map[int64]map[int64]graph.Line
	pred  map[int64]map[*rdf.Statement]bool

	termIDs map[string]int64
	ids     *uid.Set

	namespace int
}

const (
	local   = iota - 1
	unknown //nolint:deadcode,unused,varcheck
	global
)

// NewGraph returns a new empty Graph.
func NewGraph() *Graph {
	return &Graph{
		nodes: make(map[int64]graph.Node),
		from:  make(map[int64]map[int64]map[int64]graph.Line),
		to:    make(map[int64]map[int64]map[int64]graph.Line),
		pred:  make(map[int64]map[*rdf.Statement]bool),

		termIDs: make(map[string]int64),
		ids:     uid.NewSet(),
	}
}

// addNode adds n to the graph. It panics if the added node ID matches an
// existing node ID.
func (g *Graph) addNode(n graph.Node) {
	if _, exists := g.nodes[n.ID()]; exists {
		panic(fmt.Sprintf("gogo: node ID collision: %d", n.ID()))
	}
	g.nodes[n.ID()] = n
	g.ids.Use(n.ID())
}

// AddStatement adds s to the graph. It panics rdf.Term UIDs in the statement
// are not consistent with existing terms in the graph. Statements must not
// be altered while being held by the graph. If the UID fields of the terms
// in s are zero, they will be set to values consistent with the rest of the
// graph on return, mutating the parameter, otherwise the UIDs must match terms
// that already exist in the graph. The statement must be a valid RDF statement
// otherwise AddStatement will panic. Predicate IRIs must either all be
// globally namespaced (prefixed with the http scheme) or all use the qualified
// name prefix, otherwise AddStatement will panic. Subject and object IRIs
// should match.
func (g *Graph) AddStatement(s *rdf.Statement) {
	text, _, kind, err := s.Predicate.Parts()
	if err != nil {
		panic(fmt.Errorf("gogo: error extracting predicate: %w", err))
	}
	if kind != rdf.IRI {
		panic(fmt.Errorf("gogo: predicate is not an IRI: %s", s.Predicate.Value))
	}
	if strings.HasPrefix(text, "http:") {
		if g.namespace == local {
			panic(fmt.Errorf("gogo: adding predicate with global IRI to locally namespaced graph: %s", s.Predicate.Value))
		}
		g.namespace = global
	} else {
		if g.namespace == global {
			panic(fmt.Errorf("gogo: adding predicate with local IRI to globally namespaced graph: %s", s.Predicate.Value))
		}
		g.namespace = local
	}

	// The http URI subject and objects in the owl:Ontology prevent us
	// checking for correct namespacing of objects until we have the
	// entire graph loaded, so we don't check.

	_, _, kind, err = s.Subject.Parts()
	if err != nil {
		panic(fmt.Errorf("gogo: error extracting subject: %w", err))
	}
	switch kind {
	case rdf.IRI, rdf.Blank:
	default:
		panic(fmt.Errorf("gogo: subject is not an IRI or blank node: %s", s.Subject.Value))
	}

	_, _, kind, err = s.Object.Parts()
	if err != nil {
		panic(fmt.Errorf("gogo: error extracting object: %w", err))
	}
	if kind == rdf.Invalid {
		panic(fmt.Errorf("gogo: object is not a valid term: %s", s.Object.Value))
	}

	statements, ok := g.pred[s.Predicate.UID]
	if !ok {
		statements = make(map[*rdf.Statement]bool)
		g.pred[s.Predicate.UID] = statements
	}
	statements[s] = true
	g.addTerm(&s.Subject)
	g.addTerm(&s.Predicate)
	g.addTerm(&s.Object)
	g.setLine(s)
}

// addTerm adds t to the graph. It panics if the added node ID matches an existing node ID.
func (g *Graph) addTerm(t *rdf.Term) {
	if t.UID == 0 {
		id, ok := g.termIDs[t.Value]
		if ok {
			t.UID = id
			return
		}
		id = g.ids.NewID()
		g.ids.Use(id)
		t.UID = id
		g.termIDs[t.Value] = id
		return
	}

	id, ok := g.termIDs[t.Value]
	if !ok {
		g.termIDs[t.Value] = t.UID
	} else if id != t.UID {
		panic(fmt.Sprintf("gogo: term ID collision: term:%s new ID:%d old ID:%d", t.Value, t.UID, id))
	}
}

// AllStatements returns an iterator of the statements that make up the graph.
func (g *Graph) AllStatements() *Statements {
	return &Statements{eit: g.Edges()}
}

// ClosestCommonAncestor returns the term that is the closest common ancestor
// of a and b if it exists in g.
func (g *Graph) ClosestCommonAncestor(a, b rdf.Term) (r rdf.Term, ok bool) {
	var goTerm, subClassOf string
	switch g.namespace {
	case local:
		goTerm = "<obo:GO_"
		subClassOf = "<rdfs:subClassOf>"
	case global:
		goTerm = "<http://purl.obolibrary.org/obo/GO_"
		subClassOf = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>"
	default:
		return
	}
	if !strings.HasPrefix(a.Value, goTerm) || !strings.HasPrefix(b.Value, goTerm) {
		return
	}

	if a == b {
		return a, true
	}

	seen := make(map[int64]bool)
	var bf traverse.BreadthFirst
	bf.Traverse = func(e graph.Edge) bool {
		return ConnectedByAny(e, func(s *rdf.Statement) bool {
			return strings.HasPrefix(s.Object.Value, goTerm) && s.Predicate.Value == subClassOf
		})
	}
	bf.Walk(g, a, func(n graph.Node, d int) bool {
		seen[n.ID()] = true
		return false
	})
	bf.Reset()
	bf.Walk(g, b, func(n graph.Node, d int) bool {
		if seen[n.ID()] {
			r = n.(rdf.Term)
			ok = true
			return true
		}
		return false
	})
	return r, ok
}

// DescendantsOf returns all of the descendants of the given term.
func (g *Graph) DescendantsOf(t rdf.Term) []Descendant {
	var goTerm, subClassOf string
	switch g.namespace {
	case local:
		goTerm = "<obo:GO_"
		subClassOf = "<rdfs:subClassOf>"
	case global:
		goTerm = "<http://purl.obolibrary.org/obo/GO_"
		subClassOf = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>"
	default:
		return nil
	}
	if !strings.HasPrefix(t.Value, goTerm) {
		return nil
	}
	var desc []Descendant
	var bf traverse.BreadthFirst
	bf.Traverse = func(e graph.Edge) bool {
		return ConnectedByAny(e, func(s *rdf.Statement) bool {
			return strings.HasPrefix(s.Subject.Value, goTerm) && s.Predicate.Value == subClassOf
		})
	}
	bf.Walk(reverse{g}, t, func(n graph.Node, d int) bool {
		if n != t {
			desc = append(desc, Descendant{Term: n.(rdf.Term), Depth: d})
		}
		return false
	})
	return desc
}

// reverse implements the traverse.Graph reversing the direction of edges.
type reverse struct {
	*Graph
}

func (g reverse) From(id int64) graph.Nodes      { return g.Graph.To(id) }
func (g reverse) Edge(uid, vid int64) graph.Edge { return g.Graph.Edge(vid, uid) }

// Descendant represents a descendancy relationship.
type Descendant struct {
	Term  rdf.Term
	Depth int
}

// Edge returns the edge from u to v if such an edge exists and nil otherwise.
// The node v must be directly reachable from u as defined by the From method.
// The returned graph.Edge is a multi.Edge if an edge exists.
func (g *Graph) Edge(uid, vid int64) graph.Edge {
	l := g.Lines(uid, vid)
	if l == nil {
		return nil
	}
	return multi.Edge{F: g.Node(uid), T: g.Node(vid), Lines: l}
}

// Edges returns all the edges in the graph. Each edge in the returned slice
// is a multi.Edge.
func (g *Graph) Edges() graph.Edges {
	if len(g.nodes) == 0 {
		return graph.Empty
	}
	var edges []graph.Edge
	for _, u := range g.nodes {
		for _, e := range g.from[u.ID()] {
			var lines []graph.Line
			for _, l := range e {
				lines = append(lines, l)
			}
			if len(lines) != 0 {
				edges = append(edges, multi.Edge{
					F:     g.Node(u.ID()),
					T:     g.Node(lines[0].To().ID()),
					Lines: iterator.NewOrderedLines(lines),
				})
			}
		}
	}
	if len(edges) == 0 {
		return graph.Empty
	}
	return iterator.NewOrderedEdges(edges)
}

// From returns all nodes in g that can be reached directly from n.
//
// The returned graph.Nodes is only valid until the next mutation of
// the receiver.
func (g *Graph) From(id int64) graph.Nodes {
	if len(g.from[id]) == 0 {
		return graph.Empty
	}
	return iterator.NewNodesByLines(g.nodes, g.from[id])
}

// FromSubject returns all nodes in g that can be reached directly from an
// RDF subject term.
//
// The returned graph.Nodes is only valid until the next mutation of
// the receiver.
func (g *Graph) FromSubject(t rdf.Term) graph.Nodes {
	return g.From(t.UID)
}

// HasEdgeBetween returns whether an edge exists between nodes x and y without
// considering direction.
func (g *Graph) HasEdgeBetween(xid, yid int64) bool {
	if _, ok := g.from[xid][yid]; ok {
		return true
	}
	_, ok := g.from[yid][xid]
	return ok
}

// HasEdgeFromTo returns whether an edge exists in the graph from u to v.
func (g *Graph) HasEdgeFromTo(uid, vid int64) bool {
	_, ok := g.from[uid][vid]
	return ok
}

// IsDescendantOf returns whether the query q is a descendant of a and how
// many levels separate them if it is. If q is not a descendant of a, depth
// will be negative.
func (g *Graph) IsDescendantOf(a, q rdf.Term) (yes bool, depth int) {
	depth = -1
	var goTerm, subClassOf string
	switch g.namespace {
	case local:
		goTerm = "<obo:GO_"
		subClassOf = "<rdfs:subClassOf>"
	case global:
		goTerm = "<http://purl.obolibrary.org/obo/GO_"
		subClassOf = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>"
	default:
		return
	}
	if !strings.HasPrefix(a.Value, goTerm) {
		return
	}
	if !strings.HasPrefix(q.Value, goTerm) {
		return
	}
	var bf traverse.BreadthFirst
	bf.Traverse = func(e graph.Edge) bool {
		return ConnectedByAny(e, func(s *rdf.Statement) bool {
			return strings.HasPrefix(s.Object.Value, goTerm) && s.Predicate.Value == subClassOf
		})
	}
	bf.Walk(g, q, func(n graph.Node, d int) bool {
		if n == a {
			yes = true
			depth = d
			return true
		}
		return false
	})
	return yes, depth
}

// Lines returns the lines from u to v if such any such lines exists and nil otherwise.
// The node v must be directly reachable from u as defined by the From method.
func (g *Graph) Lines(uid, vid int64) graph.Lines {
	edge := g.from[uid][vid]
	if len(edge) == 0 {
		return graph.Empty
	}
	var lines []graph.Line
	for _, l := range edge {
		lines = append(lines, l)
	}
	return iterator.NewOrderedLines(lines)
}

// newLine returns a new Line from the source to the destination node.
// The returned Line will have a graph-unique ID.
// The Line's ID does not become valid in g until the Line is added to g.
func (g *Graph) newLine(from, to graph.Node) graph.Line {
	return multi.Line{F: from, T: to, UID: g.ids.NewID()}
}

// newNode returns a new unique Node to be added to g. The Node's ID does
// not become valid in g until the Node is added to g.
func (g *Graph) newNode() graph.Node {
	if len(g.nodes) == 0 {
		return multi.Node(0)
	}
	if int64(len(g.nodes)) == uid.Max {
		panic("gogo: cannot allocate node: no slot")
	}
	return multi.Node(g.ids.NewID())
}

// Node returns the node with the given ID if it exists in the graph,
// and nil otherwise.
func (g *Graph) Node(id int64) graph.Node {
	return g.nodes[id]
}

// TermFor returns the rdf.Term for the given text. The text must be
// an exact match for the rdf.Term's Value field.
func (g *Graph) TermFor(text string) (term rdf.Term, ok bool) {
	id, ok := g.termIDs[text]
	if !ok {
		return
	}
	n, ok := g.nodes[id]
	if !ok {
		var s map[*rdf.Statement]bool
		s, ok = g.pred[id]
		if !ok {
			return
		}
		for k := range s {
			return k.Predicate, true
		}
	}
	return n.(rdf.Term), true
}

// Nodes returns all the nodes in the graph.
//
// The returned graph.Nodes is only valid until the next mutation of
// the receiver.
func (g *Graph) Nodes() graph.Nodes {
	if len(g.nodes) == 0 {
		return graph.Empty
	}
	return iterator.NewNodes(g.nodes)
}

// Predicates returns a slice of all the predicates used in the graph.
func (g *Graph) Predicates() []rdf.Term {
	p := make([]rdf.Term, len(g.pred))
	i := 0
	for _, statements := range g.pred {
		for s := range statements {
			p[i] = s.Predicate
			i++
			break
		}
	}
	return p
}

// removeLine removes the line with the given end point and line IDs from
// the graph, leaving the terminal nodes. If the line does not exist it is
// a no-op.
func (g *Graph) removeLine(fid, tid, id int64) {
	if _, ok := g.nodes[fid]; !ok {
		return
	}
	if _, ok := g.nodes[tid]; !ok {
		return
	}

	delete(g.from[fid][tid], id)
	if len(g.from[fid][tid]) == 0 {
		delete(g.from[fid], tid)
	}
	delete(g.to[tid][fid], id)
	if len(g.to[tid][fid]) == 0 {
		delete(g.to[tid], fid)
	}

	g.ids.Release(id)
}

// removeNode removes the node with the given ID from the graph, as well as
// any edges attached to it. If the node is not in the graph it is a no-op.
func (g *Graph) removeNode(id int64) {
	if _, ok := g.nodes[id]; !ok {
		return
	}
	delete(g.nodes, id)

	for from := range g.from[id] {
		delete(g.to[from], id)
	}
	delete(g.from, id)

	for to := range g.to[id] {
		delete(g.from[to], id)
	}
	delete(g.to, id)

	g.ids.Release(id)
}

// RemoveStatement removes s from the graph, leaving the terminal nodes if they
// are part of another statement. If the statement does not exist in g it is a no-op.
func (g *Graph) RemoveStatement(s *rdf.Statement) {
	if !g.pred[s.Predicate.UID][s] {
		return
	}

	// Remove the connection.
	g.removeLine(s.Subject.UID, s.Object.UID, s.Predicate.UID)
	statements := g.pred[s.Predicate.UID]
	delete(statements, s)
	if len(statements) == 0 {
		delete(g.pred, s.Predicate.UID)
		if len(g.from[s.Predicate.UID]) == 0 {
			g.ids.Release(s.Predicate.UID)
			delete(g.termIDs, s.Predicate.Value)
		}
	}

	// Remove any orphan terms.
	if g.From(s.Subject.UID).Len() == 0 && g.To(s.Subject.UID).Len() == 0 {
		g.removeNode(s.Subject.UID)
		delete(g.termIDs, s.Subject.Value)
	}
	if g.From(s.Object.UID).Len() == 0 && g.To(s.Object.UID).Len() == 0 {
		g.removeNode(s.Object.UID)
		delete(g.termIDs, s.Object.Value)
	}
}

// RemoveTerm removes t and any statements referencing t from the graph. If
// the term is a predicate, all statements with the predicate are removed. If
// the term does not exist it is a no-op.
func (g *Graph) RemoveTerm(t rdf.Term) {
	// Remove any predicates.
	if statements, ok := g.pred[t.UID]; ok {
		for s := range statements {
			g.RemoveStatement(s)
		}
	}

	// Quick return.
	_, nok := g.nodes[t.UID]
	_, fok := g.from[t.UID]
	_, tok := g.to[t.UID]
	if !nok && !fok && !tok {
		return
	}

	// Remove any statements that than impinge on the term.
	to := g.From(t.UID)
	for to.Next() {
		lines := g.Lines(t.UID, to.Node().ID())
		for lines.Next() {
			g.RemoveStatement(lines.Line().(*rdf.Statement))
		}
	}
	from := g.To(t.UID)
	if from.Next() {
		lines := g.Lines(from.Node().ID(), t.UID)
		for lines.Next() {
			g.RemoveStatement(lines.Line().(*rdf.Statement))
		}
	}

	// Remove the node.
	g.removeNode(t.UID)
	delete(g.termIDs, t.Value)
}

// Roots returns all the roots of the graph. It will first attempt to find
// roots from the three known roots molecular_function, cellular_component
// and biological_process in the appropriate namespace and if none can be
// found, will search from all GO terms for the complete set of roots. If
// force is true, a complete search will be done.
func (g *Graph) Roots(force bool) []rdf.Term {
	var goTerm, subClassOf, deprecated, w3True string
	var standardRoots []string
	switch g.namespace {
	case local:
		goTerm = "<obo:GO_"
		subClassOf = "<rdfs:subClassOf>"
		deprecated = "<owl:deprecated>"
		w3True = `"true"^^<xsd:boolean>`
		standardRoots = []string{
			"<obo:GO_0003674>", // molecular_function
			"<obo:GO_0005575>", // cellular_component
			"<obo:GO_0008150>", // biological_process
		}
	case global:
		goTerm = "<http://purl.obolibrary.org/obo/GO_"
		subClassOf = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>"
		deprecated = "<http://www.w3.org/2002/07/owl#deprecated>"
		w3True = `"true"^^<http://www.w3.org/2001/XMLSchema#boolean>`
		standardRoots = []string{
			"<http://purl.obolibrary.org/obo/GO_0003674>", // molecular_function
			"<http://purl.obolibrary.org/obo/GO_0005575>", // cellular_component
			"<http://purl.obolibrary.org/obo/GO_0008150>", // biological_process
		}
	default:
		return nil
	}

	rootSet := make(map[rdf.Term]bool)

	// First check for standard roots.
	for _, r := range standardRoots {
		if t, ok := g.TermFor(r); ok {
			rootSet[t] = true
		}
	}

	// If we have any roots and we haven't been
	// asked to force finding all roots, we're done.
	// Otherwise, search from all nodes to find
	// their roots.
	if force || len(rootSet) == 0 {
		for _, n := range g.nodes {
			t := n.(rdf.Term)
			var df traverse.DepthFirst
			df.Traverse = func(e graph.Edge) bool {
				return ConnectedByAny(e, func(s *rdf.Statement) bool {
					return strings.HasPrefix(s.Object.Value, goTerm) && s.Predicate.Value == subClassOf
				})
			}
			final := df.Walk(g, t, func(n graph.Node) bool {
				t := n.(rdf.Term)
				if !strings.HasPrefix(t.Value, goTerm) {
					return false
				}
				// Ignore deprecated terms since they may be dead ends.
				dep := g.Query(t).Out(func(s *rdf.Statement) bool {
					return s.Predicate.Value == deprecated && s.Object.Value == w3True
				})
				if len(dep.Result()) != 0 {
					return false
				}

				// If we can reach another subclass, we are not done yet.
				more := g.Query(t).Out(func(s *rdf.Statement) bool {
					return strings.HasPrefix(s.Object.Value, goTerm) && s.Predicate.Value == subClassOf
				})
				return len(more.Result()) == 0
			})
			if final != nil {
				rootSet[final.(rdf.Term)] = true
			}
		}
	}

	var roots []rdf.Term
	for r := range rootSet {
		roots = append(roots, r)
	}

	return roots
}

// setLine adds l, a line from one node to another. If the nodes do not exist,
// they are added, and are set to the nodes of the line otherwise.
func (g *Graph) setLine(l graph.Line) {
	var (
		from = l.From()
		fid  = from.ID()
		to   = l.To()
		tid  = to.ID()
		lid  = l.ID()
	)

	if _, ok := g.nodes[fid]; !ok {
		g.addNode(from)
	} else {
		g.nodes[fid] = from
	}
	if _, ok := g.nodes[tid]; !ok {
		g.addNode(to)
	} else {
		g.nodes[tid] = to
	}

	switch {
	case g.from[fid] == nil:
		g.from[fid] = map[int64]map[int64]graph.Line{tid: {lid: l}}
	case g.from[fid][tid] == nil:
		g.from[fid][tid] = map[int64]graph.Line{lid: l}
	default:
		g.from[fid][tid][lid] = l
	}
	switch {
	case g.to[tid] == nil:
		g.to[tid] = map[int64]map[int64]graph.Line{fid: {lid: l}}
	case g.to[tid][fid] == nil:
		g.to[tid][fid] = map[int64]graph.Line{lid: l}
	default:
		g.to[tid][fid][lid] = l
	}

	g.ids.Use(lid)
}

// Statements returns an iterator of the statements that connect the subject
// term node u to the object term node v.
func (g *Graph) Statements(uid, vid int64) *Statements {
	return &Statements{lit: g.Lines(uid, vid)}
}

// To returns all nodes in g that can reach directly to n.
//
// The returned graph.Nodes is only valid until the next mutation of
// the receiver.
func (g *Graph) To(id int64) graph.Nodes {
	if len(g.to[id]) == 0 {
		return graph.Empty
	}
	return iterator.NewNodesByLines(g.nodes, g.to[id])
}

// ToObject returns all nodes in g that can reach directly to an RDF object
// term.
//
// The returned graph.Nodes is only valid until the next mutation of
// the receiver.
func (g *Graph) ToObject(t rdf.Term) graph.Nodes {
	return g.To(t.UID)
}

// Statements is an RDF statement iterator.
type Statements struct {
	eit graph.Edges
	lit graph.Lines
}

// Next returns whether the iterator holds any additional statements.
func (s *Statements) Next() bool {
	if s.lit != nil && s.lit.Next() {
		return true
	}
	if s.eit == nil || !s.eit.Next() {
		return false
	}
	s.lit = s.eit.Edge().(multi.Edge).Lines
	return s.lit.Next()
}

// Statement returns the current statement.
func (s *Statements) Statement() *rdf.Statement {
	return s.lit.Line().(*rdf.Statement)
}

// ConnectedByAny is a helper function to for simplifying graph traversal
// conditions.
func ConnectedByAny(e graph.Edge, with func(*rdf.Statement) bool) bool {
	it, ok := e.(multi.Edge)
	if !ok {
		return false
	}
	for it.Next() {
		s, ok := it.Line().(*rdf.Statement)
		if !ok {
			continue
		}
		ok = with(s)
		if ok {
			return true
		}
	}
	return false
}
