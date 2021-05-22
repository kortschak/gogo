// Copyright ©2020 Dan Kortschak. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gogo

import (
	"sort"

	"gonum.org/v1/gonum/graph/formats/rdf"
)

// Query represents a step in a graph query.
type Query struct {
	g *Graph

	terms []rdf.Term
}

// Query returns a query of the receiver starting from the given nodes.
// Queries may not be mixed between distinct graphs.
func (g *Graph) Query(from ...rdf.Term) Query {
	return Query{g: g, terms: from}
}

// Out returns a query holding nodes reachable out from the receiver's
// starting nodes via statements that satisfy fn.
func (q Query) Out(fn func(s *rdf.Statement) bool) Query {
	r := Query{g: q.g}
	for _, s := range q.terms {
		it := q.g.From(s.ID())
		for it.Next() {
			if ConnectedByAny(q.g.Edge(s.ID(), it.Node().ID()), fn) {
				r.terms = append(r.terms, it.Node().(rdf.Term))
			}
		}
	}
	return r
}

// In returns a query holding nodes reachable in from the receiver's
// starting nodes via statements that satisfy fn.
func (q Query) In(fn func(s *rdf.Statement) bool) Query {
	r := Query{g: q.g}
	for _, s := range q.terms {
		it := q.g.To(s.ID())
		for it.Next() {
			if ConnectedByAny(q.g.Edge(it.Node().ID(), s.ID()), fn) {
				r.terms = append(r.terms, it.Node().(rdf.Term))
			}
		}
	}
	return r
}

// And returns a query that holds the disjunction of q and p.
func (q Query) And(p Query) Query {
	if q.g != p.g {
		panic("gogo: binary query operation parameters from distinct graphs")
	}
	sort.Sort(byID(q.terms))
	sort.Sort(byID(p.terms))
	r := Query{g: q.g}
	var i, j int
	for i < len(q.terms) && j < len(p.terms) {
		qi := q.terms[i]
		pj := p.terms[j]
		switch {
		case qi.ID() < pj.ID():
			i++
		case pj.ID() < qi.ID():
			j++
		default:
			r.terms = append(r.terms, qi)
			i++
			j++
		}
	}
	return r
}

// Or returns a query that holds the conjunction of q and p.
func (q Query) Or(p Query) Query {
	if q.g != p.g {
		panic("gogo: binary query operation parameters from distinct graphs")
	}
	sort.Sort(byID(q.terms))
	sort.Sort(byID(p.terms))
	r := Query{g: q.g}
	var i, j int
	for i < len(q.terms) && j < len(p.terms) {
		qi := q.terms[i]
		pj := p.terms[j]
		switch {
		case qi.ID() < pj.ID():
			if len(r.terms) == 0 || r.terms[len(r.terms)-1].UID != qi.UID {
				r.terms = append(r.terms, qi)
			}
			i++
		case pj.ID() < qi.ID():
			if len(r.terms) == 0 || r.terms[len(r.terms)-1].UID != pj.UID {
				r.terms = append(r.terms, pj)
			}
			j++
		default:
			if len(r.terms) == 0 || r.terms[len(r.terms)-1].UID != qi.UID {
				r.terms = append(r.terms, qi)
			}
			i++
			j++
		}
	}
	r.terms = append(r.terms, q.terms[i:]...)
	r.terms = append(r.terms, p.terms[j:]...)
	return r
}

// Not returns a query that holds q less p.
func (q Query) Not(p Query) Query {
	if q.g != p.g {
		panic("gogo: binary query operation parameters from distinct graphs")
	}
	sort.Sort(byID(q.terms))
	sort.Sort(byID(p.terms))
	r := Query{g: q.g}
	var i, j int
	for i < len(q.terms) && j < len(p.terms) {
		qi := q.terms[i]
		pj := p.terms[j]
		switch {
		case qi.ID() < pj.ID():
			r.terms = append(r.terms, qi)
			i++
		case pj.ID() < qi.ID():
			j++
		default:
			i++
		}
	}
	if len(r.terms) < len(q.terms) {
		r.terms = append(r.terms, q.terms[i:len(q.terms)+min(0, i-len(r.terms))]...)
	}
	return r
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Unique returns a copy of the receiver that contains only one instance
// of each term.
func (q Query) Unique() Query {
	sort.Sort(byID(q.terms))
	r := Query{g: q.g}
	for i, t := range q.terms {
		if i == 0 || t.UID != q.terms[i-1].UID {
			r.terms = append(r.terms, t)
		}
	}
	return r
}

// Result returns the terms held by the query.
func (q Query) Result() []rdf.Term {
	return q.terms
}

type byID []rdf.Term

func (n byID) Len() int           { return len(n) }
func (n byID) Less(i, j int) bool { return n[i].ID() < n[j].ID() }
func (n byID) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
