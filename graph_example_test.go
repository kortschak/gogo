// Copyright ©2020 Dan Kortschak. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gogo_test

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"

	"gonum.org/v1/gonum/graph/formats/rdf"

	"github.com/kortschak/gogo"
)

func ExampleGraph() {
	f, err := os.Open("path/to/go.nt.gz")
	if err != nil {
		log.Fatal(err)
	}
	r, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}

	dec := rdf.NewDecoder(r)
	var statements []*rdf.Statement
	for {
		s, err := dec.Unmarshal()
		if err != nil {
			if err != io.EOF {
				log.Fatalf("error during decoding: %v", err)
			}
			break
		}

		// Statements can be filtered at this point to exclude unwanted
		// or irrelevant parts of the graph.
		statements = append(statements, s)
	}
	f.Close()

	// Canonicalise blank nodes to reduce memory footprint.
	statements, err = rdf.URDNA2015(statements, statements)
	if err != nil {
		log.Fatal(err)
	}

	g := gogo.NewGraph()
	for _, s := range statements {
		g.AddStatement(s)
	}

	// Do something with the graph.
}

var g *gogo.Graph

func init() {
	f, err := os.Open("testdata/go.nt.gz")
	if err != nil {
		log.Fatal(err)
	}
	r, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}

	dec := rdf.NewDecoder(r)
	var statements []*rdf.Statement
	for {
		s, err := dec.Unmarshal()
		if err != nil {
			if err != io.EOF {
				log.Fatalf("error during decoding: %v", err)
			}
			break
		}
		statements = append(statements, s)
	}
	f.Close()

	statements, err = rdf.URDNA2015(statements, statements)
	if err != nil {
		log.Fatal(err)
	}

	g = gogo.NewGraph()
	for _, s := range statements {
		g.AddStatement(s)
	}
}

func ExampleGraph_Roots() {
	// Check that all GO terms are rooted in the three ontology sources.

	// If force is false, the available subset of roots is returned
	// if none is found or force is true then a search is made from
	// all parts of the DAG.
	for _, r := range g.Roots(true) {
		fmt.Println(r.Value)
	}

	// Unordered output:
	//
	// <http://purl.obolibrary.org/obo/GO_0003674>
	// <http://purl.obolibrary.org/obo/GO_0005575>
	// <http://purl.obolibrary.org/obo/GO_0008150>
}

func ExampleGraph_ClosestCommonAncestor() {
	// Find the closest common ancestor of two terms in the GO DAG.

	// G protein-coupled bile acid receptor activity.
	aIRI := "<http://purl.obolibrary.org/obo/GO_0038182>"
	a, ok := g.TermFor(aIRI)
	if !ok {
		log.Fatalf("no node for %v", aIRI)
	}

	// coreceptor activity involved in canonical Wnt signaling pathway.
	bIRI := "<http://purl.obolibrary.org/obo/GO_1904928>"
	b, ok := g.TermFor(bIRI)
	if !ok {
		log.Fatalf("no node for %v", bIRI)
	}

	// We expect the closest common ancestor to be signaling receptor
	// activity <http://purl.obolibrary.org/obo/GO_0038023>.
	cca, ok := g.ClosestCommonAncestor(a, b)

	if !ok {
		log.Fatal("no common ancestor")
	}
	fmt.Printf("Closest common ancestor of %s and %s is %s", aIRI, bIRI, cca.Value)

	// Output:
	//
	// Closest common ancestor of <http://purl.obolibrary.org/obo/GO_0038182> and <http://purl.obolibrary.org/obo/GO_1904928> is <http://purl.obolibrary.org/obo/GO_0038023>
}

func ExampleGraph_IsDescendantOf() {
	// Check whether a term is a GO descendant of another in the sub-class
	// hierarchy.

	// coreceptor activity involved in canonical Wnt signaling pathway.
	aIRI := "<http://purl.obolibrary.org/obo/GO_1904928>"
	a, ok := g.TermFor(aIRI)
	if !ok {
		log.Fatalf("no node for %v", aIRI)
	}

	// signaling receptor activity.
	bIRI := "<http://purl.obolibrary.org/obo/GO_0038023>"
	b, ok := g.TermFor(bIRI)
	if !ok {
		log.Fatalf("no node for %v", bIRI)
	}

	yes, depth := g.IsDescendantOf(a, b)
	fmt.Printf("%s is descendant of %s = %t (%d levels apart)\n", bIRI, aIRI, yes, depth)

	yes, depth = g.IsDescendantOf(b, a)
	fmt.Printf("%s is descendant of %s = %t (%d levels apart)\n", aIRI, bIRI, yes, depth)

	// Output:
	//
	// <http://purl.obolibrary.org/obo/GO_0038023> is descendant of <http://purl.obolibrary.org/obo/GO_1904928> = false (-1 levels apart)
	// <http://purl.obolibrary.org/obo/GO_1904928> is descendant of <http://purl.obolibrary.org/obo/GO_0038023> = true (3 levels apart)
}

func ExampleGraph_DescendantsOf() {
	// Find all the descendants of a term and their relative distance from
	// the sub-root.

	// canonical Wnt signaling pathway.
	aIRI := "<http://purl.obolibrary.org/obo/GO_0060070>"
	a, ok := g.TermFor(aIRI)
	if !ok {
		log.Fatalf("no node for %v", aIRI)
	}

	for _, d := range g.DescendantsOf(a) {
		fmt.Printf("%s %d\n", d.Term.Value, d.Depth)
	}

	// Unordered output:
	//
	// <http://purl.obolibrary.org/obo/GO_0003267> 2
	// <http://purl.obolibrary.org/obo/GO_0044328> 1
	// <http://purl.obolibrary.org/obo/GO_0044329> 1
	// <http://purl.obolibrary.org/obo/GO_0044330> 1
	// <http://purl.obolibrary.org/obo/GO_0044334> 1
	// <http://purl.obolibrary.org/obo/GO_0044335> 1
	// <http://purl.obolibrary.org/obo/GO_0044336> 1
	// <http://purl.obolibrary.org/obo/GO_0044337> 1
	// <http://purl.obolibrary.org/obo/GO_0044338> 1
	// <http://purl.obolibrary.org/obo/GO_0044339> 1
	// <http://purl.obolibrary.org/obo/GO_0044340> 1
	// <http://purl.obolibrary.org/obo/GO_0060823> 1
	// <http://purl.obolibrary.org/obo/GO_0060901> 1
	// <http://purl.obolibrary.org/obo/GO_0061290> 1
	// <http://purl.obolibrary.org/obo/GO_0061292> 1
	// <http://purl.obolibrary.org/obo/GO_0061316> 1
	// <http://purl.obolibrary.org/obo/GO_0100012> 1
	// <http://purl.obolibrary.org/obo/GO_0100067> 1
	// <http://purl.obolibrary.org/obo/GO_1904954> 1
	// <http://purl.obolibrary.org/obo/GO_0003136> 2
	// <http://purl.obolibrary.org/obo/GO_0044343> 2
	// <http://purl.obolibrary.org/obo/GO_0061291> 2
	// <http://purl.obolibrary.org/obo/GO_0061293> 2
	// <http://purl.obolibrary.org/obo/GO_0061310> 2
	// <http://purl.obolibrary.org/obo/GO_0061315> 2
	// <http://purl.obolibrary.org/obo/GO_0061317> 2
	// <http://purl.obolibrary.org/obo/GO_0061324> 2
	// <http://purl.obolibrary.org/obo/GO_1905474> 2
}
