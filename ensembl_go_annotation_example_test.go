// Copyright ©2020 The Gonum Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gogo_test

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"gonum.org/v1/gonum/graph/formats/rdf"

	"github.com/kortschak/gogo"
)

func ExampleQuery_annotation() {
	g := gogo.NewGraph()
	var dec rdf.Decoder
	// Takes two command line parameters, an N-Triples containing
	// the SO_transcribed_from predicates of homo_sapiens.ttl and
	// an N-Triples containing the <rdfs:seeAlso> <obo:GO_*>
	// statements of homo_sapiens_xrefs.ttl.
	for _, path := range os.Args[1:] {
		f, err := os.Open(path)
		if err != nil {
			log.Fatal(err)
		}
		r, err := gzip.NewReader(f)
		if err != nil {
			log.Fatal(err)
		}

		dec.Reset(r)
		var statements []*rdf.Statement
		for {
			s, err := dec.Unmarshal()
			if err != nil {
				if err != io.EOF {
					log.Fatalf("error during decoding: %v", err)
				}
				break
			}

			s.Subject.UID = 0
			s.Predicate.UID = 0
			s.Object.UID = 0
			statements = append(statements, s)
		}
		f.Close()

		for _, s := range statements {
			g.AddStatement(s)
		}
	}

	nodes := g.Nodes()
	for nodes.Next() {
		gene := nodes.Node().(rdf.Term)
		if !strings.HasPrefix(gene.Value, "<ensembl:") {
			continue
		}

		// We are emitting directly, so we need to ensure statement
		// uniqueness. A seen per start node is enough for this. If
		// we were adding to another graph, the deduplication could
		// be handled by the destination graph.
		seen := make(map[int64]bool)

		// Get all GO terms reachable from the ENSG via an ENST
		// since that is how the Ensembl GO annotation work.
		terms := g.Query(gene).In(func(s *rdf.Statement) bool {
			// <transcript:Y> <obo:SO_transcribed_from> <ensembl:X> .
			return s.Predicate.Value == "<obo:SO_transcribed_from>"

		}).Out(func(s *rdf.Statement) bool {
			if seen[s.Object.UID] {
				return false
			}

			// <transcript:Y> <rdfs:seeAlso> <obo:GO_Z> .
			ok := s.Predicate.Value == "<rdfs:seeAlso>" &&
				strings.HasPrefix(s.Object.Value, "<obo:GO_")
			if ok {
				seen[s.Object.UID] = true
			}
			return ok

		}).Result()

		for _, t := range terms {
			fmt.Println(&rdf.Statement{
				Subject:   rdf.Term{Value: t.Value},
				Predicate: rdf.Term{Value: "<local:annotates>"},
				Object:    rdf.Term{Value: gene.Value},
			})
		}
	}
}
