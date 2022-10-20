// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/kvcache"
)

/*
	From MySQL 5.7, JSON path expression grammar:
		pathExpression ::= scope (jsonPathLeg)*
		scope ::= [ columnReference ] '$'
		columnReference ::= // omit...
		jsonPathLeg ::= member | arrayLocation | '**'
		member ::= '.' (keyName | '*')
		arrayLocation ::= '[' (non-negative-integer | '*') ']'
		keyName ::= ECMAScript-identifier | ECMAScript-string-literal

	And some implementation limits in MySQL 5.7:
		1) columnReference in scope must be empty now;
		2) double asterisk(**) could not be last leg;

	Examples:
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.a') -> "b"
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.c') -> [1, "2"]
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.a', '$.c') -> ["b", [1, "2"]]
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[0]') -> 1
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[2]') -> NULL
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[*]') -> [1, "2"]
		select json_extract('{"a": "b", "c": [1, "2"]}', '$.*') -> ["b", [1, "2"]]
*/

type jsonPathArrayIndex struct {
	fromLast bool
	index    int
}

func (i jsonPathArrayIndex) getIndexFromStart(obj BinaryJSON) int {
	if i.fromLast {
		return obj.GetElemCount() - 1 - i.index
	}

	return i.index
}

func jsonPathArrayIndexFromStart(index int) jsonPathArrayIndex {
	return jsonPathArrayIndex{
		fromLast: false,
		index:    index,
	}
}

func jsonPathArrayIndexFromLast(index int) jsonPathArrayIndex {
	return jsonPathArrayIndex{
		fromLast: true,
		index:    index,
	}
}

type jsonPathArraySelection interface {
	jsonPathArraySelection()
}

var _ jsonPathArraySelection = jsonPathArraySelectionAsterisk{}
var _ jsonPathArraySelection = jsonPathArraySelectionIndex{}
var _ jsonPathArraySelection = jsonPathArraySelectionRange{}

type jsonPathArraySelectionAsterisk struct{}

func (jsonPathArraySelectionAsterisk) jsonPathArraySelection() {}

type jsonPathArraySelectionIndex struct {
	index jsonPathArrayIndex
}

func (jsonPathArraySelectionIndex) jsonPathArraySelection() {}

// jsonPathArraySelectionRange represents a closed interval
type jsonPathArraySelectionRange struct {
	start jsonPathArrayIndex
	end   jsonPathArrayIndex
}

func (jsonPathArraySelectionRange) jsonPathArraySelection() {}

type jsonPathLegType byte

const (
	// jsonPathLegKey indicates the path leg with '.key'.
	jsonPathLegKey jsonPathLegType = 0x01
	// jsonPathLegArraySelection indicates the path leg with form '[index]', '[index to index]'.
	jsonPathLegArraySelection jsonPathLegType = 0x02
	// jsonPathLegDoubleAsterisk indicates the path leg with form '**'.
	jsonPathLegDoubleAsterisk jsonPathLegType = 0x03
)

// jsonPathLeg is only used by JSONPathExpression.
type jsonPathLeg struct {
	typ            jsonPathLegType
	arraySelection jsonPathArraySelection // if typ is jsonPathLegArraySelection, the value should be parsed into here.
	dotKey         string                 // if typ is jsonPathLegKey, the key should be parsed into here.
}

// jsonPathExpressionFlag holds attributes of JSONPathExpression
type jsonPathExpressionFlag byte

const (
	jsonPathExpressionContainsAsterisk       jsonPathExpressionFlag = 0x01
	jsonPathExpressionContainsDoubleAsterisk jsonPathExpressionFlag = 0x02
	jsonPathExpressionContainsRange          jsonPathExpressionFlag = 0x04
)

// containsAnyAsterisk returns true if pef contains any asterisk.
func (pef jsonPathExpressionFlag) containsAnyAsterisk() bool {
	pef &= jsonPathExpressionContainsAsterisk | jsonPathExpressionContainsDoubleAsterisk
	return byte(pef) != 0
}

// containsAnyRange returns true if pef contains any range.
func (pef jsonPathExpressionFlag) containsAnyRange() bool {
	pef &= jsonPathExpressionContainsRange
	return byte(pef) != 0
}

// JSONPathExpression is for JSON path expression.
type JSONPathExpression struct {
	legs  []jsonPathLeg
	flags jsonPathExpressionFlag
}

func (pe JSONPathExpression) clone() JSONPathExpression {
	legs := make([]jsonPathLeg, len(pe.legs))
	copy(legs, pe.legs)
	return JSONPathExpression{legs: legs, flags: pe.flags}
}

var peCache JSONPathExpressionCache

type jsonPathExpressionKey string

func (key jsonPathExpressionKey) Hash() []byte {
	return hack.Slice(string(key))
}

// JSONPathExpressionCache is a cache for JSONPathExpression.
type JSONPathExpressionCache struct {
	mu    sync.Mutex
	cache *kvcache.SimpleLRUCache
}

// popOneLeg returns a jsonPathLeg, and a child JSONPathExpression without that leg.
func (pe JSONPathExpression) popOneLeg() (jsonPathLeg, JSONPathExpression) {
	newPe := JSONPathExpression{
		legs:  pe.legs[1:],
		flags: 0,
	}
	for _, leg := range newPe.legs {
		if leg.typ == jsonPathLegArraySelection {
			switch leg.arraySelection.(type) {
			case jsonPathArraySelectionAsterisk:
				{
					newPe.flags |= jsonPathExpressionContainsAsterisk
				}
			case jsonPathArraySelectionRange:
				{
					newPe.flags |= jsonPathExpressionContainsRange
				}
			}
		} else if leg.typ == jsonPathLegKey && leg.dotKey == "*" {
			newPe.flags |= jsonPathExpressionContainsAsterisk
		} else if leg.typ == jsonPathLegDoubleAsterisk {
			newPe.flags |= jsonPathExpressionContainsDoubleAsterisk
		}
	}
	return pe.legs[0], newPe
}

// popOneLastLeg returns the parent JSONPathExpression and the last jsonPathLeg
func (pe JSONPathExpression) popOneLastLeg() (JSONPathExpression, jsonPathLeg) {
	lastLegIdx := len(pe.legs) - 1
	lastLeg := pe.legs[lastLegIdx]
	// It is used only in modification, it has been checked that there is no asterisks.
	return JSONPathExpression{legs: pe.legs[:lastLegIdx]}, lastLeg
}

// pushBackOneArraySelectionLeg pushback one leg of INDEX type
func (pe JSONPathExpression) pushBackOneArraySelectionLeg(arraySelection jsonPathArraySelection) JSONPathExpression {
	newPe := JSONPathExpression{
		legs:  append(pe.legs, jsonPathLeg{typ: jsonPathLegArraySelection, arraySelection: arraySelection}),
		flags: pe.flags,
	}
	switch arraySelection.(type) {
	case jsonPathArraySelectionAsterisk:
		newPe.flags |= jsonPathExpressionContainsAsterisk
	case jsonPathArraySelectionRange:
		newPe.flags |= jsonPathExpressionContainsRange
	}
	return newPe
}

// pushBackOneKeyLeg pushback one leg of KEY type
func (pe JSONPathExpression) pushBackOneKeyLeg(key string) JSONPathExpression {
	newPe := JSONPathExpression{
		legs:  append(pe.legs, jsonPathLeg{typ: jsonPathLegKey, dotKey: key}),
		flags: pe.flags,
	}
	if key == "*" {
		newPe.flags |= jsonPathExpressionContainsAsterisk
	}
	return newPe
}

// ContainsAnyAsterisk returns true if pe contains any asterisk.
func (pe JSONPathExpression) ContainsAnyAsterisk() bool {
	return pe.flags.containsAnyAsterisk()
}

// ContainsAnyRange returns true if pe contains any range.
func (pe JSONPathExpression) ContainsAnyRange() bool {
	return pe.flags.containsAnyRange()
}

type jsonPathStream struct {
	pathExpr string
	pos      int
}

func (s *jsonPathStream) skipWhiteSpace() {
	for ; s.pos < len(s.pathExpr); s.pos++ {
		if !unicode.IsSpace(rune(s.pathExpr[s.pos])) {
			break
		}
	}
}

func (s *jsonPathStream) read() byte {
	b := s.pathExpr[s.pos]
	s.pos++
	return b
}

func (s *jsonPathStream) peek() byte {
	return s.pathExpr[s.pos]
}

func (s *jsonPathStream) skip(i int) {
	s.pos += i
}

func (s *jsonPathStream) exhausted() bool {
	return s.pos >= len(s.pathExpr)
}

func (s *jsonPathStream) readWhile(f func(byte) bool) (str string, metEnd bool) {
	start := s.pos
	for ; !s.exhausted(); s.skip(1) {
		if !f(s.peek()) {
			return s.pathExpr[start:s.pos], false
		}
	}
	return s.pathExpr[start:s.pos], true
}

func parseJSONPathExpr(pathExpr string) (pe JSONPathExpression, err error) {
	s := &jsonPathStream{pathExpr: pathExpr, pos: 0}
	s.skipWhiteSpace()
	if s.exhausted() || s.read() != '$' {
		return JSONPathExpression{}, ErrInvalidJSONPath.GenWithStackByArgs(1)
	}
	s.skipWhiteSpace()

	pe.legs = make([]jsonPathLeg, 0, 16)
	pe.flags = jsonPathExpressionFlag(0)

	var ok bool

	for !s.exhausted() {
		switch s.peek() {
		case '.':
			ok = parseJSONPathMember(s, &pe)
		case '[':
			ok = parseJSONPathArray(s, &pe)
		case '*':
			ok = parseJSONPathWildcard(s, &pe)
		default:
			ok = false
		}

		if !ok {
			return JSONPathExpression{}, ErrInvalidJSONPath.GenWithStackByArgs(s.pos)
		}

		s.skipWhiteSpace()
	}

	if len(pe.legs) > 0 && pe.legs[len(pe.legs)-1].typ == jsonPathLegDoubleAsterisk {
		return JSONPathExpression{}, ErrInvalidJSONPath.GenWithStackByArgs(s.pos)
	}

	return
}

func parseJSONPathWildcard(s *jsonPathStream, p *JSONPathExpression) bool {
	s.skip(1)
	if s.exhausted() || s.read() != '*' {
		return false
	}
	if s.exhausted() || s.peek() == '*' {
		return false
	}

	p.flags |= jsonPathExpressionContainsDoubleAsterisk
	p.legs = append(p.legs, jsonPathLeg{typ: jsonPathLegDoubleAsterisk})
	return true
}

type parseResult int

const (
	// parseSuccess means this function parses the content successfully
	parseSuccess parseResult = 0

	// parseError means this function failed to parse the content, but the upper level parser could consider other options
	parseError parseResult = 1

	// parseFailure means this function faces some unrecoverable errors, it'll not backtrack and upper level should report
	// the failure directly
	parseFailure parseResult = 2
)

func (s *jsonPathStream) tryReadString(expected string) parseResult {
	recordPos := s.pos

	i := 0
	str, meetEnd := s.readWhile(func(b byte) bool {
		i += 1
		return i <= len(expected)
	})

	if meetEnd {
		s.pos = recordPos
		return parseError
	}

	if str != expected {
		s.pos = recordPos
		return parseError
	}
	return parseSuccess
}

func (s *jsonPathStream) tryReadInt() (int, parseResult) {
	recordPos := s.pos

	str, meetEnd := s.readWhile(func(b byte) bool {
		return b >= '0' && b <= '9'
	})
	if meetEnd {
		s.pos = recordPos
		return 0, parseError
	}

	index, err := strconv.Atoi(str)
	if err != nil {
		s.pos = recordPos
		return 0, parseError
	}

	return index, parseSuccess
}

// tryParseArrayIndexFromStart tries to read a 'number' array index
// if failed, the stream will not be pushed forward
func tryParseArrayIndexFromStart(s *jsonPathStream) (jsonPathArrayIndex, parseResult) {
	recordPos := s.pos

	index, result := s.tryReadInt()
	switch result {
	case parseError:
		s.pos = recordPos
		return jsonPathArrayIndex{}, parseError
	case parseFailure:
		return jsonPathArrayIndex{}, parseFailure
	}

	if index > math.MaxUint32 {
		return jsonPathArrayIndex{}, parseFailure
	}

	return jsonPathArrayIndexFromStart(index), parseSuccess
}

// tryParseArrayIndexLast tries to read a 'last' array index
// if failed, the stream will not be pushed forward
func tryParseArrayIndexLast(s *jsonPathStream) (jsonPathArrayIndex, parseResult) {
	recordPos := s.pos

	result := s.tryReadString("last")
	switch result {
	case parseError:
		s.pos = recordPos
		return jsonPathArrayIndex{}, parseError
	case parseFailure:
		return jsonPathArrayIndex{}, parseFailure
	}

	return jsonPathArrayIndexFromLast(0), parseSuccess
}

// tryParseSequence runs the parsers one by one
// if one of these parsers meet error, it will return the error. If the error is a failure, it'll not backtrack.
func tryParseSequence(jsonParsers ...func(s *jsonPathStream) (interface{}, parseResult)) func(s *jsonPathStream) ([]interface{}, parseResult) {
	return func(s *jsonPathStream) ([]interface{}, parseResult) {
		recordPos := s.pos

		returnValue := make([]interface{}, 0, len(jsonParsers))
		for _, parser := range jsonParsers {
			val, result := parser(s)

			switch result {
			case parseError:
				s.pos = recordPos
				return nil, parseError
			case parseFailure:
				return nil, parseFailure
			}

			returnValue = append(returnValue, val)
		}

		return returnValue, parseSuccess
	}
}

// tryParseOpt tries the parsers until a successful one or failure occurs
func tryParseOpt[T interface{}](jsonParsers ...func(s *jsonPathStream) (T, parseResult)) func(s *jsonPathStream) (T, parseResult) {
	return func(s *jsonPathStream) (T, parseResult) {
		var empty T
		recordPos := s.pos

		for _, parser := range jsonParsers {
			val, result := parser(s)

			switch result {
			case parseError:
				continue
			case parseFailure:
				return empty, parseFailure
			}

			return val, parseSuccess
		}

		s.pos = recordPos
		return empty, parseError
	}
}

// tryParseArrayIndexFromLast tries to read a 'last - number' array index
// if failed, the stream will not be pushed forward
func tryParseArrayIndexFromLast(s *jsonPathStream) (jsonPathArrayIndex, parseResult) {
	recordPos := s.pos

	vals, result := tryParseSequence(
		func(s *jsonPathStream) (interface{}, parseResult) {
			return nil, s.tryReadString("last")
		},
		func(s *jsonPathStream) (interface{}, parseResult) {
			s.skipWhiteSpace()
			return nil, parseSuccess
		},
		func(s *jsonPathStream) (interface{}, parseResult) {
			return nil, s.tryReadString("-")
		},
		func(s *jsonPathStream) (interface{}, parseResult) {
			s.skipWhiteSpace()
			return nil, parseSuccess
		},
		func(s *jsonPathStream) (interface{}, parseResult) {
			return s.tryReadInt()
		},
	)(s)
	switch result {
	case parseError:
		s.pos = recordPos
		return jsonPathArrayIndex{}, parseError
	case parseFailure:
		return jsonPathArrayIndex{}, parseFailure
	}

	index := vals[len(vals)-1].(int)
	if index > math.MaxUint32 {
		return jsonPathArrayIndex{}, parseFailure
	}

	return jsonPathArrayIndexFromLast(index), parseSuccess
}

// tryParseArrayIndex tries to read an arrayIndex, which is 'number', 'last' or 'last - number'
// if failed, the stream will not be pushed forward
func tryParseArrayIndex(s *jsonPathStream) (jsonPathArrayIndex, parseResult) {
	recordPos := s.pos

	// Same part of the stream is parsed multiple times (e.g. try to parse 'last' twice)
	// but to keep simplicity, it's not optimized.

	// The following three function calls cannot be reordered, because any 'last - number'
	// will make 'last' match success, and the parser don't have higher level backtracking

	index, result := tryParseOpt(
		tryParseArrayIndexFromLast,
		tryParseArrayIndexLast,
		tryParseArrayIndexFromStart,
	)(s)
	switch result {
	case parseError:
		s.pos = recordPos
		return jsonPathArrayIndex{}, parseError
	case parseFailure:
		return jsonPathArrayIndex{}, parseFailure
	}

	return index, parseSuccess
}

func tryParseArraySelectionRange(s *jsonPathStream) (jsonPathArraySelection, parseResult) {
	recordPos := s.pos

	vals, result := tryParseSequence(
		func(s *jsonPathStream) (interface{}, parseResult) {
			return tryParseArrayIndex(s)
		},
		func(s *jsonPathStream) (interface{}, parseResult) {
			recordPos := s.pos

			if !unicode.IsSpace(rune(s.peek())) {
				s.pos = recordPos
				return nil, parseError
			}
			s.skipWhiteSpace()
			// "to" must be rounded with whitespaces
			result := s.tryReadString("to")
			switch result {
			case parseError:
				s.pos = recordPos
				return nil, parseError
			case parseFailure:
				return nil, parseFailure
			}
			if !unicode.IsSpace(rune(s.peek())) {
				return nil, parseFailure
			}
			s.skipWhiteSpace()
			return nil, parseSuccess
		},
		func(s *jsonPathStream) (interface{}, parseResult) {
			// after successfully get a "to", it must be a range selection

			end, result := tryParseArrayIndex(s)
			switch result {
			case parseError:
				return nil, parseFailure
			case parseFailure:
				return nil, parseFailure
			}

			return end, parseSuccess
		},
	)(s)

	switch result {
	case parseError:
		s.pos = recordPos
		return nil, parseError
	case parseFailure:
		return nil, parseFailure
	}

	start := vals[0].(jsonPathArrayIndex)
	end := vals[len(vals)-1].(jsonPathArrayIndex)

	if !start.fromLast && !end.fromLast {
		if start.index > end.index {
			return nil, parseFailure
		}
	}
	if start.fromLast && end.fromLast {
		if start.index < end.index {
			return nil, parseFailure
		}
	}

	return jsonPathArraySelectionRange{start: start, end: end}, parseSuccess
}

func tryParseArraySelectionIndex(s *jsonPathStream) (jsonPathArraySelection, parseResult) {
	recordPos := s.pos

	index, result := tryParseArrayIndex(s)
	switch result {
	case parseError:
		s.pos = recordPos
		return nil, parseError
	case parseFailure:
		return nil, parseFailure
	}

	return jsonPathArraySelectionIndex{index: index}, parseSuccess
}

func tryParseArraySelectionAsterisk(s *jsonPathStream) (jsonPathArraySelection, parseResult) {
	recordPos := s.pos

	if s.exhausted() {
		s.pos = recordPos
		return nil, parseError
	}

	char := s.read()
	if char != '*' {
		s.pos = recordPos
		return nil, parseError
	}

	return jsonPathArraySelectionAsterisk{}, parseSuccess
}

func tryParseArraySelection(s *jsonPathStream) (jsonPathArraySelection, parseResult) {
	recordPos := s.pos

	if s.exhausted() || s.read() != '[' {
		s.pos = recordPos
		return nil, parseError
	}

	s.skipWhiteSpace()

	// these attempts cannot be reordered, because if it's a range, try parsing
	// the index would also success.
	selection, result := tryParseOpt(
		tryParseArraySelectionAsterisk,
		tryParseArraySelectionRange,
		tryParseArraySelectionIndex,
	)(s)
	switch result {
	case parseError:
		return nil, parseFailure
	case parseFailure:
		return nil, parseFailure
	}

	s.skipWhiteSpace()

	if s.exhausted() || s.read() != ']' {
		return nil, parseFailure
	}

	return selection, parseSuccess
}

func parseJSONPathArray(s *jsonPathStream, p *JSONPathExpression) bool {
	selection, result := tryParseArraySelection(s)
	if result != parseSuccess {
		return false
	}

	switch selection.(type) {
	case jsonPathArraySelectionAsterisk:
		p.flags |= jsonPathExpressionContainsAsterisk
	case jsonPathArraySelectionRange:
		p.flags |= jsonPathExpressionContainsRange
	}

	p.legs = append(p.legs, jsonPathLeg{typ: jsonPathLegArraySelection, arraySelection: selection})
	return true
}

func parseJSONPathMember(s *jsonPathStream, p *JSONPathExpression) bool {
	var err error
	s.skip(1)
	s.skipWhiteSpace()
	if s.exhausted() {
		return false
	}

	if s.peek() == '*' {
		s.skip(1)
		p.flags |= jsonPathExpressionContainsAsterisk
		p.legs = append(p.legs, jsonPathLeg{typ: jsonPathLegKey, dotKey: "*"})
	} else {
		var dotKey string
		var wasQuoted bool
		if s.peek() == '"' {
			s.skip(1)
			str, meetEnd := s.readWhile(func(b byte) bool {
				if b == '\\' {
					s.skip(1)
					return true
				}
				return b != '"'
			})
			if meetEnd {
				return false
			}
			s.skip(1)
			dotKey = str
			wasQuoted = true
		} else {
			dotKey, _ = s.readWhile(func(b byte) bool {
				return !(unicode.IsSpace(rune(b)) || b == '.' || b == '[' || b == '*')
			})
		}
		dotKey = "\"" + dotKey + "\""

		if !json.Valid(hack.Slice(dotKey)) {
			return false
		}
		dotKey, err = unquoteJSONString(dotKey[1 : len(dotKey)-1])
		if err != nil || (!wasQuoted && !isEcmascriptIdentifier(dotKey)) {
			return false
		}

		p.legs = append(p.legs, jsonPathLeg{typ: jsonPathLegKey, dotKey: dotKey})
	}
	return true
}

func isEcmascriptIdentifier(s string) bool {
	if s == "" {
		return false
	}

	for i := 0; i < len(s); i++ {
		if (i != 0 && s[i] >= '0' && s[i] <= '9') ||
			(s[i] >= 'a' && s[i] <= 'z') || (s[i] >= 'A' && s[i] <= 'Z') ||
			s[i] == '_' || s[i] == '$' || s[i] >= 0x80 {
			continue
		}
		return false
	}
	return true
}

// ParseJSONPathExpr parses a JSON path expression. Returns a JSONPathExpression
// object which can be used in JSON_EXTRACT, JSON_SET and so on.
func ParseJSONPathExpr(pathExpr string) (JSONPathExpression, error) {
	peCache.mu.Lock()
	val, ok := peCache.cache.Get(jsonPathExpressionKey(pathExpr))
	if ok {
		peCache.mu.Unlock()
		return val.(JSONPathExpression).clone(), nil
	}
	peCache.mu.Unlock()

	pathExpression, err := parseJSONPathExpr(pathExpr)
	if err == nil {
		peCache.mu.Lock()
		peCache.cache.Put(jsonPathExpressionKey(pathExpr), kvcache.Value(pathExpression))
		peCache.mu.Unlock()
	}
	return pathExpression, err
}

func (i jsonPathArrayIndex) String() string {
	indexStr := strconv.Itoa(i.index)
	if i.fromLast {
		return "last-" + indexStr
	}

	return indexStr
}

func (pe JSONPathExpression) String() string {
	var s strings.Builder

	s.WriteString("$")
	for _, leg := range pe.legs {
		switch leg.typ {
		case jsonPathLegArraySelection:
			switch selection := leg.arraySelection.(type) {
			case jsonPathArraySelectionAsterisk:
				s.WriteString("[*]")
			case jsonPathArraySelectionIndex:
				s.WriteString("[")
				s.WriteString(selection.index.String())
				s.WriteString("]")
			case jsonPathArraySelectionRange:
				s.WriteString("[")
				s.WriteString(selection.start.String())
				s.WriteString(" to ")
				s.WriteString(selection.end.String())
				s.WriteString("]")
			}
		case jsonPathLegKey:
			s.WriteString(".")
			if leg.dotKey == "*" {
				s.WriteString(leg.dotKey)
			} else {
				s.WriteString(quoteJSONString(leg.dotKey))
			}
		case jsonPathLegDoubleAsterisk:
			s.WriteString("**")
		}
	}
	return s.String()
}

func init() {
	peCache.cache = kvcache.NewSimpleLRUCache(1000, 0.1, math.MaxUint64)
}
