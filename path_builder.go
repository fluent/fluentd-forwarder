// 
// Copyright (C) 2014 Moriyoshi Koizumi
// Copyright (C) 2014 Treasure Data, Inc.
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//

package fluentd_forwarder

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type JournalFileType rune

const (
	Head = JournalFileType('b')
	Rest = JournalFileType('q')
)

type JournalPathInfo struct {
	Key             string
	Type            JournalFileType
	VariablePortion string
	TSuffix         string
	Timestamp       int64 // elapsed time in msec since epoch
	UniqueId        []byte
}

var NilJournalPathInfo = JournalPathInfo{"", 0, "", "", 0, nil}

var pathRegexp, _ = regexp.Compile(fmt.Sprintf("^(.*)[._](%c|%c)([0-9a-fA-F]{1,32})$", Head, Rest))

func encodeKey(key string) string {
	keyLen := len(key)
	retval := make([]byte, keyLen*2)
	i := 0
	for j := 0; j < keyLen; j += 1 {
		c := key[j]
		if c == '-' || c == '_' || c == '.' || (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') {
			cap_ := cap(retval)
			if i >= cap_ {
				newRetval := make([]byte, cap_+len(key))
				copy(newRetval, retval)
				retval = newRetval
			}
			retval[i] = c
			i += 1
		} else {
			cap_ := cap(retval)
			if i+3 > cap_ {
				newSize := cap_
				for {
					newSize += len(key)
					if i+3 <= newSize {
						break
					}
					if newSize < cap_ {
						// unlikely
						panic("?")
					}
				}
				newRetval := make([]byte, newSize)
				copy(newRetval, retval)
				retval = newRetval
			}
			retval[i] = '%'
			retval[i+1] = "0123456789abcdef"[(c>>4)&15]
			retval[i+2] = "0123456789abcdef"[c&15]
			i += 3
		}
	}
	return string(retval[0:i])
}

func decodeKey(encoded string) (string, error) {
	return url.QueryUnescape(encoded)
}

func convertTSuffixToUniqueId(tSuffix string) ([]byte, error) {
	tSuffixLen := len(tSuffix)
	buf := make([]byte, tSuffixLen)
	for i := 0; i < tSuffixLen; i += 2 {
		ii, err := strconv.ParseUint(tSuffix[i:i+2], 16, 8)
		if err != nil {
			return nil, err
		}
		buf[i/2] = byte(ii)
		buf[(i+tSuffixLen)/2] = byte(ii)
	}
	return buf, nil
}

func convertTSuffixToUnixNano(tSuffix string) (int64, error) {
	t, err := strconv.ParseInt(tSuffix, 16, 64)
	return t >> 12, err
}

func IsValidJournalPathInfo(info JournalPathInfo) bool {
	return len(info.Key) > 0 && info.Type != 0
}

func BuildJournalPathWithTSuffix(key string, bq JournalFileType, tSuffix string) string {
	encodedKey := encodeKey(key)
	return fmt.Sprintf(
		"%s.%c%s",
		encodedKey,
		rune(bq),
		tSuffix,
	)
}

func BuildJournalPath(key string, bq JournalFileType, time_ time.Time, randValue int64) JournalPathInfo {
	timestamp := time_.UnixNano()
	t := (timestamp/1000)<<12 | (randValue & 0xfff)
	tSuffix := strconv.FormatInt(t, 16)
	if pad := 16 - len(tSuffix); pad > 0 {
		// unlikely
		tSuffix = strings.Repeat("0", pad) + tSuffix
	}
	uniqueId, err := convertTSuffixToUniqueId(tSuffix)
	if err != nil {
		panic("WTF? " + err.Error())
	} // should never happen
	return JournalPathInfo{
		Key:             key,
		Type:            bq,
		VariablePortion: BuildJournalPathWithTSuffix(key, bq, tSuffix),
		TSuffix:         tSuffix,
		Timestamp:       timestamp,
		UniqueId:        uniqueId,
	}
}

func firstRune(s string) rune {
	for _, r := range s {
		return r
	}
	return -1 // in case the string is empty
}

func DecodeJournalPath(variablePortion string) (JournalPathInfo, error) {
	m := pathRegexp.FindStringSubmatch(variablePortion)
	if m == nil {
		return NilJournalPathInfo, errors.New("malformed path string")
	}
	key, err := decodeKey(m[1])
	if err != nil {
		return NilJournalPathInfo, errors.New("malformed path string")
	}
	uniqueId, err := convertTSuffixToUniqueId(m[3])
	if err != nil {
		return NilJournalPathInfo, errors.New("malformed path string")
	}
	timestamp, err := convertTSuffixToUnixNano(m[3])
	if err != nil {
		return NilJournalPathInfo, errors.New("malformed path string")
	}
	return JournalPathInfo{
		Key:             key,
		Type:            JournalFileType(firstRune(m[2])),
		VariablePortion: variablePortion,
		TSuffix:         m[3],
		Timestamp:       timestamp,
		UniqueId:        uniqueId,
	}, nil
}
