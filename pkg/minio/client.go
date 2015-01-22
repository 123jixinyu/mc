/*
 * Mini Object Storage, (C) 2014,2015 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package minio implements a generic Minio client
package minio

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	MAX_OBJECT_LIST = 1000
)

// Client is an Minio MINIO client.
type Client struct {
	Hostname  string
	Transport http.RoundTripper // or nil for the default
}

type Bucket struct {
	Name         string
	CreationDate string // 2006-02-03T16:45:09.000Z
}

func (c *Client) transport() http.RoundTripper {
	if c.Transport != nil {
		return c.Transport
	}
	return http.DefaultTransport
}

func (c *Client) hostname() string {
	if c.Hostname != "" {
		return c.Hostname
	}
	return "localhost"
}

// bucketURL returns the URL prefix of the bucket, with trailing slash
func (c *Client) bucketURL(bucket string) string {
	return fmt.Sprintf("http://%s/%s/", c.hostname(), bucket)
}

func (c *Client) keyURL(bucket, key string) string {
	return c.bucketURL(bucket) + key
}

func newReq(url_ string) *http.Request {
	req, err := http.NewRequest("GET", url_, nil)
	if err != nil {
		panic(fmt.Sprintf("minio client; invalid URL: %v", err))
	}
	req.Header.Set("User-Agent", "minio")
	return req
}

func (c *Client) Buckets() ([]*Bucket, error) {
	req := newReq("http://" + c.hostname() + "/")
	res, err := c.transport().RoundTrip(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("minio: Unexpected status code %d fetching bucket list", res.StatusCode)
	}
	return parseListAllMyBuckets(res.Body)
}

func parseListAllMyBuckets(r io.Reader) ([]*Bucket, error) {
	type allMyBuckets struct {
		Buckets struct {
			Bucket []*Bucket
		}
	}
	var res allMyBuckets
	if err := xml.NewDecoder(r).Decode(&res); err != nil {
		return nil, err
	}
	return res.Buckets.Bucket, nil
}

// Returns 0, os.ErrNotExist if not on MINIO, otherwise reterr is real.
func (c *Client) Stat(key, bucket string) (size int64, reterr error) {
	req := newReq(c.keyURL(bucket, key))
	req.Method = "HEAD"
	res, err := c.transport().RoundTrip(req)
	if err != nil {
		return 0, err
	}
	if res.Body != nil {
		defer res.Body.Close()
	}
	switch res.StatusCode {
	case http.StatusNotFound:
		return 0, os.ErrNotExist
	case http.StatusOK:
		return strconv.ParseInt(res.Header.Get("Content-Length"), 10, 64)
	}
	return 0, fmt.Errorf("minio: Unexpected status code %d statting object %v", res.StatusCode, key)
}

func (c *Client) PutBucket(bucket string) error {
	req := newReq(c.bucketURL(bucket))
	req.Method = "PUT"
	res, err := c.transport().RoundTrip(req)
	if res != nil && res.Body != nil {
		defer res.Body.Close()
	}

	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		// res.Write(os.Stderr)
		return fmt.Errorf("Got response code %d from minio", res.StatusCode)
	}
	return nil

}

func (c *Client) Put(bucket, key string, md5 hash.Hash, size int64, body io.Reader) error {
	req := newReq(c.keyURL(bucket, key))
	req.Method = "PUT"
	req.ContentLength = size
	if md5 != nil {
		b64 := new(bytes.Buffer)
		encoder := base64.NewEncoder(base64.StdEncoding, b64)
		encoder.Write(md5.Sum(nil))
		encoder.Close()
		req.Header.Set("Content-MD5", b64.String())
	}
	req.Body = ioutil.NopCloser(body)

	res, err := c.transport().RoundTrip(req)
	if res != nil && res.Body != nil {
		defer res.Body.Close()
	}

	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		// res.Write(os.Stderr)
		return fmt.Errorf("Got response code %d from minio", res.StatusCode)
	}
	return nil
}

type Item struct {
	Key          string
	LastModified string
	Size         int64
}

type listBucketResults struct {
	Contents    []*Item
	IsTruncated bool
	MaxKeys     int
	Name        string // bucket name
	Marker      string
}

// BucketLocation returns the Minio hostname to be used with the given bucket.
func (c *Client) BucketLocation(bucket string, hostname string) (location string, err error) {
	url_ := fmt.Sprintf("http://%s/%s/?location", hostname, url.QueryEscape(bucket))
	req := newReq(url_)
	res, err := c.transport().RoundTrip(req)
	if err != nil {
		return
	}
	var xres xmlLocationConstraint
	if err := xml.NewDecoder(res.Body).Decode(&xres); err != nil {
		return "", err
	}
	if xres.Location == "" {
		return "localhost", nil
	}
	return "minio-" + xres.Location + "." + hostname, nil
}

// GetBucket (List Objects) returns 0 to maxKeys (inclusive) items from the
// provided bucket. Keys before startAt will be skipped. (This is the MINIO
// 'marker' value). If the length of the returned items is equal to
// maxKeys, there is no indication whether or not the returned list is truncated.
func (c *Client) GetBucket(bucket string, startAt string, maxKeys int) (items []*Item, err error) {
	if maxKeys < 0 {
		return nil, errors.New("invalid negative maxKeys")
	}
	marker := startAt
	for len(items) < maxKeys {
		fetchN := maxKeys - len(items)
		if fetchN > MAX_OBJECT_LIST {
			fetchN = MAX_OBJECT_LIST
		}
		var bres listBucketResults

		url_ := fmt.Sprintf("%s?marker=%s&max-keys=%d",
			c.bucketURL(bucket), url.QueryEscape(marker), fetchN)

		// Try the enumerate three times, since s3 likes to close
		// https connections a lot, and Go sucks at dealing with it:
		// https://code.google.com/p/go/issues/detail?id=3514
		const maxTries = 5
		for try := 1; try <= maxTries; try++ {
			time.Sleep(time.Duration(try-1) * 100 * time.Millisecond)
			req := newReq(url_)
			res, err := c.transport().RoundTrip(req)
			if err != nil {
				if try < maxTries {
					continue
				}
				return nil, err
			}
			if res.StatusCode != http.StatusOK {
				if res.StatusCode < 500 {
					body, _ := ioutil.ReadAll(io.LimitReader(res.Body, 1<<20))
					aerr := &Error{
						Op:     "ListBucket",
						Code:   res.StatusCode,
						Body:   body,
						Header: res.Header,
					}
					aerr.parseXML()
					res.Body.Close()
					return nil, aerr
				}
			} else {
				bres = listBucketResults{}
				var logbuf bytes.Buffer
				err = xml.NewDecoder(io.TeeReader(res.Body, &logbuf)).Decode(&bres)
				if err != nil {
					log.Printf("Error parsing minio XML response: %v for %q", err, logbuf.Bytes())
				} else if bres.MaxKeys != fetchN || bres.Name != bucket || bres.Marker != marker {
					err = fmt.Errorf("Unexpected parse from server: %#v from: %s", bres, logbuf.Bytes())
					log.Print(err)
				}
			}
			res.Body.Close()
			if err != nil {
				if try < maxTries-1 {
					continue
				}
				log.Print(err)
				return nil, err
			}
			break
		}
		for _, it := range bres.Contents {
			if it.Key == marker && it.Key != startAt {
				// Skip first dup on pages 2 and higher.
				continue
			}
			if it.Key < startAt {
				return nil, fmt.Errorf("Unexpected response from Minio: item key %q but wanted greater than %q", it.Key, startAt)
			}
			items = append(items, it)
			marker = it.Key
		}
		if !bres.IsTruncated {
			// log.Printf("Not truncated. so breaking. items = %d; len Contents = %d, url = %s", len(items), len(bres.Contents), url_)
			break
		}
	}
	return items, nil
}

func (c *Client) Get(bucket, key string) (body io.ReadCloser, size int64, err error) {
	req := newReq(c.keyURL(bucket, key))
	res, err := c.transport().RoundTrip(req)
	if err != nil {
		return
	}
	switch res.StatusCode {
	case http.StatusOK:
		return res.Body, res.ContentLength, nil
	case http.StatusNotFound:
		res.Body.Close()
		return nil, 0, os.ErrNotExist
	default:
		res.Body.Close()
		return nil, 0, fmt.Errorf("Minio HTTP error on GET: %d", res.StatusCode)
	}
}

// GetPartial fetches part of the minio key object in bucket.
// If length is negative, the rest of the object is returned.
// The caller must close rc.
func (c *Client) GetPartial(bucket, key string, offset, length int64) (rc io.ReadCloser, err error) {
	if offset < 0 {
		return nil, errors.New("invalid negative length")
	}

	req := newReq(c.keyURL(bucket, key))
	if length >= 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	} else {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}

	res, err := c.transport().RoundTrip(req)
	if err != nil {
		return
	}
	switch res.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		return res.Body, nil
	case http.StatusNotFound:
		res.Body.Close()
		return nil, os.ErrNotExist
	default:
		res.Body.Close()
		return nil, fmt.Errorf("Minio HTTP error on GET: %d", res.StatusCode)
	}
}

func NewMinioClient(hostname string) (client *Client) {
	client = &Client{hostname, http.DefaultTransport}
	return
}

// Error is the type returned by some API operations.
type Error struct {
	Op     string
	Code   int         // HTTP status code
	Body   []byte      // response body
	Header http.Header // response headers

	// UsedEndpoint and MinioCode are the XML response's Endpoint and
	// Code fields, respectively.
	UseEndpoint string // if a temporary redirect (wrong hostname)
	MinioCode   string
}

func (e *Error) Error() string {
	if bytes.Contains(e.Body, []byte("<Error>")) {
		return fmt.Sprintf("minio.%s: status %d: %s", e.Op, e.Code, e.Body)
	}
	return fmt.Sprintf("minio.%s: status %d", e.Op, e.Code)
}

func (e *Error) parseXML() {
	var xe xmlError
	_ = xml.NewDecoder(bytes.NewReader(e.Body)).Decode(&xe)
	e.MinioCode = xe.Code
	if xe.Code == "TemporaryRedirect" {
		e.UseEndpoint = xe.Endpoint
	}
	if xe.Code == "SignatureDoesNotMatch" {
		want, _ := hex.DecodeString(strings.Replace(xe.StringToSignBytes, " ", "", -1))
		log.Printf("MINIO SignatureDoesNotMatch. StringToSign should be %d bytes: %q (%x)", len(want), want, want)
	}

}

// xmlError is the Error response from Minio.
type xmlError struct {
	XMLName           xml.Name `xml:"Error"`
	Code              string
	Message           string
	RequestId         string
	Bucket            string
	Endpoint          string
	StringToSignBytes string
}

// xmlLocationConstraint is the LocationConstraint returned from BucketLocation.
type xmlLocationConstraint struct {
	XMLName  xml.Name `xml:"LocationConstraint"`
	Location string   `xml:",chardata"`
}
