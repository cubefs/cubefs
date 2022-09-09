// Copyright 2019 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package objectnode

import (
	"encoding/xml"
	"github.com/cubefs/cubefs/util/log"
	"net/url"
)

func MarshalXMLEntity(entity interface{}) ([]byte, error) {
	var err error
	var result []byte
	if result, err = xml.Marshal(entity); err != nil {
		return nil, err
	}
	return append([]byte(xml.Header), result...), nil
}

func UnmarshalXMLEntity(bytes []byte, data interface{}) error {
	err := xml.Unmarshal(bytes, data)
	if err != nil {
		return err
	}
	return nil
}

type CopyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}

type Deleted struct {
	XMLName               xml.Name `xml:"Deleted"`
	Key                   string   `xml:"Key"`
	VersionId             string   `xml:"VersionId,omitempty"`
	DeleteMarker          string   `xml:"DeleteMarker,omitempty"`
	DeleteMarkerVersionId string   `xml:"DeleteMarkerVersionId,omitempty"`
}

type Error struct {
	XMLName   xml.Name `xml:"Error"`
	Key       string   `xml:"Key"`
	VersionId string   `xml:"VersionId,omitempty"`
	Code      string   `xml:"Code,omitempty"`
	Message   string   `xml:"Message,omitempty"`
}

type DeleteResult struct {
	XMLName xml.Name  `json:"DeleteResult"`
	Deleted []Deleted `xml:"Deleted,omitempty"`
	Error   []Error   `xml:"Error,omitempty"`
}

type InitMultipartResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

type CompleteMultipartResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type BucketOwner struct {
	XMLName     xml.Name `xml:"Owner"`
	ID          string   `xml:"ID"`
	DisplayName string   `xml:"DisplayName"`
}

type Upload struct {
	XMLName      xml.Name     `xml:"Upload"`
	Key          string       `xml:"Key"`
	UploadId     string       `xml:"UploadId"`
	StorageClass string       `xml:"StorageClass"`
	Initiated    string       `xml:"Initiated"`
	Owner        *BucketOwner `xml:"Owner"`
}

type Part struct {
	XMLName      xml.Name `xml:"Part"`
	PartNumber   int      `xml:"PartNumber"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
	Size         int      `xml:"Size"`
}

type ListPartsResult struct {
	XMLName          xml.Name     `xml:"ListPartsResult"`
	Bucket           string       `xml:"Bucket"`
	Key              string       `xml:"Key"`
	UploadId         string       `xml:"UploadId"`
	Owner            *BucketOwner `xml:"Owner"`
	StorageClass     string       `xml:"StorageClass"`
	PartNumberMarker int          `xml:"PartNumberMarker"`
	NextMarker       int          `xml:"NextPartNumberMarker"`
	MaxParts         int          `xml:"MaxParts"`
	IsTruncated      bool         `xml:"IsTruncated"`
	Parts            []*Part      `xml:"Parts"`
}

type CommonPrefix struct {
	XMLName xml.Name `xml:"CommonPrefixes"`
	Prefix  string
}

type ListUploadsResult struct {
	XMLName            xml.Name        `xml:"ListMultipartUploadsResult"`
	Bucket             string          `xml:"Bucket"`
	KeyMarker          string          `xml:"KeyMarker"`
	UploadIdMarker     string          `xml:"UploadIdMarker"`
	NextKeyMarker      string          `xml:"NextKeyMarker"`
	NextUploadIdMarker string          `xml:"NextUploadIdMarker"`
	Delimiter          string          `xml:"Delimiter"`
	Prefix             string          `xml:"Prefix"`
	MaxUploads         int             `xml:"MaxUploads"`
	IsTruncated        bool            `xml:"IsTruncated"`
	Uploads            []*Upload       `xml:"Uploads"`
	CommonPrefixes     []*CommonPrefix `xml:"CommonPrefixes"`
}

type Content struct {
	Key          string       `xml:"Key"`
	LastModified string       `xml:"LastModified"`
	ETag         string       `xml:"ETag"`
	Size         int          `xml:"Size"`
	StorageClass string       `xml:"StorageClass"`
	Owner        *BucketOwner `xml:"Owner,omitempty"`
}

type ListBucketResult struct {
	XMLName        xml.Name        `xml:"ListBucketResult"`
	Bucket         string          `xml:"Name"`
	Prefix         string          `xml:"Prefix"`
	Marker         string          `xml:"Marker"`
	MaxKeys        int             `xml:"MaxKeys"`
	Delimiter      string          `xml:"Delimiter"`
	IsTruncated    bool            `xml:"IsTruncated"`
	NextMarker     string          `xml:"NextMarker,omitempty"`
	Contents       []*Content      `xml:"Contents"`
	CommonPrefixes []*CommonPrefix `xml:"CommonPrefixes"`
}

func NewParts(fsParts []*FSPart) []*Part {
	parts := make([]*Part, 0)
	for _, fsPart := range fsParts {
		part := &Part{
			PartNumber:   fsPart.PartNumber,
			LastModified: fsPart.LastModified,
			ETag:         fsPart.ETag,
			Size:         fsPart.Size,
		}
		parts = append(parts, part)
	}
	return parts
}

func NewUploads(fsUploads []*FSUpload, accessKey string) []*Upload {
	owner := &BucketOwner{
		ID:          accessKey,
		DisplayName: accessKey,
	}
	uploads := make([]*Upload, 0)
	for _, fsUpload := range fsUploads {
		upload := &Upload{
			Key:          fsUpload.Key,
			UploadId:     fsUpload.UploadId,
			StorageClass: StorageClassStandard,
			Initiated:    fsUpload.Initiated,
			Owner:        owner,
		}
		uploads = append(uploads, upload)
	}
	return uploads
}

func NewBucketOwner(volume *Volume) *BucketOwner {
	return &BucketOwner{
		ID:          volume.Owner(),
		DisplayName: volume.Owner(),
	}
}

type Object struct {
	Key       string `xml:"Key"`
	VersionId string `xml:"VersionId,omitempty"`
}

type DeleteRequest struct {
	XMLName xml.Name `xml:"Delete"`
	Objects []Object `xml:"Object"`
}

type CopyResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	LastModified string   `xml:"LastModified,omitempty"`
	ETag         string   `xml:"ETag,omitempty"`
}

type ListBucketResultV2 struct {
	XMLName        xml.Name        `xml:"ListBucketResult"`
	Name           string          `xml:"Name"`
	Prefix         string          `xml:"Prefix,omitempty"`
	Token          string          `xml:"ContinuationToken,omitempty"`
	NextToken      string          `xml:"NextContinuationToken,omitempty"`
	KeyCount       uint64          `xml:"KeyCount"`
	MaxKeys        uint64          `xml:"MaxKeys"`
	Delimiter      string          `xml:"Delimiter,omitempty"`
	IsTruncated    bool            `xml:"IsTruncated,omitempty"`
	Contents       []*Content      `xml:"Contents"`
	CommonPrefixes []*CommonPrefix `xml:"CommonPrefixes"`
}

type Tag struct {
	Key   string `xml:"Key" json:"k"`
	Value string `xml:"Value" json:"v"`
}

type Tagging struct {
	XMLName xml.Name `json:"-"`
	TagSet  []Tag    `xml:"TagSet>Tag,omitempty" json:"ts"`
}

func (t Tagging) Encode() string {
	values := url.Values{}
	for _, tag := range t.TagSet {
		values[tag.Key] = []string{tag.Value}
	}
	return values.Encode()
}

func (t Tagging) Validate() (bool, *ErrorCode) {
	var errorCode *ErrorCode
	if len(t.TagSet) > TaggingCounts {
		return false, TagsGreaterThen10
	}
	for _, tag := range t.TagSet {
		log.LogDebugf("Validate: key : (%v), value : (%v)", tag.Key, tag.Value)
		if len(tag.Key) > TaggingKeyMaxLength {
			return false, InvalidTagKey
		}
		if len(tag.Value) > TaggingValueMaxLength {
			return false, InvalidTagValue
		}
	}
	return true, errorCode
}

func NewTagging() *Tagging {
	return &Tagging{
		XMLName: xml.Name{Local: "Tagging"},
	}
}

func ParseTagging(src string) (*Tagging, error) {
	values, err := url.ParseQuery(src)
	if err != nil {
		return nil, err
	}
	tagSet := make([]Tag, 0, len(values))
	for key, value := range values {
		tagSet = append(tagSet, Tag{Key: key, Value: value[0]})
	}
	if len(tagSet) == 0 {
		return NewTagging(), nil
	}
	return &Tagging{
		XMLName: xml.Name{Local: "Tagging"},
		TagSet:  tagSet,
	}, nil
}

type XAttr struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type PutXAttrRequest struct {
	XMLName xml.Name `xml:"PutXAttrRequest"`
	XAttr   *XAttr   `xml:"XAttr"`
}

type GetXAttrOutput struct {
	XMLName xml.Name `xml:"GetXAttrOutput"`
	XAttr   *XAttr   `xml:"XAttr"`
}

type ListXAttrsOutput struct {
	XMLName xml.Name `xml:"ListXAttrsResult"`
	Keys    []string `xml:"Keys"`
}

type PartRequest struct {
	XMLName    xml.Name `xml:"Part"`
	PartNumber int      `xml:"PartNumber"`
	ETag       string   `xml:"ETag"`
}

type CompleteMultipartUploadRequest struct {
	XMLName xml.Name       `xml:"CompleteMultipartUpload"`
	Parts   []*PartRequest `xml:"Part"`
}
