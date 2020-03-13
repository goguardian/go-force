package force

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// Interface all standard and custom objects must implement. Needed for uri generation.
type SObject interface {
	APIName() string
	ExternalIDAPIName() string
}

// Response received from force.com API after insert of an sobject.
type SObjectResponse struct {
	Id      string    `force:"id,omitempty"`
	Errors  APIErrors `force:"error,omitempty"` //TODO: Not sure if APIErrors is the right object
	Success bool      `force:"success,omitempty"`
}

func (forceAPI *ForceAPI) DescribeSObjects() (map[string]*SObjectMetaData, error) {
	if err := forceAPI.getSObjects(); err != nil {
		return nil, err
	}

	return forceAPI.apiSObjects, nil
}

func (forceAPI *ForceAPI) DescribeSObject(in SObject) (resp *SObjectDescription, err error) {
	// Check cache
	resp, ok := forceAPI.apiSObjectDescriptions[in.APIName()]
	if !ok {
		// Attempt retrieval from api
		sObjectMetaData, ok := forceAPI.apiSObjects[in.APIName()]
		if !ok {
			err = fmt.Errorf("Unable to find metadata for object: %v", in.APIName())
			return
		}

		uri := sObjectMetaData.URLs[sObjectDescribeKey]

		resp = &SObjectDescription{}
		err = forceAPI.Get(uri, nil, resp)
		if err != nil {
			return
		}

		// Create Comma Separated String of All Field Names.
		// Used for SELECT * Queries.
		length := len(resp.Fields)
		if length > 0 {
			var allFields bytes.Buffer
			for index, field := range resp.Fields {
				// Field type location cannot be directly retrieved from SQL Query.
				if field.Type != "location" {
					if index > 0 && index < length {
						allFields.WriteString(", ")
					}
					allFields.WriteString(field.Name)
				}
			}

			resp.AllFields = allFields.String()
		}

		forceAPI.apiSObjectDescriptions[in.APIName()] = resp
	}

	return
}

func (forceAPI *ForceAPI) GetSObject(id string, fields []string, out SObject) (err error) {
	uri := strings.Replace(forceAPI.apiSObjects[out.APIName()].URLs[rowTemplateKey], idKey, id, 1)

	params := url.Values{}
	if len(fields) > 0 {
		params.Add("fields", strings.Join(fields, ","))
	}

	err = forceAPI.Get(uri, params, out.(interface{}))

	return
}

func (forceAPI *ForceAPI) InsertSObject(in SObject) (resp *SObjectResponse, err error) {
	uri := forceAPI.apiSObjects[in.APIName()].URLs[sObjectKey]

	resp = &SObjectResponse{}
	err = forceAPI.Post(uri, nil, in.(interface{}), resp)

	return
}

func (forceAPI *ForceAPI) UpdateSObject(id string, in SObject) (err error) {
	uri := strings.Replace(forceAPI.apiSObjects[in.APIName()].URLs[rowTemplateKey], idKey, id, 1)

	err = forceAPI.Patch(uri, nil, in.(interface{}), nil)

	return
}

func (forceAPI *ForceAPI) DeleteSObject(id string, in SObject) (err error) {
	uri := strings.Replace(forceAPI.apiSObjects[in.APIName()].URLs[rowTemplateKey], idKey, id, 1)

	err = forceAPI.Delete(uri, nil)

	return
}

func (forceAPI *ForceAPI) GetSObjectByExternalId(id string, fields []string, out SObject) (err error) {
	uri := fmt.Sprintf("%v/%v/%v", forceAPI.apiSObjects[out.APIName()].URLs[sObjectKey],
		out.ExternalIDAPIName(), id)

	params := url.Values{}
	if len(fields) > 0 {
		params.Add("fields", strings.Join(fields, ","))
	}

	err = forceAPI.Get(uri, params, out.(interface{}))

	return
}

func (forceAPI *ForceAPI) UpsertSObjectByExternalId(id string, in SObject) (resp *SObjectResponse, err error) {
	uri := fmt.Sprintf("%v/%v/%v", forceAPI.apiSObjects[in.APIName()].URLs[sObjectKey],
		in.ExternalIDAPIName(), id)

	resp = &SObjectResponse{}
	err = forceAPI.Patch(uri, nil, in.(interface{}), resp)

	return
}

func (forceAPI *ForceAPI) DeleteSObjectByExternalId(id string, in SObject) (err error) {
	uri := fmt.Sprintf("%v/%v/%v", forceAPI.apiSObjects[in.APIName()].URLs[sObjectKey],
		in.ExternalIDAPIName(), id)

	err = forceAPI.Delete(uri, nil)

	return
}

const (
	soCreateBatchSize = 200
	soUpdateBatchSize = 200
	soDeleteBatchSize = 200
)

type SObjectRecordAttributes struct {
	Type        string `json:"type,omitempty"`
	ReferenceID string `json:"referenceId,omitempty"`
	URL         string `json:"url,omitempty"`
}

type sObjectInsertMultipleReq struct {
	Records []SObject `json:"records"`
}

type sObjectInsertMultipleResp struct {
	HasErrors bool `json:"hasErrors"`
	Results   []struct {
		ID          string `json:"id"`
		ReferenceID string `json:"referenceId"`
	} `json:"results"`
}

// InsertMultipleSObjects creates multiple unrelated records of the same type in batches of maximum "soCreateBatchSize"
// size.
// An incoming `SObject` should have "attributes" property with of type `SObjectRecordAttributes`: `Type` and
// `ReferenceID` should be filled out with corresponding SObject type (APIName) and some external ID.
// Note: supported by Salesforce API v45.0 (Spring 2019) and later.
//
// Example:
//
//    type CustomObject struct {
//      Name     string
//      Value    int
//      IsActive bool
//
//      Attributes SObjectRecordAttributes `json:"attributes"`
//    }
//
//    func (_ CustomObject) APIName() string           { return "CustomObject__c" }
//    func (_ CustomObject) ExternalIDAPIName() string { return "id" }
//
//    func foo() {
//      o1 := CustomObject{Name: "object 1", Value: 100, IsActive: false}
//      o1.Attributes = SObjectRecordAttributes{Type: o1.APIName(), ReferenceID: "1"}
//
//      o2 := CustomObject{Name: "object 2", Value: 200, IsActive: false}
//      o2.Attributes = SObjectRecordAttributes{Type: o2.APIName(), ReferenceID: "2"}
//
//      objects := []CustomObject{o1, o2}
//
//      // Convert types.
//      sObjects := make([]SObject, len(objects))
//      for i := range objects {
//        sObjects[i] = objects[i]
//      }
//      _ = forceAPI.InsertMultipleSObjects(sObjects)
//    }
//
// See https://developer.salesforce.com/docs/atlas.en-us.218.0.api_rest.meta/api_rest/dome_composite_sobject_tree_flat.htm
func (forceAPI *ForceAPI) InsertMultipleSObjects(in []SObject) (err error) {
	if len(in) == 0 {
		return nil
	}

	// Check sobject's types are the same.
	soType := in[0].APIName()
	for _, o := range in {
		if o.APIName() != soType {
			return errors.New("all objects should have the same type (APIName)")
		}
	}

	// Check if requested sobject type exists in SF.
	if _, ok := forceAPI.apiSObjects[soType]; !ok {
		return fmt.Errorf("SObject type not found: %s", soType)
	}

	uri := fmt.Sprintf("/services/data/%s/composite/tree/%s", forceAPI.apiVersion, soType)

	// Split all records to batches.
	limit := soCreateBatchSize
	for i := 0; i < len(in); i += limit {
		end := i + limit
		if end > len(in) {
			end = len(in)
		}

		records := in[i:end]

		req := sObjectInsertMultipleReq{Records: records}
		var resp sObjectInsertMultipleResp
		if err := forceAPI.Post(uri, nil, req, &resp); err != nil {
			return fmt.Errorf("forceAPI.Post: %s", err)
		}

		if resp.HasErrors {
			errRefIDs := []string{}
			for _, r := range resp.Results {
				errRefIDs = append(errRefIDs, r.ReferenceID)
			}
			return fmt.Errorf("error creating objects, refIDs: %s", strings.Join(errRefIDs, ", "))
		}
	}

	return nil
}

type sObjectUpdateMultipleReq struct {
	AllOrNone bool      `json:"allOrNone"`
	Records   []SObject `json:"records"`
}

type sObjectUpdateMultipleResp []struct {
	ID      string `json:"id"`
	Success bool   `json:"success"`
	Errors  []struct {
		StatusCode string `json:"statusCode"`
		Message    string `json:"message"`
	} `json:"errors"`
}

// UpdateMultipleSObjects update multiple records of the arbitrary type in batches of maximum "soUpdateBatchSize"
// size.
// An incoming `SObject` should have "id" property with a valid ID value, and "attributes" property with of type
// `SObjectRecordAttributes`: `Type` should be filled out with corresponding SObject type (APIName).
// Note: supported by Salesforce API v43.0 (Summer 2018) and later.
//
// Example:
//
//    type CustomObject struct {
//      ID 		 string `json:"id"`
//      Name     string
//      Value    int
//      IsActive bool
//
//      Attributes SObjectRecordAttributes `json:"attributes"`
//    }
//
//    func (_ CustomObject) APIName() string           { return "CustomObject__c" }
//    func (_ CustomObject) ExternalIDAPIName() string { return "id" }
//
//    func foo() {
//      o1 := CustomObject{ID: "id1", Name: "object 1", Value: 100, IsActive: false}
//      o1.Attributes = SObjectRecordAttributes{Type: o1.APIName()}
//
//      o2 := CustomObject{ID: "id2", Name: "object 2", Value: 200, IsActive: false}
//      o2.Attributes = SObjectRecordAttributes{Type: o2.APIName()}
//
//      objects := []CustomObject{o1, o2}
//
//      // Convert types.
//      sObjects := make([]SObject, len(objects))
//      for i := range objects {
//        sObjects[i] = objects[i]
//      }
//      _ = forceAPI.UpdateMultipleSObjects(sObjects, true)
//    }
//
// See https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_composite_sobjects_collections_update.htm
func (forceAPI *ForceAPI) UpdateMultipleSObjects(in []SObject, inTransaction bool) (err error) {
	if len(in) == 0 {
		return nil
	}

	// Check if requested sobject type exists in SF.
	for _, o := range in {
		soType := o.APIName()
		if _, ok := forceAPI.apiSObjects[soType]; !ok {
			return fmt.Errorf("SObject type not found: %s", soType)
		}
	}

	uri := fmt.Sprintf("/services/data/%s/composite/sobjects", forceAPI.apiVersion)

	// Split all records to batches.
	limit := soUpdateBatchSize
	for i := 0; i < len(in); i += limit {
		end := i + limit
		if end > len(in) {
			end = len(in)
		}

		records := in[i:end]

		req := sObjectUpdateMultipleReq{
			AllOrNone: inTransaction,
			Records:   records,
		}
		var resp sObjectUpdateMultipleResp
		if err := forceAPI.Patch(uri, nil, req, &resp); err != nil {
			return fmt.Errorf("forceAPI.Patch: %s", err)
		}

		// Check response, format errors.
		var errs []string
		for _, res := range resp {
			if !res.Success {
				codes := ""
				for _, e := range res.Errors {
					codes = fmt.Sprintf("%s, %s", codes, e.StatusCode)
				}

				errs = append(errs, fmt.Sprintf("%s: %s", res.ID, codes))
			}
		}

		if len(errs) > 0 {
			return fmt.Errorf("error updating objects: %s", strings.Join(errs, ", "))
		}
	}

	return nil
}

type sObjectDeleteMultipleResp []struct {
	ID      string `json:"id"`
	Success bool   `json:"success"`
	Errors  []struct {
		StatusCode string `json:"statusCode"`
		Message    string `json:"message"`
	} `json:"errors"`
}

// DeleteMultipleSObjects deletes multiple sObjects by IDs in batches of maximum "soDeleteBatchSize" size.
// `inTransaction` option controls if deletion of each batch should be performed in a single transaction.
// Note: supported by Salesforce API v43.0 (Summer 2018) and later.
// See https://developer.salesforce.com/docs/atlas.en-us.214.0.api_rest.meta/api_rest/resources_composite_sobjects_collections_delete.htm
func (forceAPI *ForceAPI) DeleteMultipleSObjects(ids []string, inTransaction bool) (err error) {
	if len(ids) == 0 {
		return nil
	}

	uri := fmt.Sprintf("/services/data/%s/composite/sobjects", forceAPI.apiVersion)

	params := url.Values{}
	if inTransaction {
		params.Set("allOrNone", "true")
	}

	// Split all ids to batches.
	limit := soDeleteBatchSize
	for i := 0; i < len(ids); i += limit {
		end := i + limit
		if end > len(ids) {
			end = len(ids)
		}

		batch := ids[i:end]
		params.Set("ids", strings.Join(batch, ","))

		var resp sObjectDeleteMultipleResp
		if err := forceAPI.DeleteWithResponse(uri, params, &resp); err != nil {
			return fmt.Errorf("forceAPI.Delete: %s", err)
		}

		// Check response, format errors.
		var errs []string
		for _, res := range resp {
			if !res.Success {
				codes := ""
				for _, e := range res.Errors {
					codes = fmt.Sprintf("%s, %s", codes, e.StatusCode)
				}

				errs = append(errs, fmt.Sprintf("%s: %s", res.ID, codes))
			}
		}

		if len(errs) > 0 {
			return fmt.Errorf("error deleting objects: %s", strings.Join(errs, ", "))
		}
	}

	return
}
