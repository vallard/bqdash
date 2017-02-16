// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// This App Engine application uses its default service account to list all
// the BigQuery datasets accessible via the BigQuery REST API.
package bq

import (
	"fmt"
	"log"
	"net/http"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	bigquery "google.golang.org/api/bigquery/v2"
	"google.golang.org/appengine"
)

type flight struct {
	departurestation  string
	arrivalstation    string
	number_of_flights int
}

func init() {
	http.HandleFunc("/", Handle)
}

func Handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	// Create a new App Engine context from the request.
	ctx := appengine.NewContext(r)

	flights, err := getData(ctx)
	if err != nil {
		fmt.Fprint(w, err)
	}
	for _, f := range flights {
		fmt.Fprintf(w, "%v", f)
	}
}

// get the data rows.
func getData(ctx context.Context) ([][]interface{}, error) {
	// Create a new authenticated HTTP client over urlfetch.
	hc, err := google.DefaultClient(ctx, bigquery.BigqueryScope)
	if err != nil {
		return nil, fmt.Errorf("could not create http client: %v", err)
	}
	// Create the BigQuery service.
	bq, err := bigquery.New(hc)
	if err != nil {
		return nil, fmt.Errorf("could not create service: %v", err)
	}
	legacy := false
	query := &bigquery.QueryRequest{
		UseLegacySql: &legacy,
		Query: `SELECT  departurestation, arrivalstation, count(*) AS number_of_flights 
		 FROM ` + "`practical-argon-158218.flight_data.navitar`" +
			` GROUP BY departurestation, arrivalstation
		 ORDER BY number_of_flights DESC
		 Limit 100`,
	}
	projectID := appengine.AppID(ctx)
	results, err := bq.Jobs.Query(projectID, query).Do()
	log.Print(results)
	if err != nil {
		return nil, err
	}

	_, rows := headersAndRows(results.Schema, results.Rows)
	/*err = bqschema.ToStructs(result, &flights)
	if err != nil {
		return nil, err
	}*/
	//return flights, nil
	return rows, nil

}

func headersAndRows(bqSchema *bigquery.TableSchema, bqRows []*bigquery.TableRow) ([]string, [][]interface{}) {
	if bqSchema == nil || bqRows == nil {
		return nil, nil
	}

	headers := make([]string, len(bqSchema.Fields))
	rows := make([][]interface{}, len(bqRows))
	// Create headers
	for i, f := range bqSchema.Fields {
		headers[i] = f.Name
	}
	// Create rows
	for i, tableRow := range bqRows {
		row := make([]interface{}, len(bqSchema.Fields))
		for j, tableCell := range tableRow.F {
			row[j] = tableCell.V
		}
		rows[i] = row
	}
	return headers, rows
}

// datasets returns a list with the IDs of all the Big Query datasets visible
// with the given context.
func datasets(ctx context.Context) ([]string, error) {
	// Create a new authenticated HTTP client over urlfetch.
	hc, err := google.DefaultClient(ctx, bigquery.BigqueryScope)
	if err != nil {
		return nil, fmt.Errorf("could not create http client: %v", err)
	}

	// Create the BigQuery service.
	bq, err := bigquery.New(hc)
	if err != nil {
		return nil, fmt.Errorf("could not create service: %v", err)
	}

	// Get the current application ID, which is the same as the project ID.
	projectID := appengine.AppID(ctx)

	// Return a list of IDs.
	var ids []string
	datasets, err := bq.Datasets.List(projectID).Do()
	if err != nil {
		return nil, fmt.Errorf("could not list datasets for %q: %v", projectID, err)
	}
	for _, d := range datasets.Datasets {
		ids = append(ids, d.Id)
	}
	return ids, nil
}
