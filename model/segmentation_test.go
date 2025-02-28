package model_test

import (
	"context"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"

	"github.com/pshvedko/sap_segmentation/model"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestSegmentation_Put(t *testing.T) {
	name := os.Getenv("TEST_SAP_SEGMENTATION_DB")
	if name == "" {
		t.SkipNow()
	}
	db, err := sqlx.Open("pgx", name)
	if err != nil {
		t.FailNow()
	}
	defer func() { _ = db.Close() }()
	ctx := context.TODO()
	type fields struct {
		AddressSapId string
		AdrSegment   string
		SegmentId    int64
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "",
			fields: fields{
				AddressSapId: "UNIQ",
				AdrSegment:   "1",
				SegmentId:    1,
			},
			wantErr: false,
		},
		{
			name: "",
			fields: fields{
				AddressSapId: "UNIQ",
				AdrSegment:   "2",
				SegmentId:    2,
			},
			wantErr: false,
		},
		{
			name: "",
			fields: fields{
				AddressSapId: "UNIQ",
				AdrSegment:   "3",
				SegmentId:    3,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := model.Segmentation{
				AddressSapId: tt.fields.AddressSapId,
				AdrSegment:   tt.fields.AdrSegment,
				SegmentId:    tt.fields.SegmentId,
			}
			if err := s.Put(ctx, db); (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
