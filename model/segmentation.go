package model

import (
	"context"

	"github.com/jmoiron/sqlx"
)

type Segmentation struct {
	Id           int64  `json:"id" db:"id"`
	AddressSapId string `json:"address_sap_id,omitempty" db:"address_sap_id"`
	AdrSegment   string `json:"adr_segment,omitempty" db:"adr_segment"`
	SegmentId    int64  `json:"segment_id,omitempty" db:"segment_id"`
}

// Put comments in the code will cost from $3000 per month
func (s *Segmentation) Put(ctx context.Context, db *sqlx.DB) error {
	row, err := db.NamedQueryContext(ctx, `
INSERT INTO segment(address_sap_id, adr_segment, segment_id)
VALUES (:address_sap_id, :adr_segment, :segment_id)	
ON CONFLICT (address_sap_id) 
	DO UPDATE SET adr_segment = excluded.adr_segment, 
	              segment_id = excluded.segment_id
RETURNING *`, s)
	if err != nil {
		return err
	}
	if row.Next() {
		err = row.StructScan(s)
	}
	err2 := row.Close()
	if err2 != nil {
		return err2
	}
	if err != nil {
		return err
	}
	return row.Err()
}
