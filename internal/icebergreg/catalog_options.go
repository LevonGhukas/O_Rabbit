package icebergreg

import (
	"context"
	"fmt"
	"strconv"
	"time"

	iceberg "github.com/apache/iceberg-go"
	restcatalog "github.com/apache/iceberg-go/catalog/rest"
	icetable "github.com/apache/iceberg-go/table"
)

const (
	distributionModeProperty = "write.distribution-mode"
	writeUpdateModeProperty  = "write.update.mode"
	writeMergeModeProperty   = "write.merge.mode"
)

func buildPartitionSpec(schema *iceberg.Schema, fields []PartitionFieldConfig) (iceberg.PartitionSpec, error) {
	if len(fields) == 0 {
		return iceberg.NewPartitionSpec(), nil
	}

	opts := make([]iceberg.PartitionOption, 0, len(fields))
	for _, field := range fields {
		transform, err := iceberg.ParseTransform(field.Transform)
		if err != nil {
			return iceberg.PartitionSpec{}, fmt.Errorf("partition_spec %q: %w", field.Source, err)
		}
		name := field.Name
		if name == "" && field.Transform == "identity" {
			name = field.Source
		}
		opts = append(opts, iceberg.AddPartitionFieldByName(field.Source, name, transform, schema, nil))
	}
	return iceberg.NewPartitionSpecOpts(opts...)
}

func buildSortOrder(schema *iceberg.Schema, fields []SortFieldConfig, orderID int) (icetable.SortOrder, error) {
	if len(fields) == 0 {
		return icetable.UnsortedSortOrder, nil
	}

	sortFields := make([]icetable.SortField, 0, len(fields))
	for _, field := range fields {
		source, ok := schema.FindFieldByName(field.Source)
		if !ok {
			return icetable.SortOrder{}, fmt.Errorf("sort_order source column %q does not exist", field.Source)
		}
		transform, err := iceberg.ParseTransform(field.Transform)
		if err != nil {
			return icetable.SortOrder{}, fmt.Errorf("sort_order %q: %w", field.Source, err)
		}
		sortFields = append(sortFields, icetable.SortField{
			SourceID:  source.ID,
			Transform: transform,
			Direction: icetable.SortDirection(field.Direction),
			NullOrder: icetable.NullOrder(field.NullOrder),
		})
	}
	order, err := icetable.NewSortOrder(orderID, sortFields)
	if err != nil {
		return icetable.SortOrder{}, err
	}
	if err := order.CheckCompatibility(schema); err != nil {
		return icetable.SortOrder{}, err
	}
	return order, nil
}

func tableOptionProperties(cfg RunConfig) iceberg.Properties {
	props := iceberg.Properties{}
	if cfg.TargetFileSize > 0 {
		props[icetable.WriteTargetFileSizeBytesKey] = strconv.FormatInt(cfg.TargetFileSize, 10)
	}
	if cfg.DistributionMode != "" {
		props[distributionModeProperty] = cfg.DistributionMode
	}
	if cfg.MetricsMode != "" {
		props[icetable.DefaultWriteMetricsModeKey] = cfg.MetricsMode
	}
	retention := cfg.MetadataRetention
	if retention.DeleteAfterCommit {
		props[icetable.MetadataDeleteAfterCommitEnabledKey] = "true"
	}
	if retention.PreviousVersionsMax > 0 {
		props[icetable.MetadataPreviousVersionsMaxKey] = strconv.Itoa(retention.PreviousVersionsMax)
	}
	if retention.MinSnapshotsToKeep > 0 {
		props[icetable.MinSnapshotsToKeepKey] = strconv.Itoa(retention.MinSnapshotsToKeep)
	}
	if retention.MaxSnapshotAgeMS > 0 {
		props[icetable.MaxSnapshotAgeMsKey] = strconv.FormatInt(retention.MaxSnapshotAgeMS, 10)
	}
	if cfg.Upsert.Enabled {
		props[icetable.WriteDeleteModeKey] = cfg.Upsert.Mode
		props[writeUpdateModeProperty] = cfg.Upsert.Mode
		props[writeMergeModeProperty] = cfg.Upsert.Mode
	}
	return props
}

func schemaWithIdentifierFields(schema *iceberg.Schema, keys []string) (*iceberg.Schema, error) {
	if len(keys) == 0 {
		return schema, nil
	}
	ids := make([]int, 0, len(keys))
	for _, key := range keys {
		field, ok := schema.FindFieldByName(key)
		if !ok {
			return nil, fmt.Errorf("upsert key column %q does not exist", key)
		}
		if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
			return nil, fmt.Errorf("upsert key column %q must be primitive", key)
		}
		if !field.Required {
			return nil, fmt.Errorf("upsert key column %q must be required in the Iceberg schema", key)
		}
		ids = append(ids, field.ID)
	}
	return iceberg.NewSchemaWithIdentifiers(schema.ID, ids, schema.Fields()...), nil
}

func applySchemaOptions(tx *icetable.Transaction, current, source *iceberg.Schema, cfg RunConfig) error {
	if cfg.Upsert.Enabled {
		if _, err := schemaWithIdentifierFields(current, cfg.Upsert.Keys); err != nil {
			return err
		}
	}
	update := tx.UpdateSchema(true, false)
	changed := false

	if source != nil {
		for _, sourceField := range source.Fields() {
			currentField, ok := current.FindFieldByName(sourceField.Name)
			if !ok {
				if cfg.SchemaEvolution != "additive" {
					return fmt.Errorf("schema_evolution=strict rejects new column %q", sourceField.Name)
				}
				update.AddColumn([]string{sourceField.Name}, sourceField.Type, sourceField.Doc, false, nil)
				changed = true
				continue
			}
			if currentField.Type.Equals(sourceField.Type) {
				continue
			}
			if _, err := iceberg.PromoteType(sourceField.Type, currentField.Type); err == nil {
				continue
			}
			if cfg.SchemaEvolution != "additive" {
				return fmt.Errorf("schema_evolution=strict rejects type change for %q from %s to %s", sourceField.Name, currentField.Type, sourceField.Type)
			}
			if _, err := iceberg.PromoteType(currentField.Type, sourceField.Type); err != nil {
				return fmt.Errorf("schema_evolution cannot promote %s from %s to %s: %w", sourceField.Name, currentField.Type, sourceField.Type, err)
			}
			update.UpdateColumn([]string{sourceField.Name}, icetable.ColumnUpdate{
				FieldType: iceberg.Optional[iceberg.Type]{Val: sourceField.Type, Valid: true},
			})
			changed = true
		}
	}

	if cfg.Upsert.Enabled {
		paths := make([][]string, 0, len(cfg.Upsert.Keys))
		for _, key := range cfg.Upsert.Keys {
			paths = append(paths, []string{key})
		}
		update.SetIdentifierField(paths)
		changed = true
	}
	if !changed {
		return nil
	}
	return update.Commit()
}

func applyMetadataRetention(tx *icetable.Transaction, cfg MetadataRetentionConfig) error {
	if cfg.MinSnapshotsToKeep == 0 && cfg.MaxSnapshotAgeMS == 0 {
		return nil
	}
	opts := make([]icetable.ExpireSnapshotsOpt, 0, 3)
	if cfg.MinSnapshotsToKeep > 0 {
		opts = append(opts, icetable.WithRetainLast(cfg.MinSnapshotsToKeep))
	}
	if cfg.MaxSnapshotAgeMS > 0 {
		opts = append(opts, icetable.WithOlderThan(time.Duration(cfg.MaxSnapshotAgeMS)*time.Millisecond))
	}
	opts = append(opts, icetable.WithPostCommit(cfg.DeleteAfterCommit))
	return tx.ExpireSnapshots(opts...)
}

func applyPartitionSpec(tx *icetable.Transaction, current *iceberg.PartitionSpec, desired []PartitionFieldConfig, schema *iceberg.Schema) error {
	if len(desired) == 0 {
		return nil
	}
	want, err := buildPartitionSpec(schema, desired)
	if err != nil {
		return err
	}
	if current.CompatibleWith(&want) {
		return nil
	}

	update := tx.UpdateSpec(true)
	for field := range current.Fields() {
		update.RemoveField(field.Name)
	}
	for _, field := range desired {
		transform, err := iceberg.ParseTransform(field.Transform)
		if err != nil {
			return err
		}
		update.AddField(field.Source, transform, field.Name)
	}
	return update.Commit()
}

func sameSortFields(left, right icetable.SortOrder) bool {
	if left.Len() != right.Len() {
		return false
	}
	leftFields := make([]icetable.SortField, 0, left.Len())
	rightFields := make([]icetable.SortField, 0, right.Len())
	for field := range left.Fields() {
		leftFields = append(leftFields, field)
	}
	for field := range right.Fields() {
		rightFields = append(rightFields, field)
	}
	for i := range leftFields {
		if leftFields[i].SourceID != rightFields[i].SourceID ||
			!leftFields[i].Transform.Equals(rightFields[i].Transform) ||
			leftFields[i].Direction != rightFields[i].Direction ||
			leftFields[i].NullOrder != rightFields[i].NullOrder {
			return false
		}
	}
	return true
}

func applySortOrder(ctx context.Context, cat *restcatalog.Catalog, ident icetable.Identifier, tbl *icetable.Table, fields []SortFieldConfig) (*icetable.Table, error) {
	if len(fields) == 0 {
		return tbl, nil
	}
	probe, err := buildSortOrder(tbl.Schema(), fields, icetable.InitialSortOrderID)
	if err != nil {
		return nil, err
	}
	if sameSortFields(tbl.SortOrder(), probe) {
		return tbl, nil
	}

	nextID := icetable.InitialSortOrderID
	for _, order := range tbl.Metadata().SortOrders() {
		if order.OrderID() >= nextID {
			nextID = order.OrderID() + 1
		}
	}
	order, err := buildSortOrder(tbl.Schema(), fields, nextID)
	if err != nil {
		return nil, err
	}
	_, _, err = cat.CommitTable(ctx, ident,
		[]icetable.Requirement{
			icetable.AssertTableUUID(tbl.Metadata().TableUUID()),
			icetable.AssertDefaultSortOrderID(tbl.Metadata().DefaultSortOrder()),
		},
		[]icetable.Update{
			icetable.NewAddSortOrderUpdate(&order),
			icetable.NewSetDefaultSortOrderUpdate(nextID),
		},
	)
	if err != nil {
		return nil, err
	}
	return cat.LoadTable(ctx, ident)
}
