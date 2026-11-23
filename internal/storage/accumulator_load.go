package storage

import (
	"bytes"
	"errors"
	"fmt"

	"bitcoin-pure/internal/utreexo"
	"github.com/cockroachdb/pebble"
)

// Accumulator keys sort by depth and then path. Read the leaves as a group
// for the tree build, then match the rebuilt records against ordered disk
// iterators. Proofs use the persisted branches, so the header root alone
// cannot detect wrong stored child links or sibling hashes.
func loadAccumulatorRecords(reader storeReader) (*utreexo.Accumulator, error) {
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: utxoAccumulatorNodePrefix,
		UpperBound: utxoAccumulatorNodePrefixEnd,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	leaves := make([]utreexo.UtxoLeaf, 0)
	rowCount := 0
	for iter.First(); iter.Valid(); iter.Next() {
		path, err := decodeAccumulatorNodeKey(iter.Key())
		if err != nil {
			return nil, err
		}
		rowCount++
		if path.Depth != utreexo.KeyBits {
			continue
		}
		row, err := decodeAccumulatorNodeValue(path, iter.Value())
		if err != nil {
			return nil, err
		}
		if row.Leaf == nil {
			return nil, errors.New("expected leaf at terminal accumulator depth")
		}
		leaves = append(leaves, *row.Leaf)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	acc, err := utreexo.NewAccumulatorFromLeaves(leaves)
	if err != nil {
		return nil, err
	}
	if expected := utreexo.MaterialNodeCount(acc); rowCount != expected {
		return nil, fmt.Errorf("utxo accumulator physical record count=%d, want %d", rowCount, expected)
	}
	if err := validateOrderedAccumulatorRecords(reader, acc); err != nil {
		return nil, err
	}
	return acc, nil
}

// A left-to-right traversal visits keys at each depth in Pebble order. A
// cursor per populated depth avoids one point lookup per node.
func validateOrderedAccumulatorRecords(reader storeReader, acc *utreexo.Accumulator) error {
	var iters [utreexo.KeyBits + 1]*pebble.Iterator
	defer func() {
		for _, iter := range iters {
			if iter != nil {
				iter.Close()
			}
		}
	}()
	return utreexo.ForEachAccumulatorNodeRecord(acc, func(record utreexo.AccumulatorNodeRecord) error {
		depth := record.Path.Depth
		key := accumulatorNodeKey(record.Path)
		iter := iters[depth]
		if iter == nil {
			depthPrefix := key[:len(utxoAccumulatorNodePrefix)+2]
			var err error
			iter, err = reader.NewIter(&pebble.IterOptions{
				LowerBound: depthPrefix,
				UpperBound: prefixUpperBound(depthPrefix),
			})
			if err != nil {
				return err
			}
			iters[depth] = iter
			iter.First()
		}
		if err := iter.Error(); err != nil {
			return err
		}
		if !iter.Valid() || !bytes.Equal(key, iter.Key()) || !bytes.Equal(encodeAccumulatorNodeValue(record), iter.Value()) {
			return fmt.Errorf("utxo accumulator record mismatch at depth %d", depth)
		}
		iter.Next()
		return iter.Error()
	})
}
