// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package beacon

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
)

type testChainHeaderReader struct {
	config *params.ChainConfig
}

func (c testChainHeaderReader) Config() *params.ChainConfig { return c.config }

func (testChainHeaderReader) CurrentHeader() *types.Header { return nil }

func (testChainHeaderReader) GetHeader(common.Hash, uint64) *types.Header { return nil }

func (testChainHeaderReader) GetHeaderByNumber(uint64) *types.Header { return nil }

func (testChainHeaderReader) GetHeaderByHash(common.Hash) *types.Header { return nil }

var _ consensus.ChainHeaderReader = testChainHeaderReader{}

func TestVerifyHeaderSlotNumber(t *testing.T) {
	slotNumber := uint64(42)
	tests := []struct {
		name        string
		amsterdam   bool
		includeSlot bool
		wantErr     bool
	}{
		{name: "before Amsterdam allows absent slot number"},
		{name: "before Amsterdam rejects slot number", includeSlot: true, wantErr: true},
		{name: "Amsterdam requires slot number", amsterdam: true, wantErr: true},
		{name: "Amsterdam allows slot number", amsterdam: true, includeSlot: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := *params.AllEthashProtocolChanges
			forkTime := uint64(100)
			headerTime := uint64(100)
			if !tt.amsterdam {
				forkTime = 101
			}
			config.AmsterdamTime = &forkTime

			parent := &types.Header{
				Number:     big.NewInt(1),
				Time:       headerTime - 10,
				Difficulty: new(big.Int),
				GasLimit:   30_000_000,
				GasUsed:    15_000_000,
				BaseFee:    big.NewInt(1_000_000_000),
			}
			header := &types.Header{
				Number:     big.NewInt(2),
				Time:       headerTime,
				UncleHash:  types.EmptyUncleHash,
				Difficulty: new(big.Int),
				GasLimit:   parent.GasLimit,
				GasUsed:    parent.GasUsed,
				BaseFee:    new(big.Int).Set(parent.BaseFee),
			}
			if tt.includeSlot {
				header.SlotNumber = &slotNumber
			}

			err := (&Beacon{}).verifyHeader(testChainHeaderReader{config: &config}, header, parent)
			if tt.wantErr && err == nil {
				t.Fatal("expected slotNumber validation error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected header validation error: %v", err)
			}
		})
	}
}
