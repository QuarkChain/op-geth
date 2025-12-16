// Copyright 2025 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/params"
)

// ChainIDEntry is the ENR entry which advertises the chain ID on discovery.
type ChainIDEntry uint64

// ENRKey implements enr.Entry.
func (e ChainIDEntry) ENRKey() string {
	return "chainID"
}

// SetChainIDENR sets the chainID ENR entry on the local node.
func SetChainIDENR(ln *enode.LocalNode, cfg *params.ChainConfig) {
	if cfg == nil || cfg.ChainID == nil {
		return
	}
	entry := ChainIDEntry(cfg.ChainID.Uint64())
	ln.Set(&entry)
}
