// Copyright 2020 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum/cmd/devp2p/internal/v5test"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p/discover"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/p2p/enr"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/urfave/cli/v2"
)

var (
	discv5DumpFlag = &cli.BoolFlag{
		Name:  "dump",
		Usage: "Dump all peers every 10 seconds",
	}
	opStackChainIDFlag = &cli.Uint64Flag{
		Name:  "opstack-chainid",
		Usage: "Filter nodes by OP Stack chain ID (only nodes with matching opstack ENR entry will be accepted)",
	}
	chainIDFlag = &cli.Uint64Flag{
		Name:  "chainid",
		Usage: "Filter nodes by chain ID (only nodes with matching chainID ENR entry will be accepted)",
	}
	discv5Command = &cli.Command{
		Name:  "discv5",
		Usage: "Node Discovery v5 tools",
		Subcommands: []*cli.Command{
			discv5PingCommand,
			discv5ResolveCommand,
			discv5CrawlCommand,
			discv5TestCommand,
			discv5ListenCommand,
		},
	}
	discv5PingCommand = &cli.Command{
		Name:   "ping",
		Usage:  "Sends ping to a node",
		Action: discv5Ping,
		Flags:  discoveryNodeFlags,
	}
	discv5ResolveCommand = &cli.Command{
		Name:   "resolve",
		Usage:  "Finds a node in the DHT",
		Action: discv5Resolve,
		Flags:  discoveryNodeFlags,
	}
	discv5CrawlCommand = &cli.Command{
		Name:   "crawl",
		Usage:  "Updates a nodes.json file with random nodes found in the DHT",
		Action: discv5Crawl,
		Flags: slices.Concat(discoveryNodeFlags, []cli.Flag{
			crawlTimeoutFlag,
		}),
	}
	discv5TestCommand = &cli.Command{
		Name:   "test",
		Usage:  "Runs protocol tests against a node",
		Action: discv5Test,
		Flags: []cli.Flag{
			testPatternFlag,
			testTAPFlag,
			testListen1Flag,
			testListen2Flag,
		},
	}
	discv5ListenCommand = &cli.Command{
		Name:   "listen",
		Usage:  "Runs a node",
		Action: discv5Listen,
		Flags:  slices.Concat(discoveryNodeFlags, []cli.Flag{discv5DumpFlag, opStackChainIDFlag, chainIDFlag}),
	}
)

func discv5Ping(ctx *cli.Context) error {
	n := getNodeArg(ctx)
	disc, _ := startV5(ctx)
	defer disc.Close()

	_, err := disc.Ping(n)
	fmt.Println(err)
	return nil
}

func discv5Resolve(ctx *cli.Context) error {
	n := getNodeArg(ctx)
	disc, _ := startV5(ctx)
	defer disc.Close()

	fmt.Println(disc.Resolve(n))
	return nil
}

func discv5Crawl(ctx *cli.Context) error {
	if ctx.NArg() < 1 {
		return errors.New("need nodes file as argument")
	}
	nodesFile := ctx.Args().First()
	inputSet := make(nodeSet)
	if common.FileExist(nodesFile) {
		inputSet = loadNodesJSON(nodesFile)
	}

	disc, config := startV5(ctx)
	defer disc.Close()

	c, err := newCrawler(inputSet, config.Bootnodes, disc, disc.RandomNodes())
	if err != nil {
		return err
	}
	c.revalidateInterval = 10 * time.Minute
	output := c.run(ctx.Duration(crawlTimeoutFlag.Name), ctx.Int(crawlParallelismFlag.Name))
	writeNodesJSON(nodesFile, output)
	return nil
}

// discv5Test runs the protocol test suite.
func discv5Test(ctx *cli.Context) error {
	suite := &v5test.Suite{
		Dest:    getNodeArg(ctx),
		Listen1: ctx.String(testListen1Flag.Name),
		Listen2: ctx.String(testListen2Flag.Name),
	}
	return runTests(ctx, suite.AllTests())
}

func discv5Listen(ctx *cli.Context) error {
	disc, _ := startV5(ctx)
	defer disc.Close()

	fmt.Println(disc.Self())

	if ctx.Bool(discv5DumpFlag.Name) {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			nodes := disc.AllNodes()
			fmt.Printf("\n--- Dumping %d peers ---\n", len(nodes))
			for i, n := range nodes {
				fmt.Printf("\n[%d] %s\n", i+1, n.String())
				dumpRecord(os.Stdout, n.Record())
			}
		}
	}

	select {}
}

// opStackENRData is the ENR entry for OP Stack chain identification.
type opStackENRData struct {
	chainID uint64
	version uint64
}

func (o *opStackENRData) ENRKey() string { return "opstack" }

func (o *opStackENRData) DecodeRLP(s *rlp.Stream) error {
	b, err := s.Bytes()
	if err != nil {
		return fmt.Errorf("failed to decode outer ENR entry: %w", err)
	}
	r := bytes.NewReader(b)
	chainID, err := binary.ReadUvarint(r)
	if err != nil {
		return fmt.Errorf("failed to read chain ID var int: %w", err)
	}
	version, err := binary.ReadUvarint(r)
	if err != nil {
		return fmt.Errorf("failed to read version var int: %w", err)
	}
	o.chainID = chainID
	o.version = version
	return nil
}

var _ enr.Entry = (*opStackENRData)(nil)

// chainIDENRData is the ENR entry for chain ID identification.
type chainIDENRData uint64

func (c chainIDENRData) ENRKey() string { return "chainID" }

var _ enr.Entry = (*chainIDENRData)(nil)

// startV5 starts an ephemeral discovery v5 node.
func startV5(ctx *cli.Context) (*discover.UDPv5, discover.Config) {
	ln, config := makeDiscoveryConfig(ctx)

	// Set up OP Stack chain ID filter if specified
	if ctx.IsSet(opStackChainIDFlag.Name) {
		expectedChainID := ctx.Uint64(opStackChainIDFlag.Name)
		config.NodeFilter = func(node *enode.Node) bool {
			var dat opStackENRData
			if err := node.Load(&dat); err != nil {
				log.Debug("Node has no opstack ENR entry", "id", node.ID(), "ip", node.IP(), "err", err)
				return false
			}
			if dat.chainID != expectedChainID {
				log.Info("Node has different chain ID", "id", node.ID(), "ip", node.IP(), "got", dat.chainID, "expected", expectedChainID)
				return false
			}
			if dat.version != 0 {
				log.Info("Node has different version", "id", node.ID(), "ip", node.IP(), "got", dat.version, "expected", 0)
				return false
			}
			log.Info("Node passed filter", "id", node.ID(), "ip", node.IP(), "chainID", dat.chainID)
			return true
		}
		log.Info("OP Stack node filter enabled", "chainID", expectedChainID)
	}

	// Set up chain ID filter if specified
	if ctx.IsSet(chainIDFlag.Name) {
		expectedChainID := ctx.Uint64(chainIDFlag.Name)
		config.NodeFilter = func(node *enode.Node) bool {
			var dat chainIDENRData
			if err := node.Load(&dat); err != nil {
				log.Debug("Node has no chainID ENR entry", "id", node.ID(), "ip", node.IP(), "err", err)
				return false
			}
			if uint64(dat) != expectedChainID {
				log.Info("Node has different chain ID", "id", node.ID(), "ip", node.IP(), "got", uint64(dat), "expected", expectedChainID)
				return false
			}
			log.Info("Node passed chainID filter", "id", node.ID(), "ip", node.IP(), "chainID", uint64(dat))
			return true
		}
		log.Info("Chain ID node filter enabled", "chainID", expectedChainID)
	}

	socket := listen(ctx, ln)
	disc, err := discover.ListenV5(socket, ln, config)
	if err != nil {
		exit(err)
	}
	return disc, config
}
