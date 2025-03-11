// Copyright 2017 The go-ethereum Authors
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

// faucet is an Ether faucet backed by a light client.
package main

//go:generate go-bindata -nometadata -o website.go faucet.html
//go:generate gofmt -w -s website.go

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/gorilla/websocket"
)

const (
	startKey = "StartBlock"
)

var (
	ethRpcFlag  = flag.String("ethrpc", "", "Ethereum mainnet rpc URL for ethclient to get address balance")
	sepRpcFlag  = flag.String("sepwsrpc", "", "Ethereum Sepolia rpc URL for ethclient to get swap transaction")
	wsRpcFlag   = flag.String("wsrpc", "", "websocket rpc URL for ethclient to get data and submit tx")
	apiPortFlag = flag.Int("apiport", 81, "Listener port for the HTTP API connection")
	dataPath    = flag.String("datadir", "./es-data", "Data directory for the databases")

	payoutFlag  = flag.Int("faucet.amount", 1, "Number of (0.01) WSC to pay out per user request")
	minutesFlag = flag.Int("faucet.minutes", 1440, "Number of minutes to wait between funding rounds")

	accJSONFlag = flag.String("account.json", "", "Key json file to fund user requests with")
	accPassFlag = flag.String("account.pass", "", "Decryption password to access faucet funds")

	logFlag = flag.Int("loglevel", 0, "Log level to use for Ethereum and the faucet")
)

var (
	minEthNoAuthAddrNeed = new(big.Int).Exp(big.NewInt(10), big.NewInt(16), nil)
	ether                = new(big.Int).Exp(big.NewInt(10), big.NewInt(16), nil)
)

func main() {
	// Parse the flags and set up the logger to print everything requested
	flag.Parse()
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, slog.Level(*logFlag), true)))

	// Load up the account key and decrypt its password
	blob, err := os.ReadFile(*accPassFlag)
	if err != nil {
		log.Crit("Failed to read account password contents", "file", *accPassFlag, "err", err)
	}
	pass := strings.TrimSuffix(string(blob), "\n")

	path := filepath.Join(os.Getenv("HOME"), ".faucet", "keys")
	os.RemoveAll(path)
	ks := keystore.NewKeyStore(path, keystore.StandardScryptN, keystore.StandardScryptP)
	if blob, err = os.ReadFile(*accJSONFlag); err != nil {
		log.Crit("Failed to read account key contents", "file", *accJSONFlag, "err", err)
	}
	acc, err := ks.Import(blob, pass, pass)
	if err != nil && err != keystore.ErrAccountAlreadyExists {
		log.Crit("Failed to import faucet signer account", "err", err)
	}
	if err := ks.Unlock(acc, pass); err != nil {
		log.Crit("Failed to unlock faucet signer account", "err", err)
	}
	log.Info("keystore info", "address", acc.Address)
	// Assemble and start the faucet light service

	db, err := leveldb.New(*dataPath, 2048, 8196, "es-data/db/faucet/", false)

	faucet, err := newFaucet(*wsRpcFlag, *ethRpcFlag, *sepRpcFlag, ks, rawdb.NewDatabase(db))
	if err != nil {
		log.Crit("Failed to start faucet", "err", err)
	}

	if err := faucet.listenAndServe(*apiPortFlag); err != nil {
		log.Crit("Failed to launch faucet API", "err", err)
	}
}

// request represents an accepted funding request.
type request struct {
	Type    string             `json:"type"`    // Avatar URL to make the UI nicer
	Account common.Address     `json:"account"` // Ethereum address being funded
	Time    time.Time          `json:"time"`    // Timestamp when the request was accepted
	Tx      *types.Transaction `json:"tx"`      // Transaction funding the account
}

// faucet represents a crypto faucet backed by an Ethereum light client.
type faucet struct {
	ethClient *ethclient.Client // Client connection to the Ethereum chain
	sepClient *ethclient.Client // Client connection to the Sepolia chain
	client    *ethclient.Client // Client connection to the L2 chain
	signer    types.Signer

	start  *big.Int       // Block high start to monitor
	latest *big.Int       // Block high start to monitor
	db     ethdb.Database // DB save swap related data

	keystore *keystore.KeyStore // Keystore containing the single signer
	account  accounts.Account   // Account funding user faucet requests
	head     *types.Header      // Current head header of the faucet
	balance  *big.Int           // Current balance of the faucet
	nonce    uint64             // Current pending nonce of the faucet
	chainId  *big.Int           // Current chainId use to generate faucet tx
	price    *big.Int           // Current gas price to issue funds with

	conns    []*wsConn            // Currently live websocket connections
	timeouts map[string]time.Time // History of users and their funding timeouts
	reqs     []*request           // Currently pending funding requests
	update   chan struct{}        // Channel to signal request updates

	lock sync.RWMutex // Lock protecting the faucet's internals
}

// wsConn wraps a websocket connection with a write mutex as the underlying
// websocket library does not synchronize access to the stream.
type wsConn struct {
	conn  *websocket.Conn
	wlock sync.Mutex
}

func newFaucet(rpcFlag, ethRpcFlag, sepRpcFlag string, ks *keystore.KeyStore, db ethdb.Database) (*faucet, error) {
	client, err := ethclient.Dial(rpcFlag)
	if err != nil {
		return nil, err
	}
	chainId, err := client.ChainID(context.Background())
	if err != nil {
		return nil, fmt.Errorf("ChainID failed:%w", err)
	}

	var ethClient *ethclient.Client
	ethClient, err = ethclient.Dial(ethRpcFlag)
	if err != nil {
		return nil, err
	}

	var sepClient *ethclient.Client
	sepClient, err = ethclient.Dial(sepRpcFlag)
	if err != nil {
		return nil, err
	}

	signer := types.NewCancunSigner(chainId)
	return &faucet{
		ethClient: ethClient,
		sepClient: sepClient,
		client:    client,
		signer:    signer,
		keystore:  ks,
		db:        db,
		chainId:   chainId,
		account:   ks.Accounts()[0],
		timeouts:  make(map[string]time.Time),
		update:    make(chan struct{}, 1),
	}, nil
}

// listenAndServe registers the HTTP handlers for the faucet and boots it up
// for service user funding requests.
func (f *faucet) listenAndServe(port int) error {
	go f.loop()
	go f.loopSwap()

	// http.HandleFunc("/", f.webHandler)
	http.HandleFunc("/api", f.apiHandler)
	return http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

// apiHandler handles requests for Ether grants and transaction statuses.
func (f *faucet) apiHandler(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	// Start tracking the connection and drop at the end
	defer conn.Close()

	f.lock.Lock()
	wsconn := &wsConn{conn: conn}
	f.conns = append(f.conns, wsconn)
	f.lock.Unlock()

	defer func() {
		f.lock.Lock()
		for i, c := range f.conns {
			if c.conn == conn {
				f.conns = append(f.conns[:i], f.conns[i+1:]...)
				break
			}
		}
		f.lock.Unlock()
	}()
	// Gather the initial stats from the network to report
	var (
		head    *types.Header
		balance *big.Int
		nonce   uint64
	)
	for head == nil || balance == nil {
		// Retrieve the current stats cached by the faucet
		f.lock.RLock()
		if f.head != nil {
			head = types.CopyHeader(f.head)
		}
		if f.balance != nil {
			balance = new(big.Int).Set(f.balance)
		}
		nonce = f.nonce
		f.lock.RUnlock()

		if head == nil || balance == nil {
			// Report the faucet offline until initial stats are ready
			//lint:ignore ST1005 This error is to be displayed in the browser
			if err = sendError(wsconn, "", errors.New("faucet offline")); err != nil {
				log.Warn("Failed to send faucet error to client", "err", err)
				return
			}
			time.Sleep(3 * time.Second)
		}
	}
	// Send over the initial stats and the latest header
	f.lock.RLock()
	reqs := f.reqs
	f.lock.RUnlock()
	if err = send(wsconn, map[string]interface{}{
		"funds":    new(big.Int).Div(balance, ether),
		"funded":   nonce,
		"requests": reqs,
		"target":   f.account.Address.Hex(),
	}, 3*time.Second); err != nil {
		log.Warn("Failed to send initial stats to client", "err", err)
		return
	}
	if err = send(wsconn, head, 3*time.Second); err != nil {
		log.Warn("Failed to send initial header to client", "err", err)
		return
	}
	// Keep reading requests from the websocket until the connection breaks
	for {
		// Fetch the next funding request and validate against github
		var msg struct {
			Address string `json:"address"`
			Hash    string `json:"hash"`
		}
		var resp string
		var source string
		if err = conn.ReadJSON(&msg); err != nil {
			return
		}

		if msg.Address != "" {
			source = "faucet"
			resp, err = f.faucet(common.HexToAddress(msg.Address))
		} else {
			source = "swap"
			resp, err = f.checkSwapTX(common.HexToHash(msg.Hash))
		}

		if err != nil {
			if err = sendError(wsconn, source, err); err != nil {
				log.Warn("Failed to send prefix error to client", "err", err)
				return
			}
			continue
		}
		if err = sendSuccess(wsconn, source, resp); err != nil {
			log.Warn("Failed to send funding success to client", "err", err)
			return
		}

		select {
		case f.update <- struct{}{}:
		default:
		}
	}
}

func (f *faucet) checkSwapTX(hash common.Hash) (string, error) {
	timeout := time.Now().Add(1 * time.Minute)
	for !time.Now().After(timeout) {
		bs, err := f.db.Get(hash.Bytes())
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		l2TxHash := common.BytesToHash(bs)
		return fmt.Sprintf("Assign tx related to Sepolia tx %s has submitted to L2 with hash %s", hash.Hex(), l2TxHash.Hex()), nil
	}

	return "", fmt.Errorf("do not found assign tx from L2 related to %s tx in Sepolia within 1 minute", hash.Hex())
}

func (f *faucet) faucet(address common.Address) (string, error) {
	var (
		id = address.Hex()
	)

	err := checkEthMainnetBalance(f.ethClient, address)
	if err != nil {
		return "", err
	}
	log.Info("Faucet request valid", "address", address)

	// Ensure the user didn't request funds too recently
	f.lock.Lock()
	defer f.lock.Unlock()
	var (
		timeout time.Time
	)
	if timeout = f.timeouts[id]; time.Now().After(timeout) {
		// User wasn't funded recently, create the funding transaction
		amount := new(big.Int).Mul(big.NewInt(int64(*payoutFlag)), ether)
		signed, err := f.sendTX(address, amount)
		if err != nil {
			return "", err
		}

		f.reqs = append(f.reqs, &request{
			Type:    "faucet",
			Account: address,
			Time:    time.Now(),
			Tx:      signed,
		})
		timeout := time.Duration(*minutesFlag) * time.Minute
		grace := timeout / 288 // 24h timeout => 5m grace

		f.timeouts[id] = time.Now().Add(timeout - grace)
		return fmt.Sprintf("Funding request accepted for into %s", address.Hex()), nil
	}

	return "", fmt.Errorf("%s left until next allowance", common.PrettyDuration(time.Until(timeout)))
}

func (f *faucet) swap(to common.Address, val *big.Int) (common.Hash, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	signed, err := f.sendTX(to, val)
	if err != nil {
		return common.Hash{}, err
	}
	f.reqs = append(f.reqs, &request{
		Type:    "swap",
		Account: to,
		Time:    time.Now(),
		Tx:      signed,
	})

	return signed.Hash(), nil
}

func (f *faucet) sendTX(to common.Address, val *big.Int) (*types.Transaction, error) {
	tx := types.NewTx(&types.LegacyTx{
		Nonce:    f.nonce + uint64(len(f.reqs)),
		To:       &to,
		Value:    val,
		Gas:      21000,
		GasPrice: f.price,
		Data:     nil,
	})
	signed, err := f.keystore.SignTx(f.account, tx, f.chainId)
	if err != nil {
		log.Warn("SignTx failed", "err", err)
		return nil, err
	}
	// Submit the transaction and mark as funded if successful
	if err := f.client.SendTransaction(context.Background(), signed); err != nil {
		sender, _ := f.signer.Sender(signed)
		log.Warn("SendTransaction failed", "err", err, "from", sender)
		return nil, err
	}
	return signed, nil
}

// refresh attempts to retrieve the latest header from the chain and extract the
// associated faucet balance and nonce for connectivity caching.
func (f *faucet) refresh(head *types.Header) error {
	// Ensure a state update does not run for too long
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// If no header was specified, use the current chain head
	var err error
	if head == nil {
		if head, err = f.client.HeaderByNumber(ctx, nil); err != nil {
			return err
		}
	}
	// Retrieve the balance, nonce and gas price from the current head
	var (
		balance *big.Int
		nonce   uint64
		price   *big.Int
	)
	if balance, err = f.client.BalanceAt(ctx, f.account.Address, head.Number); err != nil {
		return err
	}
	if nonce, err = f.client.NonceAt(ctx, f.account.Address, head.Number); err != nil {
		return err
	}
	if price, err = f.client.SuggestGasPrice(ctx); err != nil {
		return err
	}
	// Everything succeeded, update the cached stats and eject old requests
	f.lock.Lock()
	f.head, f.balance = head, balance
	f.price, f.nonce = price, nonce
	for len(f.reqs) > 0 && f.reqs[0].Tx.Nonce() < f.nonce {
		f.reqs = f.reqs[1:]
	}
	f.lock.Unlock()

	return nil
}

func (f *faucet) loopSwap() {
	chainID, err := f.sepClient.ChainID(context.Background())
	if err != nil {
		log.Crit("Failed to fetch Sepolia chainID", "err", err)
	}
	s, err := f.sepClient.BlockNumber(context.Background())
	if err != nil {
		log.Crit("Failed to fetch latest Sepolia block number", "err", err)
	}
	f.latest = new(big.Int).SetUint64(s)
	log.Info("Sepolia latest block init", "blockNumber", f.latest)

	val, err := f.db.Get([]byte(fmt.Sprintf(startKey)))
	if err == nil {
		f.start = new(big.Int).SetBytes(val)
		log.Info("Sepolia start block init", "blockNumber", f.start)
	}

	if f.start == nil || f.start.Cmp(f.latest) > 0 || f.start.Cmp(new(big.Int).SetUint64(0)) <= 0 {
		f.start = f.latest
		log.Info("Sepolia start block init to latest", "blockNumber", f.start)
	}

	heads := make(chan *types.Header, 16)
	sub, err := f.sepClient.SubscribeNewHead(context.Background(), heads)
	if err != nil {
		log.Crit("Failed to subscribe to Sepolia head events", "err", err)
	}
	defer sub.Unsubscribe()

	go func() {
		signer := types.NewCancunSigner(chainID)
		for {
			blockNum, latestNum := f.start, f.latest
			for blockNum.Cmp(latestNum) <= 0 {
				block, err := f.sepClient.BlockByNumber(context.Background(), blockNum)
				if err != nil {
					log.Warn("Error fetching Sepolia block:", "blockNumber", blockNum, "err", err)
					break
				}
				log.Info("Processing Sepolia block", "blockNumber", blockNum, "txCount", len(block.Transactions()))

				success := true
				for _, tx := range block.Transactions() {
					if tx.To() != nil && *tx.To() == f.account.Address {
						exist, err := f.db.Has(tx.Hash().Bytes())
						if err == nil && exist {
							continue
						}

						from, err := types.Sender(signer, tx)
						if err != nil {
							log.Warn("Failed to swap data", "err", err)
							success = false
							break
						}

						amount := new(big.Int).Mul(tx.Value(), new(big.Int).SetUint64(10000))
						hash, err := f.swap(from, amount)
						if err != nil {
							log.Warn("Failed to swap data", "err", err)
							success = false
							break
						}
						f.db.Put(tx.Hash().Bytes(), hash.Bytes())

						fmt.Printf("ETH Transfer Swap! Block: %d, Tx Hash: %s, Value: %s wei\n",
							block.NumberU64(), tx.Hash().Hex(), tx.Value().String())
					}
				}
				if success {
					blockNum = new(big.Int).Add(blockNum, big.NewInt(1))
					f.start = blockNum
					f.db.Put([]byte(fmt.Sprintf(startKey)), block.Number().Bytes())
					log.Info("Processed Sepolia block", "blockNumber", blockNum)
				} else {
					time.Sleep(2 * time.Second)
				}
			}
			time.Sleep(time.Second)
		}
	}()

	for {
		select {
		case head := <-heads:
			f.latest = head.Number
			log.Info("Sepolia latest block number updated", "blockNumber", f.latest)
		}
	}
}

// loop keeps waiting for interesting events and pushes them out to connected
// websockets.
func (f *faucet) loop() {
	// Wait for chain events and push them to clients
	heads := make(chan *types.Header, 16)
	sub, err := f.client.SubscribeNewHead(context.Background(), heads)
	if err != nil {
		log.Crit("Failed to subscribe to head events", "err", err)
	}
	defer sub.Unsubscribe()

	// Start a goroutine to update the state from head notifications in the background
	update := make(chan *types.Header)

	go func() {
		for head := range update {
			// New chain head arrived, query the current stats and stream to clients
			timestamp := time.Unix(int64(head.Time), 0)
			if time.Since(timestamp) > time.Hour {
				log.Warn("Skipping faucet refresh, head too old", "number", head.Number, "hash", head.Hash(), "age", common.PrettyAge(timestamp))
				continue
			}
			if err := f.refresh(head); err != nil {
				log.Warn("Failed to update faucet state", "block", head.Number, "hash", head.Hash(), "err", err)
				continue
			}
			// Faucet state retrieved, update locally and send to clients
			f.lock.RLock()
			log.Debug("Updated faucet state", "number", head.Number, "hash", head.Hash(), "age", common.PrettyAge(timestamp), "balance", f.balance, "nonce", f.nonce, "price", f.price)

			balance := new(big.Int).Div(f.balance, ether)

			for _, conn := range f.conns {
				if err := send(conn, map[string]interface{}{
					"funds":    balance,
					"funded":   f.nonce,
					"requests": f.reqs,
					"target":   f.account.Address.Hex(),
				}, time.Second); err != nil {
					log.Warn("Failed to send stats to client", "err", err)
					conn.conn.Close()
					continue
				}
				if err := send(conn, head, time.Second); err != nil {
					log.Warn("Failed to send header to client", "err", err)
					conn.conn.Close()
				}
			}
			f.lock.RUnlock()
		}
	}()
	// Wait for various events and assign to the appropriate background threads
	for {
		select {
		case head := <-heads:
			// New head arrived, send if for state update if there's none running
			select {
			case update <- head:
			default:
			}

		case <-f.update:
			// Pending requests updated, stream to clients
			f.lock.RLock()
			for _, conn := range f.conns {
				if err := send(conn, map[string]interface{}{"requests": f.reqs}, time.Second); err != nil {
					log.Warn("Failed to send requests to client", "err", err)
					conn.conn.Close()
				}
			}
			f.lock.RUnlock()
		}
	}
}

// sends transmits a data packet to the remote end of the websocket, but also
// setting a write deadline to prevent waiting forever on the node.
func send(conn *wsConn, value interface{}, timeout time.Duration) error {
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	conn.wlock.Lock()
	defer conn.wlock.Unlock()
	conn.conn.SetWriteDeadline(time.Now().Add(timeout))
	return conn.conn.WriteJSON(value)
}

// sendError transmits an error to the remote end of the websocket, also setting
// the write deadline to 1 second to prevent waiting forever.
func sendError(conn *wsConn, source string, err error) error {
	return send(conn, map[string]string{"error": err.Error(), "source": source}, time.Second)
}

// sendSuccess transmits a success message to the remote end of the websocket, also
// setting the write deadline to 1 second to prevent waiting forever.
func sendSuccess(conn *wsConn, source string, msg string) error {
	return send(conn, map[string]string{"success": msg, "source": source}, time.Second)
}

// checkEthMainnetBalance tries to interpret a faucet request as a plain Ethereum address,
// without actually performing any remote authentication. This mode is prone to
// Byzantine attack, so only ever use for truly private networks.
func checkEthMainnetBalance(ethClient *ethclient.Client, address common.Address) error {
	bal, err := ethClient.BalanceAt(context.Background(), address, nil)
	if err != nil {
		return errors.New("fail to get user balance from Ethereum mainnet")
	}
	if bal.Cmp(minEthNoAuthAddrNeed) < 0 {
		return errors.New(fmt.Sprintf("your address %s need to have at least 0.01 eth in Ethereum mainnet.", address))
	}
	return nil
}
