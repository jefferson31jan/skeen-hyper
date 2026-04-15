package skeen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/protoutil"
)

type SkeenRegistry struct {
	mutex  sync.RWMutex
	chains map[string]*chain
}

var globalRegistry = &SkeenRegistry{
	chains: make(map[string]*chain),
}

var serverOnce sync.Once

type MulticastMsg struct {
	TxID          string   `json:"tx_id"`
	ChannelID     string   `json:"channel_id"`
	SenderNode    string   `json:"sender_node"`
	SenderTSLocal uint64   `json:"sender_ts_local"`
	CrossChannels []string `json:"cross_channels"`
	Payload       []byte   `json:"payload"`   // <-- NOVO: Dados físicos da transação
	Signature     []byte   `json:"signature"` // <-- NOVO: Assinatura do cliente
}

type skeenConsenter struct{}

func New() consensus.Consenter {
	serverOnce.Do(func() {
		go startNetworkServer()
	})
	return &skeenConsenter{}
}

func (s *skeenConsenter) HandleChain(support consensus.ConsenterSupport, metadata *common.Metadata) (consensus.Chain, error) {
	c := &chain{
		support:      support,
		channelID:    support.ChannelID(),
		lamportClock: 0,
		pendingQueue: make(map[string]*PendingTx),
	}

	globalRegistry.mutex.Lock()
	globalRegistry.chains[support.ChannelID()] = c
	globalRegistry.mutex.Unlock()

	return c, nil
}

func (s *skeenConsenter) IsChannelMember(joinBlock *common.Block) (bool, error) {
	fmt.Printf("\n[SKEEN] Validando permissão: O nó foi aceito como membro do canal dinâmico!\n")
	return true, nil
}

type PendingTx struct {
	TxID          string
	Envelope      *common.Envelope
	TSLocal       uint64
	TSFinal       uint64
	IsFinal       bool
	ReceivedTS    map[string]uint64
	CrossChannels []string
}

type chain struct {
	support   consensus.ConsenterSupport
	channelID string

	mutex        sync.Mutex
	lamportClock uint64
	pendingQueue map[string]*PendingTx
}

func (c *chain) Configure(env *common.Envelope, configSeq uint64) error { return nil }
func (c *chain) WaitReady() error                                       { return nil }
func (c *chain) Errored() <-chan struct{}                               { return nil }

func (c *chain) Start() {
	fmt.Printf("[SKEEN] Nó iniciado para o canal: %s\n", c.channelID)
}

func (c *chain) Halt() {
	fmt.Printf("[SKEEN] Nó parado para o canal: %s\n", c.channelID)
}

func (c *chain) Order(env *common.Envelope, configSeq uint64) error {
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		return fmt.Errorf("SKEEN: erro ao abrir payload: %v", err)
	}

	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return fmt.Errorf("SKEEN: erro cabeçalho: %v", err)
	}

	txID := chdr.TxId
	if txID == "" {
		txID = "TX_SISTEMA_INTERNO"
	}

	crossChans := []string{c.channelID}
	if strings.HasPrefix(txID, "CROSS_") {
		crossChans = []string{"canal1", "canal2"}
	}

	c.mutex.Lock()
	c.lamportClock++
	meuTSLocal := c.lamportClock

	c.pendingQueue[txID] = &PendingTx{
		TxID:          txID,
		Envelope:      env,
		TSLocal:       meuTSLocal,
		IsFinal:       false,
		ReceivedTS:    make(map[string]uint64),
		CrossChannels: crossChans,
	}
	fmt.Printf("[SKEEN Canal %s] Tx [%s] recebida. TS Local: %d\n", c.channelID, txID, meuTSLocal)
	c.mutex.Unlock()

	msg := MulticastMsg{
		TxID:          txID,
		ChannelID:     c.channelID,
		SenderNode:    "Coordinator",
		SenderTSLocal: meuTSLocal,
		CrossChannels: crossChans,
		Payload:       env.Payload,   // <-- Anexando os dados
		Signature:     env.Signature, // <-- Anexando a assinatura
	}
	payloadJSON, _ := json.Marshal(msg)

	peers := []string{
		"http://127.0.0.1:17050",
		"http://127.0.0.1:18050",
		"http://127.0.0.1:19050",
		"http://127.0.0.1:20050",
	}

	for i, peer := range peers {
		nodeName := fmt.Sprintf("Orderer-%d", i+1)
		go func(url string, name string) {
			resp, err := http.Post(url+"/skeen/multicast", "application/json", bytes.NewBuffer(payloadJSON))
			if err == nil {
				defer resp.Body.Close()
				var result map[string]uint64
				json.NewDecoder(resp.Body).Decode(&result)

				remoteTS := result["ts"]
				c.ReceiveMulticast(txID, name, remoteTS)
			}
		}(peer, nodeName)
	}

	return nil
}

func (c *chain) ReceiveMulticast(txID string, senderChannel string, senderTSLocal uint64) {
	c.mutex.Lock()
	tx, exists := c.pendingQueue[txID]
	if !exists {
		c.mutex.Unlock()
		return
	}

	tx.ReceivedTS[senderChannel] = senderTSLocal

	// // Se ainda não temos os 4 votos, solta o cadeado e encerra
	// if len(tx.ReceivedTS) < 4 {
	// 	c.mutex.Unlock()
	// 	return
	// }

	// Quorum BFT (2f+1). Para N=4 (f=1), precisamos de 3 respostas.
	if len(tx.ReceivedTS) < 3 {
		c.mutex.Unlock()
		return
	}

	// Se temos os 4 votos, achamos o máximo local AGORA, enquanto a memória está protegida
	var maxTSLocal uint64
	for _, ts := range tx.ReceivedTS {
		if ts > maxTSLocal {
			maxTSLocal = ts
		}
	}
	crossChannels := tx.CrossChannels

	// A MÁGICA ACONTECE AQUI: Libertamos o cadeado ANTES de ir para a rede HTTP!
	c.mutex.Unlock()

	// Dispara a coordenação Cross-Shard totalmente fora do Lock
	c.tryFinalizeTx(txID, maxTSLocal, crossChannels)
}

// Repare que a assinatura mudou: recebe o maxTSLocal calculado em vez do ponteiro do Tx
func (c *chain) tryFinalizeTx(txID string, maxTSLocal uint64, crossChannels []string) {
	finalTS := maxTSLocal

	if len(crossChannels) > 1 {
		for _, outroCanal := range crossChannels {
			if outroCanal == c.channelID {
				continue
			}

			url := "http://127.0.0.1:17050/skeen/exchange_ts"
			reqData, _ := json.Marshal(map[string]interface{}{
				"tx_id":      txID,
				"channel_id": outroCanal,
			})

			resp, err := http.Post(url, "application/json", bytes.NewBuffer(reqData))
			if err == nil {
				defer resp.Body.Close()
				var result map[string]uint64
				json.NewDecoder(resp.Body).Decode(&result)

				remoteTS := result["max_ts"]
				if remoteTS > finalTS {
					finalTS = remoteTS
				}
			}
		}
	}

	// Como liberamos o cadeado, os logs de consolação vão imprimir fora de ordem síncrona (normal em assincronismo)
	fmt.Printf("🏆 [COORDENADOR Shard %s] CONSENSO ATINGIDO: %s | TS Final: %d\n", c.channelID, txID, finalTS)

	peers := []string{"http://127.0.0.1:17050", "http://127.0.0.1:18050", "http://127.0.0.1:19050", "http://127.0.0.1:20050"}
	commitData, _ := json.Marshal(map[string]interface{}{
		"tx_id":      txID,
		"channel_id": c.channelID,
		"final_ts":   finalTS,
	})

	for _, url := range peers {
		go http.Post(url+"/skeen/commit", "application/json", bytes.NewBuffer(commitData))
	}
}

func (c *chain) FinalizeAndDeliver(txID string, finalTS uint64) {
	c.mutex.Lock()
	defer c.mutex.Unlock() // Garante que a porta só abra quando o bloco estiver no disco!

	tx, exists := c.pendingQueue[txID]
	if !exists {
		return
	}
	delete(c.pendingQueue, txID)

	// Agora essas duas operações são atômicas e protegidas de concorrência
	blocoSkeen := c.support.CreateNextBlock([]*common.Envelope{tx.Envelope})
	c.support.WriteBlock(blocoSkeen, nil)

	fmt.Printf("[SKEEN Canal %s] *** BLOCO GERADO E GRAVADO COM SUCESSO NO LEDGER! ***\n", c.channelID)
}

func (c *chain) processQueue() {
	var readyTxs []*PendingTx
	for _, tx := range c.pendingQueue {
		if tx.IsFinal {
			readyTxs = append(readyTxs, tx)
		}
	}
	if len(readyTxs) == 0 {
		return
	}
	sort.Slice(readyTxs, func(i, j int) bool {
		return readyTxs[i].TSFinal < readyTxs[j].TSFinal
	})
	for _, tx := range readyTxs {
		messageBatches, _ := c.support.BlockCutter().Ordered(tx.Envelope)
		for _, batch := range messageBatches {
			block := c.support.CreateNextBlock(batch)
			c.support.WriteBlock(block, nil)
		}
		delete(c.pendingQueue, tx.TxID)
	}
}

func startNetworkServer() {
	fabricPortStr := os.Getenv("ORDERER_GENERAL_LISTENPORT")
	if fabricPortStr == "" {
		fabricPortStr = "7050"
	}
	fabricPort, _ := strconv.Atoi(fabricPortStr)
	skeenPort := fabricPort + 10000

	http.HandleFunc("/skeen/multicast", func(w http.ResponseWriter, r *http.Request) {
		var msg MulticastMsg
		json.NewDecoder(r.Body).Decode(&msg)

		globalRegistry.mutex.RLock()
		c, exists := globalRegistry.chains[msg.ChannelID]
		globalRegistry.mutex.RUnlock()

		var tsResponse uint64 = 0
		if exists {
			c.mutex.Lock()
			if msg.SenderTSLocal > c.lamportClock {
				c.lamportClock = msg.SenderTSLocal
			}
			c.lamportClock++
			tsResponse = c.lamportClock

			// NOVO: Adiciona a transação física na memória DESTE nó!
			if _, ok := c.pendingQueue[msg.TxID]; !ok {
				c.pendingQueue[msg.TxID] = &PendingTx{
					TxID:          msg.TxID,
					Envelope:      &common.Envelope{Payload: msg.Payload, Signature: msg.Signature},
					TSLocal:       tsResponse,
					IsFinal:       false,
					ReceivedTS:    make(map[string]uint64),
					CrossChannels: msg.CrossChannels,
				}
			}
			c.mutex.Unlock()

			// NOVO: Faz o terminal gritar que o nó também trabalhou!
			fmt.Printf("[SKEEN Canal %s] Multicast recebido. TS Local gerado: %d\n", msg.ChannelID, tsResponse)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]uint64{"ts": tsResponse})
	})

	http.HandleFunc("/skeen/exchange_ts", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TxID      string `json:"tx_id"`
			ChannelID string `json:"channel_id"`
		}
		json.NewDecoder(r.Body).Decode(&req)

		globalRegistry.mutex.RLock()
		c, exists := globalRegistry.chains[req.ChannelID]
		globalRegistry.mutex.RUnlock()

		var currentMax uint64 = 0
		if exists {
			c.mutex.Lock()
			currentMax = c.lamportClock
			c.mutex.Unlock()
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]uint64{"max_ts": currentMax})
	})

	http.HandleFunc("/skeen/commit", func(w http.ResponseWriter, r *http.Request) {
		var msg struct {
			TxID      string `json:"tx_id"`
			ChannelID string `json:"channel_id"`
			FinalTS   uint64 `json:"final_ts"`
		}
		json.NewDecoder(r.Body).Decode(&msg)

		globalRegistry.mutex.RLock()
		c, exists := globalRegistry.chains[msg.ChannelID]
		globalRegistry.mutex.RUnlock()

		if exists {
			c.FinalizeAndDeliver(msg.TxID, msg.FinalTS)
		}
		w.WriteHeader(http.StatusOK)
	})

	fmt.Printf("[SKEEN NETWORK LAYER] API HTTP rodando na porta: %d\n", skeenPort)
	http.ListenAndServe(fmt.Sprintf(":%d", skeenPort), nil)
}
