package skeen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/protoutil"
)

// ==========================================
// REGISTROS GLOBAIS E ESTRUTURAS
// ==========================================
var (
	startTime    time.Time
	txCount      int
	metricsMutex sync.Mutex
)

type SkeenRegistry struct {
	mutex  sync.RWMutex
	chains map[string]*chain
}

var globalRegistry = &SkeenRegistry{
	chains: make(map[string]*chain),
}

var serverOnce sync.Once

type skeenConsenter struct{}

func New() consensus.Consenter {
	serverOnce.Do(func() {
		go startNetworkServer()
	})
	return &skeenConsenter{}
}

func (s *skeenConsenter) HandleChain(support consensus.ConsenterSupport, metadata *common.Metadata) (consensus.Chain, error) {
	c := &chain{
		support:        support,
		channelID:      support.ChannelID(),
		lamportClock:   0,
		pendingQueue:   make(map[string]*PendingTx),
		localTSHistory: make(map[string]uint64),
	}

	globalRegistry.mutex.Lock()
	globalRegistry.chains[support.ChannelID()] = c
	globalRegistry.mutex.Unlock()

	return c, nil
}

func (s *skeenConsenter) IsChannelMember(joinBlock *common.Block) (bool, error) {
	return true, nil
}

type PendingTx struct {
	TxID          string
	Envelope      *common.Envelope
	TSLocal       uint64
	CrossChannels []string
}

type chain struct {
	support   consensus.ConsenterSupport
	channelID string

	mutex          sync.Mutex
	blockMutex     sync.Mutex
	lamportClock   uint64
	pendingQueue   map[string]*PendingTx
	localTSHistory map[string]uint64
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

// ==========================================
// MOTOR DE CONSENSO (A PORTA DE ENTRADA)
// ==========================================

func (c *chain) Order(env *common.Envelope, configSeq uint64) error {
	metricsMutex.Lock()
	if txCount == 0 {
		startTime = time.Now() // Marca o início da rajada de testes
	}
	txCount++
	metricsMutex.Unlock()

	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		return err
	}

	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return err
	}

	txID := chdr.TxId
	if txID == "" {
		txID = "INTERNAL_TX"
	}

	crossChans := []string{c.channelID}
	if strings.HasPrefix(txID, "CROSS_") {
		partes := strings.Split(txID, "_")
		if len(partes) >= 2 {
			crossChans = strings.Split(partes[1], "-")
		}
	}

	// 🚨 FIREWALL DE PROTEÇÃO (EDGE FILTERING) 🚨
	souParticipante := false
	for _, ch := range crossChans {
		if ch == c.channelID {
			souParticipante = true
			break
		}
	}

	if !souParticipante {
		return nil
	}

	c.mutex.Lock()
	if _, exists := c.pendingQueue[txID]; exists {
		c.mutex.Unlock()
		return nil
	}
	if _, exists := c.localTSHistory[txID]; exists {
		c.mutex.Unlock()
		return nil
	}

	c.lamportClock++
	meuTSLocal := c.lamportClock

	c.pendingQueue[txID] = &PendingTx{
		TxID:          txID,
		Envelope:      env,
		TSLocal:       meuTSLocal,
		CrossChannels: crossChans,
	}
	c.mutex.Unlock()

	fmt.Printf("[SKEEN Canal %s] Tx [%s] recebida. TS: %d | Papel: PEER (All-to-All)\n", c.channelID, txID, meuTSLocal)

	if len(crossChans) > 1 {
		go c.tryFinalizeTx(txID, meuTSLocal, crossChans)
	} else {
		c.FinalizeAndDeliver(txID, meuTSLocal)
	}

	return nil
}

// ==========================================
// FASE 2: COORDENAÇÃO DESCENTRALIZADA (O(N^2))
// ==========================================

func (c *chain) tryFinalizeTx(txID string, maxTSLocal uint64, crossChannels []string) {
	finalTS := maxTSLocal

	if len(crossChannels) > 1 {
		// ALL-TO-ALL: Faz polling de todos os nós participantes (ignora os de fora)
		for _, outroCanal := range crossChannels {
			if outroCanal == c.channelID {
				continue
			}

			port := getPortForChannel(outroCanal)
			if port == 0 {
				continue
			}

			url := fmt.Sprintf("http://127.0.0.1:%d/skeen/exchange_ts", port)
			reqData, _ := json.Marshal(map[string]interface{}{"tx_id": txID, "channel_id": outroCanal})

			fmt.Printf("📡 [ALL-TO-ALL] Shard %s pedindo TS do %s para Tx [%s]\n", c.channelID, outroCanal, txID)

			for i := 0; i < 15; i++ {
				resp, err := http.Post(url, "application/json", bytes.NewBuffer(reqData))
				if err == nil {
					var result struct {
						MaxTS uint64 `json:"max_ts"`
						Found bool   `json:"found"`
					}
					if err := json.NewDecoder(resp.Body).Decode(&result); err == nil {
						if result.Found {
							fmt.Printf("📩 [ALL-TO-ALL] Shard %s recebeu TS: %d do %s para Tx [%s]\n", c.channelID, result.MaxTS, outroCanal, txID)
							if result.MaxTS > finalTS {
								finalTS = result.MaxTS
							}
							resp.Body.Close()
							break
						}
					}
					resp.Body.Close()
				}
				time.Sleep(time.Duration(20*(i+1)) * time.Millisecond)
			}
		}
	}

	fmt.Printf("🔄 [ALL-TO-ALL Shard %s] CONSENSO INDEPENDENTE ATINGIDO: %s | TS Final: %d\n", c.channelID, txID, finalTS)
	c.FinalizeAndDeliver(txID, finalTS)
}

func (c *chain) FinalizeAndDeliver(txID string, finalTS uint64) {
	c.mutex.Lock()

	if finalTS > c.lamportClock {
		c.lamportClock = finalTS
		fmt.Printf("⏱️ [SYNC] Shard %s adiantou o relógio interno para %d\n", c.channelID, c.lamportClock)
	}

	tx, exists := c.pendingQueue[txID]
	if !exists {
		c.mutex.Unlock()
		return
	}

	c.localTSHistory[txID] = tx.TSLocal
	envelopeParaBlockCutter := tx.Envelope
	delete(c.pendingQueue, txID)
	c.mutex.Unlock()

	c.blockMutex.Lock()
	defer c.blockMutex.Unlock()

	batches, _ := c.support.BlockCutter().Ordered(envelopeParaBlockCutter)

	for _, batch := range batches {
		blocoSkeen := c.support.CreateNextBlock(batch)
		c.support.WriteBlock(blocoSkeen, nil)

		// 🚨 MÉTRICA REAL PARA A TESE 🚨
		metricsMutex.Lock()
		decorrido := time.Since(startTime).Seconds()
		tpsReal := float64(txCount) / decorrido
		fmt.Printf("\n[MÉTRICA SKEEN %s] 🧱 Bloco %d Gravado | Total Txs: %d | Tempo: %.2fs | TPS REAL: %.2f\n\n",
			c.channelID, blocoSkeen.Header.Number, txCount, decorrido, tpsReal)
		metricsMutex.Unlock()
	}

}

// ==========================================
// CAMADA DE REDE (API HTTP)
// ==========================================

func startNetworkServer() {
	fabricPortStr := os.Getenv("ORDERER_GENERAL_LISTENPORT")
	if fabricPortStr == "" {
		fabricPortStr = "7050"
	}
	fabricPort, _ := strconv.Atoi(fabricPortStr)
	skeenPort := fabricPort + 10000

	http.HandleFunc("/skeen/exchange_ts", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TxID      string `json:"tx_id"`
			ChannelID string `json:"channel_id"`
		}
		json.NewDecoder(r.Body).Decode(&req)

		globalRegistry.mutex.RLock()
		c, exists := globalRegistry.chains[req.ChannelID]
		globalRegistry.mutex.RUnlock()

		var currentTS uint64 = 0
		found := false

		if exists {
			c.mutex.Lock()
			if tx, ok := c.pendingQueue[req.TxID]; ok {
				currentTS = tx.TSLocal
				found = true
			} else if histTS, ok := c.localTSHistory[req.TxID]; ok {
				currentTS = histTS
				found = true
			}
			c.mutex.Unlock()
		}

		if found {
			fmt.Printf("📨 [FEEDBACK API] Shard %s devolvendo TS %d para quem pediu. Tx [%s]\n", req.ChannelID, currentTS, req.TxID)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"max_ts": currentTS,
			"found":  found,
		})
	})

	fmt.Printf("[SKEEN NETWORK LAYER] API HTTP All-to-All ativa na porta: %d\n", skeenPort)
	http.ListenAndServe(fmt.Sprintf(":%d", skeenPort), nil)
}

func getPortForChannel(channelID string) int {
	switch channelID {
	case "canal1":
		return 17050
	case "canal2":
		return 18050
	case "canal3":
		return 19050
	case "canal4":
		return 20050
	}
	return 0
}
