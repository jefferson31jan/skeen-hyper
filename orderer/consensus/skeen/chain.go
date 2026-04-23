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
	blockMutex     sync.Mutex // Cadeado exclusivo para gravação no HD
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

	// 1. LÊ O ROTEAMENTO DO CLIENTE NO TxID (Ex: CROSS_canal3-canal1_BENCH_...)
	crossChans := []string{c.channelID}
	if strings.HasPrefix(txID, "CROSS_") {
		partes := strings.Split(txID, "_")
		if len(partes) >= 2 {
			crossChans = strings.Split(partes[1], "-")
		}
	}

	c.mutex.Lock()
	// 2. PROTEÇÃO DE IDEMPOTÊNCIA E SPLIT-BRAIN
	if _, exists := c.pendingQueue[txID]; exists {
		c.mutex.Unlock()
		return nil
	}
	if _, exists := c.localTSHistory[txID]; exists {
		c.mutex.Unlock()
		return nil
	}

	// 3. GERAÇÃO DO RELÓGIO LÓGICO
	c.lamportClock++
	meuTSLocal := c.lamportClock

	c.pendingQueue[txID] = &PendingTx{
		TxID:          txID,
		Envelope:      env,
		TSLocal:       meuTSLocal,
		CrossChannels: crossChans,
	}
	c.mutex.Unlock()

	// 4. LÍDER DINÂMICO (LOAD BALANCING): O primeiro canal da string é o Coordenador
	isCoordinator := (len(crossChans) > 0 && crossChans[0] == c.channelID)

	var role string
	if isCoordinator {
		role = "COORDENADOR"
	} else {
		role = "SEGUIDOR"
	}
	fmt.Printf("[SKEEN Canal %s] Tx [%s] recebida. TS: %d | Papel: %s\n", c.channelID, txID, meuTSLocal, role)

	// 5. EXECUÇÃO DO PAPEL
	if isCoordinator && len(crossChans) > 1 {
		// O Coordenador vai buscar os relógios dos seguidores de forma assíncrona
		go c.tryFinalizeTx(txID, meuTSLocal, crossChans)
	} else if len(crossChans) == 1 {
		// Se for Intra-Shard, finaliza e entrega para o BlockCutter imediatamente
		c.FinalizeAndDeliver(txID, meuTSLocal)
	}
	// Se for Seguidor em uma transação Cross-Shard, aguarda o /commit via HTTP

	return nil
}

// ==========================================
// FASE 2: COORDENAÇÃO E FINALIZAÇÃO
// ==========================================

func (c *chain) tryFinalizeTx(txID string, maxTSLocal uint64, crossChannels []string) {
	finalTS := maxTSLocal

	if len(crossChannels) > 1 {
		// 🚀 OTIMIZAÇÃO: Sem 'Sleep(30ms)' aqui. Active Polling direto para máxima velocidade.

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

			fmt.Printf("📡 [FEEDBACK] Coordenador %s solicitando TS do %s via HTTP para Tx [%s]\n", c.channelID, outroCanal, txID)

			// LOOP DE RETRY BLINDADO
			for i := 0; i < 15; i++ {
				resp, err := http.Post(url, "application/json", bytes.NewBuffer(reqData))
				if err == nil {
					var result struct {
						MaxTS uint64 `json:"max_ts"`
						Found bool   `json:"found"`
					}
					if err := json.NewDecoder(resp.Body).Decode(&result); err == nil {
						if result.Found {
							fmt.Printf("📩 [FEEDBACK] Coordenador %s recebeu TS Local: %d do %s para Tx [%s]\n", c.channelID, result.MaxTS, outroCanal, txID)

							if result.MaxTS > finalTS {
								finalTS = result.MaxTS
							}
							resp.Body.Close()
							break // Achou o voto definitivo
						}
					}
					resp.Body.Close()
				}
				// Backoff exponencial suave caso o nó ainda não tenha recebido a transação
				time.Sleep(time.Duration(20*(i+1)) * time.Millisecond)
			}
		}
	}

	fmt.Printf("🏆 [COORDENADOR Shard %s] CONSENSO ATINGIDO: %s | TS Final: %d\n", c.channelID, txID, finalTS)

	// Envia a ordem de COMMIT para os seguidores (com o FinalTS correto!)
	commitData, _ := json.Marshal(map[string]interface{}{
		"tx_id": txID, "channel_id": c.channelID, "final_ts": finalTS,
	})

	fmt.Printf("📢 [FEEDBACK] Coordenador %s enviando ordem de COMMIT Global para os seguidores na Tx [%s]\n", c.channelID, txID)

	for _, ch := range crossChannels {
		if ch == c.channelID {
			continue
		}
		port := getPortForChannel(ch)
		if port != 0 {
			go http.Post(fmt.Sprintf("http://127.0.0.1:%d/skeen/commit", port), "application/json", bytes.NewBuffer(commitData))
		}
	}

	// O Coordenador finaliza e entrega a sua própria cópia da transação
	c.FinalizeAndDeliver(txID, finalTS)
}

func (c *chain) FinalizeAndDeliver(txID string, finalTS uint64) {
	c.mutex.Lock()

	// 🚨 CORREÇÃO ALGORITMO 1 (LINHA 16): Sincronização do Relógio de Lamport
	// Se a rede chegou a um consenso sobre um tempo futuro, este nó é "puxado" para esse tempo.
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
	c.mutex.Unlock() // Libera a memória rápida do Skeen

	// -------------------------------------------------------------
	// 🚀 ZONA DE EXCLUSÃO MÚTUA DE GRAVAÇÃO (FILA INDIANA PARA O HD)
	// -------------------------------------------------------------
	c.blockMutex.Lock()
	defer c.blockMutex.Unlock()

	batches, _ := c.support.BlockCutter().Ordered(envelopeParaBlockCutter)

	for _, batch := range batches {
		blocoSkeen := c.support.CreateNextBlock(batch)
		c.support.WriteBlock(blocoSkeen, nil)
		// Para reduzir o ruído no terminal com 10.000 TPS, este log final pode ser comentado depois
		fmt.Printf("[SKEEN Canal %s] 🧱 *** BLOCO [%d] GRAVADO NO DISCO (Lote cheio: %d txs) ***\n", c.channelID, blocoSkeen.Header.Number, len(batch))
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
			fmt.Printf("📨 [FEEDBACK API] Shard %s respondendo solicitação de TS com valor %d para Tx [%s]\n", req.ChannelID, currentTS, req.TxID)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"max_ts": currentTS,
			"found":  found,
		})
	})

	http.HandleFunc("/skeen/commit", func(w http.ResponseWriter, r *http.Request) {
		var msg struct {
			TxID      string `json:"tx_id"`
			ChannelID string `json:"channel_id"`
			FinalTS   uint64 `json:"final_ts"` // 🚨 LÊ O TIMESTAMP FINAL DO COORDENADOR
		}
		json.NewDecoder(r.Body).Decode(&msg)

		fmt.Printf("✅ [FEEDBACK API] Shard(s) local(is) recebendo ordem de COMMIT Global para Tx [%s] com TS Final %d\n", msg.TxID, msg.FinalTS)

		globalRegistry.mutex.RLock()
		for _, c := range globalRegistry.chains {
			// 🚨 PASSA O FINALTS REAL PARA QUE O SEGUIDOR POSSA SINCRONIZAR O SEU RELÓGIO
			c.FinalizeAndDeliver(msg.TxID, msg.FinalTS)
		}
		globalRegistry.mutex.RUnlock()

		w.WriteHeader(http.StatusOK)
	})

	fmt.Printf("[SKEEN NETWORK LAYER] API HTTP ativa na porta: %d\n", skeenPort)
	http.ListenAndServe(fmt.Sprintf(":%d", skeenPort), nil)
}

// ==========================================
// FUNÇÕES AUXILIARES
// ==========================================

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
