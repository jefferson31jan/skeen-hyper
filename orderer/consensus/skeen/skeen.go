package skeen

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/orderer/consensus"
)

// SkeenRegistry mantém apenas as chains *locais* deste Orderer
type SkeenRegistry struct {
	mutex  sync.RWMutex
	chains map[string]*chain
}

var globalRegistry = &SkeenRegistry{
	chains: make(map[string]*chain),
}

// === NOVO: ESTRUTURAS DE REDE ===
// MulticastMsg é o pacote que viaja pela rede (TCP/IP)
type MulticastMsg struct {
	TxID          string `json:"tx_id"`
	SenderNode    string `json:"sender_node"`
	SenderTSLocal uint64 `json:"sender_ts_local"`
}

var serverOnce sync.Once

// SkeenConsenter
type skeenConsenter struct{}

func New() consensus.Consenter {
	// Garante que o servidor web do Skeen só suba UMA vez por nó
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

// === NOVO: O CORAÇÃO DA REDE ===
// Este servidor escuta as mensagens dos outros nós 
func startNetworkServer() {
	fabricPortStr := os.Getenv("ORDERER_GENERAL_LISTENPORT")
	if fabricPortStr == "" { fabricPortStr = "7050" }
	fabricPort, _ := strconv.Atoi(fabricPortStr)
	skeenPort := fabricPort + 10000

	http.HandleFunc("/skeen/multicast", func(w http.ResponseWriter, r *http.Request) {
		var msg MulticastMsg
		json.NewDecoder(r.Body).Decode(&msg)

		globalRegistry.mutex.RLock()
		c, exists := globalRegistry.chains["canal1"]
		globalRegistry.mutex.RUnlock()

		var tsResponse uint64 = 0
		if exists {
			c.mutex.Lock()
			// Regra do Skeen: Atualiza com o TS do Coordenador e incrementa
			if msg.SenderTSLocal > c.lamportClock {
				c.lamportClock = msg.SenderTSLocal
			}
			c.lamportClock++
			tsResponse = c.lamportClock
			c.mutex.Unlock()
		}

		// Retorna o Relógio Lógico deste nó para o Coordenador
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]uint64{"ts": tsResponse})
	})

	fmt.Printf("[SKEEN NETWORK LAYER] API HTTP rodando na porta: %d\n", skeenPort)
	http.ListenAndServe(fmt.Sprintf(":%d", skeenPort), nil)

	// Nova rota para receber o veredito final do Coordenador
	http.HandleFunc("/skeen/commit", func(w http.ResponseWriter, r *http.Request) {
		var msg struct {
			TxID    string `json:"tx_id"`
			FinalTS uint64 `json:"final_ts"`
		}
		json.NewDecoder(r.Body).Decode(&msg)

		globalRegistry.mutex.RLock()
		c, exists := globalRegistry.chains["canal1"]
		globalRegistry.mutex.RUnlock()

		if exists {
			fmt.Printf("[SKEEN] Commit recebido para Tx [%s] -> TS Final: %d\n", msg.TxID, msg.FinalTS)
			// Aqui cada nó chama a entrega oficial no seu próprio ledger
			c.FinalizeAndDeliver(msg.TxID, msg.FinalTS)
		}
		w.WriteHeader(http.StatusOK)
	})
}