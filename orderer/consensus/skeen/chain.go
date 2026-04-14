package skeen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/protoutil"
)

// PendingTx representa uma transação que está aguardando o consenso do Skeen
type PendingTx struct {
	TxID       string // NOVO: Guarda o ID para podermos deletar da fila depois
	Envelope   *common.Envelope
	TSLocal    uint64
	TSFinal    uint64
	IsFinal    bool
	ReceivedTS map[string]uint64 // NOVO: Guarda os TS recebidos (CanalID -> TS)
}

// chain implementa a interface consensus.Chain do Fabric para um canal específico.
type chain struct {
	support   consensus.ConsenterSupport
	channelID string

	// --- Motor Matemático do Skeen ---
	mutex        sync.Mutex            // Protege as variáveis abaixo contra concorrência
	lamportClock uint64                // Relógio Lógico de Lamport local deste canal
	pendingQueue map[string]*PendingTx // Buffer: Fila de transações pendentes (ID -> Tx)
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

	c.mutex.Lock()
	c.lamportClock++
	meuTSLocal := c.lamportClock

	c.pendingQueue[txID] = &PendingTx{
		TxID:       txID,
		Envelope:   env,
		TSLocal:    meuTSLocal,
		IsFinal:    false,
		ReceivedTS: make(map[string]uint64),
	}
	fmt.Printf("[SKEEN Canal %s] Tx [%s] recebida. TS Local: %d\n", c.channelID, txID, meuTSLocal)
	c.mutex.Unlock()

	// === NOVO: DISPARO DE MULTICAST VIA REDE HTTP ===
	// === NOVO: O COORDENADOR COLETANDO OS VOTOS (Multicast Skeen) ===
	msg := MulticastMsg{
		TxID:          txID,
		SenderNode:    "Coordinator",
		SenderTSLocal: meuTSLocal,
	}
	payloadJSON, _ := json.Marshal(msg)

	peers := []string{
		"http://127.0.0.1:17050", // Nó 1 (ele vota também)
		"http://127.0.0.1:18050", // Nó 2
		"http://127.0.0.1:19050", // Nó 3
		"http://127.0.0.1:20050", // Nó 4
	}

	for i, peer := range peers {
		nodeName := fmt.Sprintf("Orderer-%d", i+1)
		// Dispara requisições e espera as respostas JSON
		go func(url string, name string) {
			resp, err := http.Post(url+"/skeen/multicast", "application/json", bytes.NewBuffer(payloadJSON))
			if err == nil {
				defer resp.Body.Close()
				var result map[string]uint64
				json.NewDecoder(resp.Body).Decode(&result)

				// Recebeu o voto do nó remoto! Entrega pra caixa de correio
				remoteTS := result["ts"]
				c.ReceiveMulticast(txID, name, remoteTS)
			}
		}(peer, nodeName)
	}

	return nil
}

// Configure é chamado para transações de configuração do canal.
func (c *chain) Configure(env *common.Envelope, configSeq uint64) error {
	return nil
}

func (c *chain) WaitReady() error {
	return nil
}

func (c *chain) Errored() <-chan struct{} {
	return nil
}

func (c *chain) Start() {
	fmt.Printf("[SKEEN] Nó iniciado para o canal: %s\n", c.channelID)
}

func (c *chain) Halt() {
	fmt.Printf("[SKEEN] Nó parado para o canal: %s\n", c.channelID)
}

// ReceiveMulticast é a "caixa de correio" chamada por outros canais do Skeen.
// Ele recebe o ID da transação e o TS Local gerado pelo outro canal.
func (c *chain) ReceiveMulticast(txID string, senderChannel string, senderTSLocal uint64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	tx, exists := c.pendingQueue[txID]
	if !exists {
		return
	} // Ignora se não for o Coordenador

	// Guarda o voto usando o nome real do Orderer para não sobrescrever!
	tx.ReceivedTS[senderChannel] = senderTSLocal
	fmt.Printf("[SKEEN Canal %s] Voto recebido: %s respondeu com TS %d para Tx [%s]\n", c.channelID, senderChannel, senderTSLocal, txID)

	c.tryFinalizeTx(txID, tx)
}

// tryFinalizeTx verifica se todos os canais já responderam
// NOTA: Esta função assume que c.mutex já está travado pela função que a chamou!
// tryFinalizeTx verifica se todos os canais já responderam
func (c *chain) tryFinalizeTx(txID string, tx *PendingTx) {
	if len(tx.ReceivedTS) < 4 {
		return
	} // Espera os 4 votos

	// 1. Calcula o TS Máximo
	var maxTS uint64
	for _, ts := range tx.ReceivedTS {
		if ts > maxTS {
			maxTS = ts
		}
	}

	fmt.Printf("\n======================================================\n")
	fmt.Printf("🏆 [COORDENADOR] CONSENSO ATINGIDO: %s | TS Final: %d\n", txID, maxTS)
	fmt.Printf("======================================================\n")

	// ====================================================================
	// A PONTE ENTRE O SKEEN E O FABRIC (GRAVAÇÃO NO DISCO)
	// ====================================================================

	// ====================================================================
	// A PONTE ENTRE O SKEEN E O FABRIC (GRAVAÇÃO NO DISCO)
	// ====================================================================

	// 1. Pedimos ao Fabric para empacotar a transação em um Bloco oficial
	blocoSkeen := c.support.CreateNextBlock([]*common.Envelope{tx.Envelope})

	// 2. Mandamos o Fabric gravar o bloco fisicamente no Ledger (SSD)
	c.support.WriteBlock(blocoSkeen, nil)

	fmt.Printf("[SKEEN] *** BLOCO GERADO E GRAVADO COM SUCESSO NO LEDGER! ***\n")

	// 2. Notifica todos os nós (inclusive ele mesmo) via Commit
	peers := []string{"http://127.0.0.1:17050", "http://127.0.0.1:18050", "http://127.0.0.1:19050", "http://127.0.0.1:20050"}
	commitData, _ := json.Marshal(map[string]interface{}{
		"tx_id":    txID,
		"final_ts": maxTS,
	})

	for _, url := range peers {
		go http.Post(url+"/skeen/commit", "application/json", bytes.NewBuffer(commitData))
	}
}

// Nova função que será chamada pelo Commit em todos os nós
func (c *chain) FinalizeAndDeliver(txID string, finalTS uint64) {
	c.mutex.Lock()
	tx, exists := c.pendingQueue[txID]
	if !exists {
		c.mutex.Unlock()
		return
	}
	delete(c.pendingQueue, txID)
	c.mutex.Unlock()

	// Agora SIM todos os nós chamam o corte do bloco
	fmt.Printf("[SKEEN] Entregando Tx [%s] ao Ledger Local via BlockCutter\n", txID)
	// c.support.WriteConfigBlock(tx.Envelope, nil) // Ou a chamada correta do seu logger

	// 1. Criamos um novo bloco contendo apenas a nossa transação Skeen
	blocoSkeen := c.support.CreateNextBlock([]*common.Envelope{tx.Envelope})

	// 2. Gravamos o bloco fisicamente no Ledger (disco)
	c.support.WriteBlock(blocoSkeen, nil)

	fmt.Printf("\n[SKEEN Canal %s] *** NOVO BLOCO GRAVADO COM SUCESSO! ***\n", c.channelID)

}

// processQueue varre a fila, ordena pelo TS_Final e cria os blocos
func (c *chain) processQueue() {
	var readyTxs []*PendingTx

	// 1. Separa apenas as transações que já atingiram consenso
	for _, tx := range c.pendingQueue {
		if tx.IsFinal {
			readyTxs = append(readyTxs, tx)
		}
	}

	// Se não tem nada pronto, sai da função
	if len(readyTxs) == 0 {
		return
	}

	// 2. Ordena as transações pelo TS_Final (do menor para o maior)
	sort.Slice(readyTxs, func(i, j int) bool {
		return readyTxs[i].TSFinal < readyTxs[j].TSFinal
	})

	// 3. Entrega as transações ordenadas para o BlockCutter do Fabric
	for _, tx := range readyTxs {
		fmt.Printf("[SKEEN Canal %s] Entregando Tx [%s] ao BlockCutter (TS_Final: %d)\n", c.channelID, tx.TxID, tx.TSFinal)

		// O BlockCutter diz se já deu o limite de transações para fechar um bloco
		messageBatches, _ := c.support.BlockCutter().Ordered(tx.Envelope)

		for _, batch := range messageBatches {
			// Cria o bloco com as transações agrupadas
			block := c.support.CreateNextBlock(batch)

			// Escreve o bloco fisicamente no canal (O SUCESSO DO CONSENSO!)
			c.support.WriteBlock(block, nil)
			fmt.Printf("[SKEEN Canal %s] *** NOVO BLOCO GRAVADO *** (Número: %d)\n", c.channelID, block.Header.Number)
		}

		// Remove a transação da nossa fila pendente, pois já foi processada
		delete(c.pendingQueue, tx.TxID)
	}
}
