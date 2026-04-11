package skeen

import (
	"fmt"
	"sort" // NOVO: Para ordenar a fila pelo TS_Final
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
	// 1. Extração do ID (Pode ser feito sem travar a memória)
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		return fmt.Errorf("SKEEN: erro ao abrir payload: %v", err)
	}

	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return fmt.Errorf("SKEEN: erro ao abrir cabeçalho: %v", err)
	}

	txID := chdr.TxId
	if txID == "" {
		txID = "TX_SISTEMA_INTERNO"
	}

	// 2. Incremento do Relógio e Fila
	c.mutex.Lock()
	c.lamportClock++
	meuTSLocal := c.lamportClock

	// NOVO: Inicializamos a transação com o mapa de ReceivedTS
	c.pendingQueue[txID] = &PendingTx{
		TxID:       txID, // NOVO
		Envelope:   env,
		TSLocal:    meuTSLocal,
		IsFinal:    false,
		ReceivedTS: make(map[string]uint64),
	}
	fmt.Printf("[SKEEN Canal %s] Tx [%s] recebida. TS Local: %d\n", c.channelID, txID, meuTSLocal)
	c.mutex.Unlock()

	// 3. O Multicast do Skeen (Conversando com outros canais)
	// Lemos a lista de canais do Roteador Global (RLock apenas para leitura)
	globalRegistry.mutex.RLock()
	for id, otherChain := range globalRegistry.chains {
		// Não manda mensagem para si mesmo
		if id != c.channelID {
			// Dispara uma 'Goroutine' (thread em 2º plano) para simular o envio pela rede!
			go otherChain.ReceiveMulticast(txID, c.channelID, meuTSLocal)
		}
	}
	globalRegistry.mutex.RUnlock()

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

	// 1. Atualiza o relógio lógico de Lamport (regra do MAX)
	if senderTSLocal > c.lamportClock {
		c.lamportClock = senderTSLocal
	}

	// 2. Busca a transação na nossa fila
	tx, exists := c.pendingQueue[txID]
	if !exists {
		// Em um sistema real, a rede pode ter atrasos. Se a Tx chegou via multicast antes
		// do cliente enviá-la para nós, deveríamos colocar num buffer de espera.
		// Para o protótipo, vamos apenas registrar o aviso.
		return
	}

	// 3. Guarda o Timestamp que o outro canal nos enviou
	tx.ReceivedTS[senderChannel] = senderTSLocal
	fmt.Printf("[SKEEN Canal %s] Multicast anotado: Canal %s enviou TS %d para Tx [%s]\n", c.channelID, senderChannel, senderTSLocal, txID)

	// 4. Tenta ver se já temos votos suficientes para decidir o TS_Final
	c.tryFinalizeTx(txID, tx)
}

// tryFinalizeTx verifica se todos os canais já responderam
// NOTA: Esta função assume que c.mutex já está travado pela função que a chamou!
func (c *chain) tryFinalizeTx(txID string, tx *PendingTx) {
	// Quantos canais existem na rede inteira?
	globalRegistry.mutex.RLock()
	totalChannels := len(globalRegistry.chains)
	globalRegistry.mutex.RUnlock()

	// O total de respostas é o nosso próprio TS (1) + os que estão no mapa
	totalRespostas := len(tx.ReceivedTS) + 1

	if totalRespostas >= totalChannels {
		// Chegaram todos! Vamos aplicar a matemática do Skeen (TS_Final = MAX de todos os TS)
		maxTS := tx.TSLocal
		for _, ts := range tx.ReceivedTS {
			if ts > maxTS {
				maxTS = ts
			}
		}

		// Grava a decisão final
		tx.TSFinal = maxTS
		tx.IsFinal = true
		fmt.Printf("[SKEEN Canal %s] === CONSENSO ATINGIDO === Tx [%s] cravada com TS Final: %d\n", c.channelID, txID, maxTS)

		// NOVO: Chama a fila para processar, ordenar e gerar os blocos!
		c.processQueue()

		// TODO (Fase 5): Reordenar a c.pendingQueue baseada no TSFinal e enviar ao BlockCutter!
	}
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
