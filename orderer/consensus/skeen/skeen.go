package skeen

import (
	"fmt"
	"sync"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric/orderer/consensus"
)

// SkeenRegistry é o "roteador" global que permite aos canais conversarem
type SkeenRegistry struct {
	mutex  sync.RWMutex
	chains map[string]*chain // Mapeia o ID do Canal para a sua estrutura
}

// globalRegistry é a variável única (Singleton) compartilhada por todo o Orderer
var globalRegistry = &SkeenRegistry{
	chains: make(map[string]*chain),
}

type skeenConsenter struct{}

func New() consensus.Consenter {
	return &skeenConsenter{}
}

// HandleChain é chamado quando o canal nasce
func (s *skeenConsenter) HandleChain(support consensus.ConsenterSupport, metadata *common.Metadata) (consensus.Chain, error) {
	c := &chain{
		support:      support,
		channelID:    support.ChannelID(),
		lamportClock: 0,
		pendingQueue: make(map[string]*PendingTx),
	}

	// O canal acabou de nascer! Vamos registrá-lo no roteador global
	globalRegistry.mutex.Lock()
	globalRegistry.chains[support.ChannelID()] = c
	globalRegistry.mutex.Unlock()

	return c, nil
}

// IsChannelMember implementa a interface ClusterConsenter exigida pelo Fabric v3
func (s *skeenConsenter) IsChannelMember(joinBlock *common.Block) (bool, error) {
	fmt.Printf("\n[SKEEN] Validando permissão: O nó foi aceito como membro do canal dinâmico!\n")

	// Como o Skeen é um protótipo puro, vamos sempre retornar 'true'
	// indicando que este nó Orderer tem permissão total para entrar no canal.
	return true, nil
}
