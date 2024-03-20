package node

import (
	"github.com/cometbft/cometbft/node/querier"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
)

func setUpQuerier(blockStore *store.BlockStore, stateStore sm.Store, port string) error {
	q := querier.CosmosQuerier{
		BlockStore: blockStore,
		StateStore: stateStore,
	}
	return q.ListenAndServe(port)
}
