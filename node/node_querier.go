package node

import (
	"github.com/tendermint/tendermint/node/querier"
	sm "github.com/tendermint/tendermint/state"
	"github.com/tendermint/tendermint/store"
)

func setUpQuerier(blockStore *store.BlockStore, stateStore sm.Store, port string) error {
	q := querier.CosmosQuerier{
		BlockStore: blockStore,
		StateStore: stateStore,
	}
	return q.ListenAndServe(port)
}
