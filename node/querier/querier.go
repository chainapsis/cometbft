package querier

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"

	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
	"google.golang.org/grpc"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

var (
	errBlockNotFound = fmt.Errorf("block_not_found")
)

type CosmosQuerier struct {
	BlockStore *store.BlockStore
	StateStore sm.Store
}

func (q *CosmosQuerier) mustEmbedUnimplementedCosmosIndexerServer() {
	panic("Forward-compatibility: Unknown method!")
}

func (q *CosmosQuerier) ListenAndServe(port string) error {
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return err
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	RegisterCosmosIndexerServer(grpcServer, q)
	return grpcServer.Serve(lis)
}

func (q *CosmosQuerier) GetBlock(ctx context.Context, request *GetBlockRequest) (*GetBlockResponse, error) {
	b, err := q.getBlockFromLocal(request.Height)
	if err != nil {
		return nil, err
	}

	return &GetBlockResponse{
		Block: b,
	}, nil
}

func (q *CosmosQuerier) GetBlockStreamFrom(stream CosmosIndexer_GetBlockStreamFromServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
		}

		req, err := stream.Recv()

		if err == io.EOF {
			fmt.Println("stream ended")
			return nil
		}
		if err != nil {
			fmt.Printf("Stream Receive Error: %s\n", err.Error())
			continue
		}

		block, err := q.getBlockFromLocal(req.Height)
		var res *GetBlockResponse
		if err != nil {
			fmt.Printf("Get Block From Local Error: %s\n", err.Error())
			res = &GetBlockResponse{
				Block:     nil,
				IsPresent: false,
			}
		} else {
			res = &GetBlockResponse{
				Block:     block,
				IsPresent: true,
			}
		}
		if err = stream.Send(res); err != nil {
			fmt.Printf("Block Send Error: %s\n", err.Error())
		}
		if res.IsPresent {
			fmt.Printf("Block Send Height: %d\n", res.Block.Height)
		} else {
			fmt.Printf("No Block On Height: %d\n", req.Height)
		}
	}

	return nil
}

func (q *CosmosQuerier) getBlockFromLocal(height int64) (*Block, error) {
	tmBlock := q.BlockStore.LoadBlock(height)
	if tmBlock == nil {
		return nil, errBlockNotFound
	}
	abciResponse, err := q.StateStore.LoadABCIResponses(height)
	if err != nil {
		return nil, err
	}

	var txs []*Tx

	for i, tx := range tmBlock.Data.Txs {
		txr := abciResponse.DeliverTxs[i]
		txHash := fmt.Sprintf("%X", tx.Hash())
		pbTx := Tx{
			TxHash:    txHash,
			Code:      txr.Code,
			Log:       "",
			Info:      txr.Info,
			GasWanted: txr.GasWanted,
			GasUsed:   txr.GasUsed,
			Codespace: txr.Codespace,
			TxBytes:   tx,
		}
		txs = append(txs, &pbTx)
	}

	return &Block{Height: height, Txs: txs, Time: timestamppb.New(tmBlock.Time)}, nil
}
