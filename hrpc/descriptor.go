package hrpc

import (
	"context"
	"github.com/tsuna/gohbase/pb"
	"google.golang.org/protobuf/proto"
)

type TableDescriptors struct {
	base
	tableNames       []string
	regex            string
	includeSysTables bool
	namespace        string
}

// NewGetTableDescriptors creates a new GetTableNames request that will list tables in hbase.
//
// By default matchs all tables. Use the options (ListRegex, ListNamespace, ListSysTables) to
// set non default behaviour.
func NewGetTableDescriptors(ctx context.Context, opts ...func(Call) error) (*TableDescriptors, error) {
	td := &TableDescriptors{
		base: base{
			ctx:      ctx,
			resultch: make(chan RPCResult, 1),
		},
		regex: ".*",
	}
	if err := applyOptions(td, opts...); err != nil {
		return nil, err
	}
	return td, nil
}

// Name returns the name of this RPC call.
func (tn *TableDescriptors) Name() string {
	return "GetTableDescriptors"
}

// Description returns the description of this RPC call.
func (tn *TableDescriptors) Description() string {
	return tn.Name()
}

// ToProto converts the RPC into a protobuf message.
func (tn *TableDescriptors) ToProto() proto.Message {
	pbTableNames := make([]*pb.TableName, len(tn.tableNames))
	ns := "default"
	if tn.namespace != "" {
		ns = tn.namespace
	}
	for _, tableName := range tn.tableNames {
		pbTableName := &pb.TableName{
			Namespace: []byte(ns),
			Qualifier: []byte(tableName),
		}
		pbTableNames = append(pbTableNames, pbTableName)
	}

	return &pb.GetTableDescriptorsRequest{
		TableNames:       pbTableNames,
		Regex:            proto.String(tn.regex),
		IncludeSysTables: proto.Bool(tn.includeSysTables),
		Namespace:        proto.String(tn.namespace),
	}
}

// NewResponse creates an empty protobuf message to read the response of this
// RPC.
func (tn *TableDescriptors) NewResponse() proto.Message {
	return &pb.GetTableDescriptorsResponse{}
}
