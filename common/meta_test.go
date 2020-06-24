package common_test

import (
	"encoding/json"
	"testing"

	"github.com/eyeKill/KV/common"
	"github.com/stretchr/testify/assert"
)

func TestMarshalMasterNode(t *testing.T) {
	ast := assert.New(t)
	node := common.MasterNode{Host: common.Node{
		Hostname: "localhost",
		Port:     1234,
	}}
	b, err := json.Marshal(&node)
	ast.Nil(err)
	n, err := common.UnmarshalNode(b)
	ast.Nil(err)
	switch n.(type) {
	case *common.MasterNode:
		node := n.(*common.MasterNode)
		ast.Equal("localhost", node.Host.Hostname)
		ast.Equal(uint16(1234), node.Host.Port)
	default:
		t.Errorf("Failed, should be %T, found %T", node, n)
	}
}

func TestMarshalPrimaryWorkerNode(t *testing.T) {
	ast := assert.New(t)
	node := common.PrimaryWorkerNode{
		Id:     1,
		Host:   common.Node{},
		Weight: 0,
		Status: "",
	}
	b, err := json.Marshal(&node)
	ast.Nil(err)
	n, err := common.UnmarshalNode(b)
	ast.Nil(err)
	switch n.(type) {
	case *common.PrimaryWorkerNode:
		node := n.(*common.PrimaryWorkerNode)
		ast.Equal(common.WorkerId(1), node.Id)
	default:
		t.Errorf("Failed, should be %T, found %T", node, n)
	}
}

func TestMarshalNode(t *testing.T) {
	ast := assert.New(t)
	node := common.BackupWorkerNode{
		Id:     1,
		Host:   common.Node{},
		Status: "",
	}
	b, err := json.Marshal(&node)
	ast.Nil(err)
	n, err := common.UnmarshalNode(b)
	ast.Nil(err)
	switch n.(type) {
	case *common.BackupWorkerNode:
		node := n.(*common.BackupWorkerNode)
		ast.Equal(common.WorkerId(1), node.Id)
	default:
		t.Errorf("Failed, should be %T, found %T", node, n)
	}
}
