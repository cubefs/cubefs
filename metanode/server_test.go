package metanode

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/btree"
	"testing"
)

func TestMetaNode_startServer(t *testing.T) {

	tree := btree.New(16)

	_ = tree.ReplaceOrInsert(&Inode{
		Inode: 1,
		Type:  2,
		Extents: &SortedExtents{
			eks: []proto.ExtentKey{
				proto.ExtentKey{
					FileOffset: 0,
				},
			},
		},
	})

	_ = tree.ReplaceOrInsert(&Inode{
		Inode: 1,
		Type:  20,
		Extents: &SortedExtents{
			eks: []proto.ExtentKey{
				proto.ExtentKey{
					FileOffset: 110,
				},
			},
		},
	})

	println(tree.Len())

	tree2 := tree.Clone()

	fmt.Println("================================================= first get  ")

	get := tree.CopyGet(&Inode{
		Inode: 1,
	})
	println(get.(*Inode).Type)
	println("eklen", len(get.(*Inode).Extents.eks))
	println("file off", get.(*Inode).Extents.eks[0].FileOffset)

	fmt.Println("================================================= modify ")
	get.(*Inode).Type = 3
	get.(*Inode).Extents.eks[0].FileOffset = 10
	get.(*Inode).Extents.eks = append(get.(*Inode).Extents.eks, proto.ExtentKey{
		FileOffset: 0,
	})
	println(get.(*Inode).Type)
	println("eklen", len(get.(*Inode).Extents.eks))
	println("file off", get.(*Inode).Extents.eks[0].FileOffset)

	fmt.Println("================================================= use old ")
	get2 := tree.Get(&Inode{
		Inode: 1,
	})
	println(get2.(*Inode).Type)
	println("eklen", len(get2.(*Inode).Extents.eks))
	println("file off", get2.(*Inode).Extents.eks[0].FileOffset)

	fmt.Println("================================================= use clone ")

	get3 := tree2.Get(&Inode{
		Inode: 1,
	})
	println(get3.(*Inode).Type)
	println("eklen", len(get3.(*Inode).Extents.eks))
	println("file off", get3.(*Inode).Extents.eks[0].FileOffset)
}
