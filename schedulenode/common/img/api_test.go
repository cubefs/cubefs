package img

import (
	"fmt"
	"testing"
)

func TestGetClusterSpace(t *testing.T) {
	space, err := GetClusterSpace("imgmaster.jd.local", "ds_image")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(*space)
}

func TestGetClusterVolView(t *testing.T) {
	imgClusterView, err := GetClusterVolView("imgmaster.jd.local", "ds_image")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(len(imgClusterView.Vols))
}
