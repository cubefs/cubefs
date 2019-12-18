package objectnode

type authnodeStore struct {
	vm *volumeManager //vol *volume
}

func (s *authnodeStore) Init(vm *volumeManager) {
	s.vm = vm
	//TODO: init authnode store
}

func (s *authnodeStore) Get(vol, path, key string) (val []byte, err error) {

	return
}

func (s *authnodeStore) Put(vol, path, key string, data []byte) (err error) {
	//TODO: implement authonode store put method

	return nil
}

func (s *authnodeStore) Delete(vol, path, key string) (err error) {
	//TODO: implement authonode store put method

	return
}

func (s *authnodeStore) List(vol, path string) (data [][]byte, err error) {
	//TODO: implement authonode store list method

	return
}
