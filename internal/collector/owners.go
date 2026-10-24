package collector

import "container/list"

// ownerCache remembers resolved owners, dropping the least recently used beyond its size, so a
// cluster with more ReplicaSets and Jobs than it holds still keeps its busy owners resolved. It is
// not safe for concurrent use.
type ownerCache struct {
	size  int
	items map[string]*list.Element
	order *list.List
}

type ownerEntry struct {
	key   string
	owner owner
}

func newOwnerCache(size int) *ownerCache {
	return &ownerCache{size: size, items: map[string]*list.Element{}, order: list.New()}
}

func (c *ownerCache) get(key string) (owner, bool) {
	e, ok := c.items[key]
	if !ok {
		return owner{}, false
	}
	c.order.MoveToFront(e)
	return e.Value.(*ownerEntry).owner, true
}

func (c *ownerCache) put(key string, o owner) {
	if e, ok := c.items[key]; ok {
		e.Value.(*ownerEntry).owner = o
		c.order.MoveToFront(e)
		return
	}
	c.items[key] = c.order.PushFront(&ownerEntry{key: key, owner: o})
	for c.order.Len() > c.size {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.items, oldest.Value.(*ownerEntry).key)
	}
}
