package kafkagomaterilization

import (
	"crypto/sha256"
	"fmt"
)

type InMemoryCache struct {
	cache map[int32]InMemoryPartition
}

func (c InMemoryCache) getKey(key []byte) string {
	s := sha256.New()
	s.Write(key)
	sum := s.Sum(nil)
	return fmt.Sprintf("%x", sum)
}

func (c *InMemoryCache) Init() {
	if c.cache == nil {
		c.cache = make(map[int32]InMemoryPartition)
	}
}

func (c *InMemoryCache) GetPartitionOrCreate(partition int32) (InMemoryPartition, error) {
	c.Init()

	part, ok := c.cache[partition]
	if !ok {
		part.Init()
	}

	return part, nil
}

func (c *InMemoryCache) Put(partition int32, key, value []byte) (error, bool) {
	part, err := c.GetPartitionOrCreate(partition)
	if err != nil {
		return err, false
	}

	return part.Put(c.getKey(key), value)
}

func (c *InMemoryCache) DeletePartition(partition int32) (error, bool) {
	_, ok := c.cache[partition]
	if ok {
		delete(c.cache, partition)
	}

	return nil, ok
}
func (c *InMemoryCache) DeleteWithPartition(partition int32, key []byte) (error, bool) {
	part, err := c.GetPartitionOrCreate(partition)
	if err != nil {
		return err, false
	}

	return part.Delete(c.getKey(key))

}
func (c *InMemoryCache) Delete(key []byte) (error, bool) {
	c.Init()

	didDelete := false
	for _, v := range c.cache {
		err, ok := v.Delete(c.getKey(key))

		if ok {
			didDelete = true
		}

		if err != nil {
			return err, didDelete
		}
	}

	return nil, didDelete
}

func (c *InMemoryCache) GetWithPartition(partition int32, key []byte) ([]byte, error) {
	part, err := c.GetPartitionOrCreate(partition)
	if err != nil {
		return nil, err
	}

	return part.Get(c.getKey(key))
}
func (c *InMemoryCache) Get(key []byte) ([]byte, error) {
	c.Init()

	for _, v := range c.cache {
		val, err := v.Get(c.getKey(key))

		if err != nil {
			return val, err
		}

		if val != nil {
			return val, nil
		}
	}

	return nil, nil
}

type InMemoryPartition struct {
	cache map[string][]byte
}

func (c *InMemoryPartition) Init() {
	if c.cache == nil {
		c.cache = make(map[string][]byte)
	}
}

func (c *InMemoryPartition) Put(key string, value []byte) (error, bool) {
	c.cache[key] = value
	return nil, true
}
func (c *InMemoryPartition) Delete(key string) (error, bool) {
	if c.cache[key] != nil {
		delete(c.cache, key)
		return nil, true
	} else {
		return nil, false
	}
}
func (c *InMemoryPartition) Get(key string) ([]byte, error) {
	return c.cache[key], nil
}
