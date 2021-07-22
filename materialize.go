package kafkagomaterilization

type Cache interface {
	Put(partition int32, key, value []byte) (error, bool)

	DeletePartition(partition int32) (error, bool)
	DeleteWithPartition(partition int32, key []byte) (error, bool)
	Delete(key []byte) (error, bool)

	GetWithPartition(partition int32, key []byte) ([]byte, error)
	Get(key []byte) ([]byte, error)
}
