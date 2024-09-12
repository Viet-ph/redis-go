package datastore

type Data struct {
	value any
	//hasExpiry bool
	//expiredAt time.Time
	//createdAt time.Time
}

func NewData(value any) *Data {
	return &Data{
		value: value,
		//createdAt: time.Now().UTC(),
	}
}

func (data *Data) GetValue() any {
	return data.value
}
