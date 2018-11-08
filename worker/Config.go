package worker

import (
	"encoding/json"
	"io/ioutil"
)

// 配置结构体
type Config struct {
	EtcdEndpoints        []string `json:"etcdEndpoints"`
	EtcdDialTimeout      int      `json:"etcdDialTimeout"`
	MongodbURI           string   `json:"mongodbUri"`
	MongodbAuthSource    string   `json:"mongodbAuthSource"`
	MongodbUser          string   `json:"mongodbUser"`
	MongodbPass          string   `json:"mongodbPass"`
	MongodbTimeout       int      `json:"mongodbTimeout"`
	MongodbAuthMechanism string   `json:"mongodbAuthMechanism"`
	JobLogBatchSize      int      `json:"jobLogBatchSize"`
	JobLogCommitTimeout  int      `json:"jobLogCommitTimeout"`
}

var G_config *Config

// 加载配置
func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf    Config
	)
	// 1. 把配置文件读进来
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}

	// 2. JSON 反序列化
	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}

	// 3. 赋值单例
	G_config = &conf

	//fmt.Println(conf)

	return
}
