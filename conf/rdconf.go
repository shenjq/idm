package conf

import (
	"flag"
	"fmt"
	"github.com/golang/glog"
	"gopkg.in/ini.v1"
)

type DbInfo struct { //结构体名称与配置文件section名称必须一致
	User    string //首字母必须大写
	Pass    string
	Addr    string
	Port    int
	Dbname  string
	Charset string
	Maxconn int
	Maxidle int
}

type CtabSpec struct {
	Task1 string
	Task2 string
	Task3 string
}

type IniStruct struct {
	Ver string
	DbInfo
	CtabSpec
}

var gIni = &IniStruct{}
var gConfFile string

func init() {
	currpath, err := getCurrentPath()
	if err != nil {
		glog.V(0).Infof("GetCurrentPath err,%v", err)
	}
	defaultCfgFile := fmt.Sprintf("%sconf/conf.ini", currpath)
	flag.StringVar(&gConfFile, "cfgfile", defaultCfgFile, "config file,default path:"+defaultCfgFile)
}
func getCurrentPath() (string, error) {
	return "", nil
}

func InitConf() (err error) {
	//cfg, _ := ini.Load("/Users/shenjq/go/src/demo/idm/conf/conf.ini")
	//err = cfg.MapTo(gIni)
	err = ini.MapTo(gIni, "/Users/shenjq/go/src/demo/idm/conf/conf.ini")
	if err != nil {
		glog.V(0).Infof("initconf err,%v", err)
		return
	}
	glog.V(3).Infof("conf=%v", gIni)
	return
}

func GetIni() *IniStruct {
	return gIni
}
