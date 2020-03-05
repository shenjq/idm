package conf

import (
	"errors"
	"flag"
	"fmt"
	"github.com/golang/glog"
	"gopkg.in/ini.v1"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
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
		fmt.Printf("GetCurrentPath err,%v", err)
	}
	defaultCfgFile := fmt.Sprintf("%sconf/conf.ini", currpath)
	flag.StringVar(&gConfFile, "cfgfile", defaultCfgFile, "config file,default path:"+defaultCfgFile)
}
func getCurrentPath() (string, error) {
	file, err := exec.LookPath(os.Args[0])
	if err != nil {
		return "", err
	}
	path, err := filepath.Abs(file)
	if err != nil {
		return "", err
	}

	if runtime.GOOS == "windows" {
		path = strings.Replace(path, "\\", "/", -1)
	}

	i := strings.LastIndex(path, "/")
	if i < 0 {
		return "", errors.New(`Can't find "/" or "\".`)
	}

	return string(path[0 : i+1]), nil
}

func InitConf() (err error) {
	//cfg, _ := ini.Load("/Users/shenjq/go/src/demo/idm/conf/conf.ini")
	//err = cfg.MapTo(gIni)
	err = ini.MapTo(gIni, gConfFile)
	if err != nil {
		fmt.Printf("getConfigfile err,%v", err)
		glog.V(0).Infof("initconf err,%v", err)
		return
	}
	glog.V(3).Infof("conf=%v", gIni)
	return
}

func GetIni() *IniStruct {
	return gIni
}
