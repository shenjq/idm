package main

import (
	"flag"
	"fmt"
	"github.com/golang/glog"
	_ "github.com/icattlecoder/godaemon"
	"idm/busii"
	"idm/conf"
	"idm/pub"
	"net/http"
	"time"
)

var gArgs = make(map[string]string)

func main() {
	prepare()
	defer glog.Flush()

	var err error
	glog.V(0).Infof("initconf ...\n")
	err = conf.InitConf()
	if err != nil {
		glog.V(0).Infof("initconf fail,err:%v\n", err)
		return
	}

	err = pub.InitDB()
	if err != nil {
		glog.V(0).Infof("initDB fail:%v\n", err)
		return
	}

	busii.CreateIB()
	busii.CreateTB()

	err = busii.InitIdxinfo()
	if err != nil {
		glog.V(0).Infof("initIdxinfo fail,err:%v\n", err)
		return
	}
	err = busii.InitTask()
	if err != nil {
		glog.V(0).Infof("initTask fail,err:%v\n", err)
		return
	}

	busii.SignalHander()
	go busii.Cron()
	go busii.GetIB().DoIdxBusi()
	go busii.GetTB().DoTmBusi()

	glog.V(0).Info("start server ...")
	glog.Flush()
	mux := http.NewServeMux()
	mux.HandleFunc("/help", Middle(help))
	mux.HandleFunc("/idx", Middle(busii.Idx_handler))
	mux.HandleFunc("/tm", Middle(busii.Tm_handler))
	mux.HandleFunc("/query", Middle(busii.Query))
	err = http.ListenAndServe("0.0.0.0:8080", mux)
	if err != nil {
		glog.V(0).Infof("http.ListenAndServe fail,err:%v\n", err)
		glog.Flush()
		return
	}
	fmt.Printf("服务启动完成。\n")
}

func Middle(f http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer glog.V(3).Infof("Middle request: %s cost %.4f seconds\n", r.URL.String(), time.Now().Sub(start).Seconds())
		f(w, r)
	})
}

func help(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "更新指标数据、任务数据，接收GET/POST请求,支持form、json格式.\n")
	fmt.Fprintf(w, "Usage:\n")
	fmt.Fprintf(w, "指标数据更新地址: http://ip:8080/idx 接受id,value,host\n")
	fmt.Fprintf(w, "任务数据更新地址: http://ip:8080/tm  接受id,stat,note,host\n")
	fmt.Fprintf(w, "示例:\n")
	fmt.Fprintf(w, "curl -H \"Content-Type: application/json\" -X POST  --data '{\"id\":\"1000001\",\"value\":20.4,\"host\":\"hostname/ip\"}' http://ip:8080/idx\n")
	fmt.Fprintf(w, "curl -X POST  http://ip:8080/idx -d 'id=1000001&value=20.4&host=ipaddress/hostname' \n")
	fmt.Fprintf(w, "curl http://ip:8080/idx?id=1000001&value=20.4&host=ipaddress/hostname \n")
	fmt.Fprintf(w, "curl -X POST  http://ip:8080/tm -d 'id=1000001&stat=s/f/e&note=说明&host=ipaddress/hostname' \n")
	fmt.Fprintf(w, "curl http://ip:8080/query?id=5000001 \n")
	fmt.Fprintf(w, "curl http://ip:8080/query?id=20200202001001 \n")
}

func prepare() bool {
	flag.Parse()
	flag.Visit(getArgs)
	_, ok := gArgs["v"]
	if !ok {
		fmt.Printf("未设置日志级别，默认值为0\n")
	}
	_, ok = gArgs["d"]
	if !ok {
		fmt.Printf("可设置-d=true 后台运行程序\n")
	}
	_, ok = gArgs["log_dir"]
	if !ok {
		fmt.Printf("未设置日志目录log_dir，不记录日志!!!\n")
	}
	return true
}

func getArgs(f *flag.Flag) {
	gArgs[f.Name] = f.Value.String()
}
