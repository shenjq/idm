package busii

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"github.com/robfig/cron"
	"idm/conf"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

type Resp struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
}

func (r *Resp) Resp(w http.ResponseWriter) {
	if err := json.NewEncoder(w).Encode(r); err != nil {
		glog.V(0).Infof("%v\n", err)
	}
}

func RemoteIp(req *http.Request) string {
	remoteAddr := req.RemoteAddr

	if ip := req.Header.Get("Remote_addr"); ip != "" {
		remoteAddr = ip
	} else {
		remoteAddr, _, _ = net.SplitHostPort(remoteAddr)
	}

	if remoteAddr == "::1" {
		remoteAddr = "127.0.0.1"
	}
	glog.V(3).Infof("clinet ip:[%s]", remoteAddr)
	return remoteAddr
}

func SignalHander() {
	ch_sig := make(chan os.Signal)
	signal.Notify(ch_sig, syscall.SIGUSR1, syscall.SIGUSR2)
	go func() {
		for {
			v := <-ch_sig
			if v == syscall.SIGUSR1 {
				glog.V(0).Info("收到自定义中断信号SIGUSR1，执行指标数据初始化操作。")
				err := InitIdxinfo()
				if err != nil {
					glog.V(0).Infof("initIdxinfo fail,err:%v\n", err)
				}
			} else if v == syscall.SIGUSR2 {
				glog.V(0).Info("收到自定义中断信号SIGUSR2，执行任务(NextDay)初始化操作。")
				err := InitNextDayTask()
				if err != nil {
					glog.V(0).Infof("InitNextDayTask fail,err:%v\n", err)
				}
			}
		}
	}()
}

func Query(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	id, uError := r.Form["id"]
	var result Resp
	if !uError || (len(id[0]) != 7 && len(id[0]) != 14) { //暂定
		result.Code = "401"
		result.Msg = "参数有误"
	} else if len(id[0]) == 7 {
		var msg string
		v1, ok := gIdxMap[id[0]]
		if ok {
			msg = fmt.Sprintf("指标信息:%s", v1.toString())
		} else {
			msg = fmt.Sprintf("未找到指标[%s]信息", id[0])
		}
		v2, ok2 := gResultMap[id[0]]
		if ok2 {
			result.Msg = fmt.Sprintf("%s,最近一次提交记录:%v", msg, v2)
		} else {
			result.Msg = fmt.Sprintf("%s,没有最近提交记录.", msg)
		}
		result.Code = "200"
	} else if len(id[0]) == 14 {
		var t Task
		t.Id = id[0]
		taskinfo, err := t.GetTask()
		if err != nil {
			result.Msg = fmt.Sprintf("未找到指标%s相关信息.%v", id[0], err)
		} else {
			result.Msg = fmt.Sprintf("该指标相关信息=%s.", taskinfo)
		}
		result.Code = "200"
	}
	result.Resp(w)
}

func Cron() {
	glog.V(1).Info("开始启动后台定时任务...")
	c := cron.New()
	if c == nil {
		glog.V(0).Info("cron.new err")
		return
	}
	//定时任务1:将上日task数据迁移到task_his中
	err := c.AddFunc(conf.GetIni().Task1, cron_task1)
	if err != nil {
		glog.V(0).Infof("addfunc err,%v", err)
		return
	}
	//定时任务2:根据task_list生成task（第二天任务）
	err = c.AddFunc(conf.GetIni().Task2, cron_task2)
	if err != nil {
		glog.V(0).Infof("addfunc err,%v", err)
		return
	}
	c.Start()
	defer c.Stop()
	select {}
}

func cron_task1() {
	glog.V(1).Infof("crontab 调用定时任务1:迁移task。")
	if err := backupTask(); err != nil {
		glog.V(0).Infof("backupTask err,%v", err)
	}
}

func cron_task2() {
	glog.V(1).Infof("crontab 调用定时任务2:生成task。")
	InitNextDayTask()
}
