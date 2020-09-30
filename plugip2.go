package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/robfig/cron"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

var dir *string
var ServerAddress *string
var WechatAddress *string
var NoticeEmp *string

type Resp struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
}

type plugact struct {
	ip     string
	action string
	dur    int
}

type plugplan struct {
	tm    *time.Time
	entry *plugact
}

const QLEN = 512

var GFlag int32

var CH_plugnow = make(chan plugact, 16)
var Q_plugplan = make([] plugplan, QLEN)
var update_flag bool

var currPlugFile string = "plugip_hw.txt"
var plugplanFile string = "plugplan.json"

var totalPlugFile string = "plugip_hw_total.txt"
var currPlugFullPath = ""
var totalPlugFullPath = ""

func init() {
	dir = flag.String("dir", "./", "default dir")
	ServerAddress = flag.String("serveraddress", "99.0.0.97:17788", "EC serveraddress: Ip:port")
	WechatAddress = flag.String("wechataddress", "99.172.64.127:7889", "send wechat serveraddress: Ip:port")
	NoticeEmp = flag.String("noticeemp", "牟晓钟", "通知人: 牟晓钟|xxx")

}

func paraCheck() {
	fmt.Printf("dir=[%s]\n", *dir)
	if strings.HasSuffix(*dir, "/") {
		currPlugFullPath = fmt.Sprintf("%s%s", *dir, currPlugFile)
		totalPlugFullPath = fmt.Sprintf("%s%s", *dir, totalPlugFile)
	} else {
		currPlugFullPath = fmt.Sprintf("%s/%s", *dir, currPlugFile)
		totalPlugFullPath = fmt.Sprintf("%s/%s", *dir, totalPlugFile)
	}
	fmt.Printf("currpath =[%s]\n", currPlugFullPath)
	fmt.Printf("totalpath=[%s]\n", totalPlugFullPath)
	fmt.Printf("EC-ServerAddress=[%s]\n", *ServerAddress)
	fmt.Printf("WechatAddress=[%s]\n", *WechatAddress)
	fmt.Printf("NoticeEmp=[%s]\n", *NoticeEmp)

	tm := time.Unix(0, 0)
	for i, _ := range Q_plugplan {
		Q_plugplan[i].tm = &tm
		Q_plugplan[i].entry = nil
	}
}

func main() {
	flag.Parse()

	paraCheck()

	go Cron()
	go doAction()

	//start web-server
	fmt.Printf("start server ...\n")
	mux := http.NewServeMux()

	mux.HandleFunc("/plugip", do_plugip)
	mux.HandleFunc("/query", do_query)

	err := http.ListenAndServe("0.0.0.0:9527", mux)
	if err != nil {
		fmt.Printf("http.ListenAndServe fail,err:%v\n", err)
		return
	}
}

func ipCheck(ip string) bool {
	strings.TrimSpace(ip)
	ct := strings.Count(ip, ".")
	if len(ip) < 7 || len(ip) > 15 || ct != 3 {
		return false
	}
	return true
}

func do_plugip(w http.ResponseWriter, r *http.Request) {

	urlStr := r.URL.String()
	fmt.Printf("%s Request :%s\n", getTimeStr1(), urlStr)

	r.ParseForm()

	var ip string
	iptmp, ok := r.Form["ip"]
	if !ok {
		ip = ""
	} else {
		ip = iptmp[0]
	}

	var result Resp
	chk := ipCheck(ip)
	if !chk {
		result.Code = "-1"
		result.Msg = "ip格式有误"
		result.Resp(w)
		return
	}

	var entry plugact
	entry.ip = ip
	acttmp, ok := r.Form["action"]
	if !ok {
		entry.action = "add"
	} else {
		entry.action = acttmp[0]
	}

	var sdur string
	durtmp, ok := r.Form["dur"]
	if !ok {
		sdur = "5" //default = 5 min
	} else {
		sdur = durtmp[0]
	}
	i, err := strconv.Atoi(sdur)
	if err != nil {
		fmt.Printf("dur para err:%v\n", err)
		i = 5
	}
	entry.dur = i

	CH_plugnow <- entry

	result.Code = "200"
	result.Msg = "提交成功"
	result.Resp(w)
}

func do_query(w http.ResponseWriter, r *http.Request) {
	var ip string

	urlStr := r.URL.String()
	fmt.Printf("%s Request :%s\n", getTimeStr1(), urlStr)

	r.ParseForm()
	stmp, ok := r.Form["ip"]
	if !ok {
		ip = ""
	} else {
		ip = stmp[0]
	}
	/*
		var result Resp
		chk := ipCheck(ip)
		if !chk {
			result.Code = "-1"
			result.Msg = "ip格式有误"
			result.Resp(w)
			return
		}
	*/
	var cmd string
	if len(strings.TrimSpace(ip)) == 0 {
		cmd = fmt.Sprintf("cat %s", totalPlugFullPath)
	} else {
		cmd = fmt.Sprintf("grep %s %s", ip, totalPlugFullPath)
	}
	out, err := execOSCmd(cmd)
	if err != nil {
		result := Resp{
			Code: "200",
			Msg:  "错误：" + err.Error(),
		}
		result.Resp(w)
	}
	fmt.Fprint(w, out)
}

func (r *Resp) Resp(w http.ResponseWriter) {
	if err := json.NewEncoder(w).Encode(r); err != nil {
		fmt.Printf("http Resp err,%v\n", err)
	}
}

//返回os命令返回结果
func execOSCmd(cmd string) (string, error) {
	cmdarray := strings.Split(cmd, " ")
	if len(cmdarray) == 0 {
		return "", fmt.Errorf("命令行为空")
	}
	args := make([]string, 0)
	for i, v := range cmdarray {
		if i == 0 {
			continue
		}
		args = append(args, v)
	}

	cd := exec.Command(cmdarray[0], args...)
	fmt.Printf("ExecOSCmd: %v\n", cd.Args)
	var o, e bytes.Buffer
	cd.Stdout = &o
	cd.Stderr = &e
	err := cd.Start()
	if err != nil {
		fmt.Printf("exec.start err, %v\n", err)
		return "", err
	}
	err = cd.Wait()
	if err != nil {
		fmt.Printf("exec.Command err, %v\n", err)
		return "", err
	}

	fmt.Printf("%v ....done\n", cd.Args)
	return o.String(), nil
}

func doAction() {
	fmt.Printf("启动处理任务进程...\n")
	for {
		//从通道处读取数据
		act, ok := <-CH_plugnow
		if !ok {
			fmt.Printf("数据读取完毕.")
			break
		}
		fmt.Printf("====>从队列CH_plugnow读取任务:%#v\n", act)

		//write log
		wirteLogFile(&act)

		//do action(add/delete)

		//
		if act.action == "add" {
			fmt.Printf("====>1:%#v\n", act)
			tm := time.Now().Add(time.Duration(act.dur) * time.Minute)
			act.dur = 0
			act.action = "delete"
			for i, v := range Q_plugplan {
				fmt.Printf("====>xxx %#v\n", v)
				if v.tm.Equal(time.Unix(0, 0)) {
					fmt.Printf("====>2:%v,%#v\n", v.tm, act)
					for {
						if GFlag > 0 {
							fmt.Printf("冲突!!sleep ...\n")
							time.Sleep(time.Microsecond * 500)
							continue
						}
						GFlag++
						Q_plugplan[i].tm = &tm
						Q_plugplan[i].entry = &act
						GFlag--
						fmt.Printf("====>333333333:%v\n", Q_plugplan[i].tm)
						break
					}
					break
				}
			}
		}
	}
}

func Cron() {
	fmt.Printf("开始启动后台定时任务...\n")
	c := cron.New()
	if c == nil {
		fmt.Printf("cron.new err")
		return
	}
	//定时任务1
	spec := fmt.Sprintf("0 */1 * * * *")
	err := c.AddFunc(spec, delAct)
	if err != nil {
		fmt.Printf("addfunc err,%v", err)
		return
	}

	c.Start()
	defer c.Stop()
	select {}
}

func delAct() {
	for i, v := range Q_plugplan {
		if v.tm.After(time.Unix(0, 0)) && v.tm.Before(time.Now()) {
			fmt.Printf("====>4444444:%v\n", v.tm)
			CH_plugnow <- *v.entry
			for {
				if GFlag > 0 {
					fmt.Printf("冲突!!sleep ...\n")
					time.Sleep(time.Microsecond * 200)
					continue
				}
				GFlag++
				tm := time.Unix(0, 0)
				Q_plugplan[i].tm = &tm
				GFlag--
				break
			}
		}
	}
	if update_flag {
		update_flag = false
		byteTmp, err := json.Marshal(&Q_plugplan)
		if err != nil {
			ioutil.WriteFile(plugplanFile, byteTmp, 0664)
		}
	}
}

func wirteLogFile(act *plugact) {

	fd, err := os.OpenFile(totalPlugFullPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
	if err != nil {
		fmt.Printf("open file err,%v\n", err)
		return
	}
	defer fd.Close()

	writer := bufio.NewWriter(fd)

	stmp := fmt.Sprintf("%s %v\n", getTimeStr1(), act)
	writer.WriteString(stmp)

	writer.Flush()

	/*

		if icount > 0 {
			out := fmt.Sprintf("过去5分钟封禁了%d个互联网攻击IP.", icount)
			EasyAlert2EC("", "ip地址封禁", out)
			out = fmt.Sprintf("过去5分钟封禁了%d个互联网攻击IP,%v", icount, iplist)
			Easy2WeChat(out)
		}

	*/
}

func getTimeStr1() string {
	timeTemplate1 := "2006-01-02 15:04:05" //常规类型
	return time.Now().Format(timeTemplate1)
}

//获取管理网地址
func getMgrIP(target string) string {
	conn, err := net.Dial("udp", target)
	if err != nil {
		fmt.Printf("net.Dial err,%v", err)
		return ""
	}
	defer conn.Close()
	return strings.Split(conn.LocalAddr().String(), ":")[0]
}

type Warninfo struct {
	EcType      string `json:"ectype"`
	Id_original string `json:"id_original"`
	Source      string `json:"source"` //RT_logfile,index,aops...
	Ip          string `json:"ip"`
	Severity    string `json:"severity"`
	Title       string `json:"title"`
	Summary     string `json:"summary"`
	Status      string `json:"status"`
	ShowTimes   string `json:"showtimes"`
	NoticeEmpNo string `json:"noticeempno"`
	NoticeWay   string `json:"noticeway"` //0不通知
}

func Easy2WeChat(content string) error {
	url := fmt.Sprintf("http://%s", *WechatAddress)
	cnt := fmt.Sprintf("trancd=1001&name=%s&content=%s", *NoticeEmp, content)
	fmt.Printf("content:%s", cnt)
	PostForm(url, cnt)

	return nil
}

func EasyAlert2EC(ectype, title, content string) {
	winfo := new(Warninfo)
	winfo.EcType = ectype
	winfo.Summary = content
	winfo.Title = title
	winfo.Ip = getMgrIP("99.0.0.254:80")
	winfo.Severity = "4"
	winfo.Source = "Ses-check"
	winfo.NoticeEmpNo = *NoticeEmp
	winfo.NoticeWay = "0"
	winfo.ShowTimes = "600"

	Alert2EC(winfo)
}

func Alert2EC(winfo *Warninfo) error {
	url := fmt.Sprintf("http://%s/warn", *ServerAddress)
	jsonbytes, err := json.Marshal(winfo)
	if err != nil {
		fmt.Printf("Marshal err:%v", err)
		return err
	}

	fmt.Printf("%s", string(jsonbytes))
	_, err = PostJson(url, string(jsonbytes))
	if err != nil {
		fmt.Printf("提交失败:%v", err)
	}
	return err
}

//dataformat json.Unmarshal([]byte(jsonStr), &data) ##data struct
func PostJson(url, data string) (string, error) {
	contentType := "application/json"
	//jsonStr, _ := json.Marshal(data)
	//return post(url, contentType, bytes.NewReader(jsonStr))
	return post(url, contentType, strings.NewReader(data))
}

//dataformat id=1000001&value=20.4&host=ipaddress
func PostForm(url string, data string) (string, error) {
	contentType := "application/x-www-form-urlencoded"
	return post(url, contentType, strings.NewReader(data))
}

// 发送POST请求
// url：         请求地址
// body：        POST请求提交的数据
// contentType： 请求体格式，如：application/json,application/x-www-form-urlencoded
// return：     请求放回的内容
func post(url string, contentType string, body io.Reader) (string, error) {

	//fmt.Printf("[%v]\n", body)
	// 超时时间：5秒
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(url, contentType, body)
	if err != nil {
		fmt.Printf("http post err,%v\n", err)
		return "", err
	}
	defer resp.Body.Close()

	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(result), nil
}
