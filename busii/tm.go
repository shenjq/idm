package busii

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"idm/pub"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Task struct {
	Id   string    `json:"id"` //字段首字符大写！！！
	Tm   time.Time `json:"tm"`
	Stat string    `json:"stat"`
	Note string    `json:"note"`
	Host string    `json:"host"`
}

type TaskInfo struct {
	Id       string
	Taskname string
	Planbgtm sql.NullTime
	Realbgtm sql.NullTime
	Planedtm sql.NullTime
	Realedtm sql.NullTime
	Stat     string
	Hero     string
	Idxid    sql.NullString
}

type TmBusi struct {
	Ch *chan Task
	Db *sql.DB
}

var gCH_task = make(chan Task, 256)
var gTB TmBusi

func GetTB() *TmBusi {
	return &gTB
}

func CreateTB() {
	gTB.Ch = &gCH_task
	gTB.Db = pub.GetDb()
}

func (t *TaskInfo) toString() string {
	var strplanbg, strrealbg, strplaned, strrealed, stridx string
	if t.Planbgtm.Valid {
		strplanbg = fmt.Sprintf("计划开始时间:%s", pub.GetFmtDateStr1(t.Planbgtm.Time))
	}
	if t.Realbgtm.Valid {
		strrealbg = fmt.Sprintf("实际开始时间:%s", pub.GetFmtDateStr1(t.Realbgtm.Time))
	}
	if t.Planedtm.Valid {
		strplaned = fmt.Sprintf("计划完成时间:%s", pub.GetFmtDateStr1(t.Planbgtm.Time))
	}
	if t.Realedtm.Valid {
		strrealed = fmt.Sprintf("实际完成时间:%s", pub.GetFmtDateStr1(t.Realedtm.Time))
	}
	if t.Idxid.Valid {
		stridx = fmt.Sprintf("对应指标:%s", t.Idxid.String)
	}

	return fmt.Sprintf("taskID:%s,taskName:%s,%s,%s,%s,%s,负责人:%s,%s,状态:%s",
		t.Id, t.Taskname, strplanbg, strrealbg, strplaned, strrealed, t.Hero, stridx, t.Stat)
}

func (t *Task) GetTask() (tf string, err error) {
	sqlstr := `select * from task where id = ? union select * from task_his where id = ?`
	var taskinfo TaskInfo
	err = pub.QueryOneRow(sqlstr, t.Id, t.Id).Scan(&taskinfo.Id,
		&taskinfo.Taskname,
		&taskinfo.Planbgtm,
		&taskinfo.Realbgtm,
		&taskinfo.Planedtm,
		&taskinfo.Realedtm,
		&taskinfo.Hero,
		&taskinfo.Idxid,
		&taskinfo.Stat)
	if err != nil {
		return "", err
	}
	return taskinfo.toString(), nil
}

//检查报文,发送报文至channel
func (t *Task) DoTask() (err error) {
	glog.V(3).Infof("准备提交请求,%v", t)
	t.Stat = strings.ToUpper(t.Stat)
	if t.Stat != "S" && t.Stat != "F" && t.Stat != "E" {
		glog.V(3).Infof("状态位参数错误,%s", t.Stat)
		return fmt.Errorf("状态位参数错误,%s", t.Stat)
	}
	ct := pub.SelectCount("select count(*) ct from task where id =?", t.Id)
	if ct != 1 {
		glog.V(3).Infof("id:%s有误", t.Id)
		return fmt.Errorf("检查输入id,%s", t.Id)
	}
	t.Tm = time.Now()
	gCH_task <- *t
	return nil
}

//数据格式：id=1000001&stat=s/f/e&note=说明&host=ipaddress/hostname
func Tm_handler(w http.ResponseWriter, r *http.Request) {
	var task Task
	var result Resp

	defer result.Resp(w)

	tp := r.Header.Get("Content-Type")
	if strings.Count(tp, "json") == 1 { //json格式
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			glog.V(0).Infof("ReadAll err:%v", err)
			result.Code = "401"
			result.Msg = "读取参数有误"
			return
		}
		body_str := string(body)
		glog.V(3).Infof("json格式请求报文:%s\n", body_str)
		num := strings.Count(body_str, `"id"`) + strings.Count(body_str, `"stat"`)
		err = json.Unmarshal(body, &task)
		if err != nil || num != 2 {
			result.Code = "401"
			result.Msg = "参数有误"
			return
		} else {
			if len(strings.TrimSpace(task.Host)) == 0 {
				task.Host = RemoteIp(r)
			}
		}
	} else { //form格式
		err := r.ParseForm()
		if err != nil {
			glog.V(0).Infof("ParseForm err:%v", err)
			result.Code = "401"
			result.Msg = "参数有误"
			return
		}
		glog.V(3).Infof("form格式报文:%v", r.Form)

		id, iok := r.Form["id"]
		stat, sok := r.Form["stat"]
		note, nok := r.Form["note"]
		host, hok := r.Form["host"]
		if !iok || !sok {
			result.Code = "401"
			result.Msg = "参数有误"
			return
		} else {
			var sNote, clientIp string
			if !nok {
				sNote = ""
			} else {
				sNote = note[0]
			}
			if !hok {
				clientIp = RemoteIp(r)
			} else {
				clientIp = host[0]
			}
			task = Task{
				Id:   id[0],
				Stat: stat[0],
				Note: sNote,
				Host: clientIp,
			}
		}
	}

	err := task.DoTask()
	glog.V(3).Infof("Form 格式化后数据:%v\n", task)
	if err != nil {
		result.Msg = fmt.Sprintf("提交失败,%v", err)
	} else {
		result.Msg = "提交成功"
	}
	result.Code = "200"
}

//后台执行任务
func (tb *TmBusi) DoTmBusi() {
	var err error
	for {
		v, ok := <-gCH_task
		if !ok {
			glog.V(1).Infof("数据读取完毕.")
			break
		}
		glog.V(3).Infof("处理服务从通道获取数据:[%v]\n", v)
		err = v.updateTask()
		if err != nil {
			glog.V(0).Infof("updateTask failed,err:%v\n", err)
		} else {
			err = v.sendIdx()
		}
	}
}

//更新任务
func (t *Task) updateTask() (err error) {
	glog.V(3).Infof("updateTask(),开始处理请求,%v", t)

	tx, err := gTB.Db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			glog.V(0).Infoln("rollback!!!!!!")
			tx.Rollback()
		} else {
			glog.V(3).Info("updateTask() success.")
			tx.Commit()
		}
	}()

	if t.Stat == "E" {
		instr := `insert task_note (id,tm,host,note) values (?,?,?)`
		_, err = tx.Exec(instr, t.Id, t.Tm, t.Host, t.Note)
		if err != nil {
			glog.V(0).Infof("insert into task_note failed,err:%v\n", err)
			return
		}
		glog.V(3).Infof("insert into task_note success")
		_, err = tx.Exec("update task set stat=? where id=?", t.Stat, t.Id)
		if err != nil {
			glog.V(0).Infof("update task failed,err:%v\n", err)
			return
		}
		tx.Commit()
		return nil
	}

	var upstr string
	if t.Stat == "S" {
		upstr = `update task set realbgtm=?,stat=? where id=?`
	} else if t.Stat == "F" {
		upstr = `update task set realedtm=?,stat=? where id=?`
	}
	_, err = tx.Exec(upstr, t.Tm, t.Stat, t.Id)
	if err != nil {
		glog.V(0).Infof("update task failed,err:%v\n", err)
		return
	}
	glog.V(3).Infof("update task success")

	tx.Commit()
	return nil
}

//判断、更新指标数据
func (t *Task) sendIdx() (err error) {
	if t.Stat != "F" {
		return nil
	}
	var realbgtime, realedtime sql.NullTime
	var idxid sql.NullString
	err = pub.QueryOneRow("select realbgtm,realedtm,idxid from task where id =?", t.Id).Scan(&realbgtime, &realedtime, &idxid)
	if err != nil {
		glog.V(0).Infof("select task err,%v", err)
		return
	}
	if !realbgtime.Valid || !realedtime.Valid || !idxid.Valid {
		return nil
	}

	para := fmt.Sprintf("id=%s&value=%f&host=localhost", idxid.String, realedtime.Time.Sub(realbgtime.Time).Seconds())
	var resp *http.Response
	resp, err = http.Post("http://127.0.0.1:8080/idxform",
		"application/x-www-form-urlencoded", strings.NewReader(para))
	if err != nil {
		glog.V(0).Infof("http.post err,%v", err)
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.V(0).Infof("read http err,%v", err)
		return
	}
	glog.V(3).Info(string(body))

	return nil
}

//定时判断是否需要预警
func (t *Task) warn() (err error) {
	return nil
}

//程序启动后判断是否需要初始化
func InitTask() (err error) {
	datestr := pub.GetDateStr4()
	sqlstr := fmt.Sprintf("select count(*) ct from task where id like '%s%%'", datestr)
	if ct := pub.SelectCount(sqlstr); ct > 0 {
		glog.V(3).Info("不需要初始化任务表.")
		return nil
	}
	return InitDayTask(datestr)
}

func InitNextDayTask() (err error) {
	datestr := pub.GetNextDateStr4()
	return InitDayTask(datestr)
}

//根据日期(格式Yyyymmdd)初始化当日任务表
func InitDayTask(datestr string) (err error) {
	tx, err := gTB.Db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			glog.V(0).Infoln("rollback!!!!!!")
			tx.Rollback()
		} else {
			tx.Commit()
		}
		glog.V(3).Info("初始化任务表结束.")
	}()

	glog.V(3).Infof("启动初始化[%s]任务表......", datestr)
	//删除task表原数据
	str := fmt.Sprintf("delete from task where id like '%s%%'", datestr)
	_, err = tx.Exec(str)
	if err != nil {
		glog.V(0).Infof("delete task failed,err:%v\n", err)
		return
	}
	var rows *sql.Rows
	rows, err = gTB.Db.Query("select * from task_list")
	defer func() {
		if rows != nil {
			rows.Close()
		}
	}()
	if err != nil {
		glog.V(0).Infof("Query failed,err:%v\n", err)
		return
	}
	var id, taskname, planbgtm, planedtm, hero, idxid, rate string
	var bgtime, edtime time.Time
	sqlstr := `insert into task (id,taskname,planbgtm,planedtm,hero,idxid,stat) value(?,?,?,?,?,?,?)`
	for rows.Next() {
		err = rows.Scan(&id, &taskname, &planbgtm, &planedtm, &hero, &idxid, &rate)
		if err != nil {
			glog.V(0).Infof("Scan failed,err:%v\n", err)
			return
		}
		glog.V(3).Infof(">>启动任务%s,%s的初始化---->", id, taskname)
		var isTask bool
		isTask, err = isTodayTask(rate, datestr)
		if err != nil {
			glog.V(0).Infof("任务%s判断频率出错,%v", id, err)
		}
		if !isTask {
			glog.V(3).Infof("task:%s不是当天任务", id)
			continue
		}

		tmcount := 0
		if len(strings.TrimSpace(planbgtm)) > 0 {
			sp := strings.Split(planbgtm, ":")
			if len(sp) < 2 {
				glog.V(0).Infof("planbgtm format err,%s.", planbgtm)
				continue
			}
			iH, e1 := strconv.Atoi(sp[0])
			iM, e2 := strconv.Atoi(sp[1])
			if e1 != nil || e2 != nil {
				glog.V(0).Infof("planbgtm format err,%s.", planbgtm)
				continue
			}
			tmstr1 := fmt.Sprintf("%s%02d:%02d", datestr, iH, iM)
			glog.V(6).Infof("debug--->begintime:[%s]", tmstr1)
			bgtime, err = time.ParseInLocation("2006010215:04", tmstr1, time.Local)
			if err != nil {
				glog.V(0).Infof("任务%s判断频率出错,%v", id, err)
				continue
			}
			tmcount++
		}
		if len(strings.TrimSpace(planedtm)) > 0 {
			sp := strings.Split(planedtm, ":")
			if len(sp) < 2 {
				glog.V(0).Infof("planedtm format err,%s.", planedtm)
				continue
			}
			iH, e1 := strconv.Atoi(sp[0])
			iM, e2 := strconv.Atoi(sp[1])
			if e1 != nil || e2 != nil {
				glog.V(0).Infof("planedtm format err,%s.", planedtm)
				continue
			}
			tmstr2 := fmt.Sprintf("%s%02d:%02d", datestr, iH, iM)
			glog.V(6).Infof("debug--->begintime:[%s]", tmstr2)
			edtime, err = time.ParseInLocation("2006010215:04", tmstr2, time.Local)
			if err != nil {
				glog.V(0).Infof("任务%s判断频率出错,%v", id, err)
				continue
			}
			tmcount++
		}

		if tmcount == 2 && edtime.Before(bgtime) {
			edtime = edtime.Add(time.Hour * 24)
		}
		glog.V(6).Infof("id:%s,bgtime:%v,edtime:%v", id, bgtime, edtime)
		//逐条插入task表
		var result sql.Result
		result, err = tx.Exec(sqlstr, datestr+id, taskname, bgtime, edtime, hero, idxid, "0")
		if err != nil {
			glog.V(0).Infof("insert into task failed,err:%v\n", err)
			return
		}
		glog.V(3).Infof("任务%s初始化成功.", datestr+id)
		glog.V(6).Infoln(result.RowsAffected())
	}
	tx.Commit()

	return nil
}

/*
判断是否当天任务，rate值典型如下：
D
M[][01|10|-1]
M[12][31]
*/
func isTodayTask(r string, datestr string) (bool, error) {
	rate := r

	glog.V(6).Infof("判断%s,%s是否是当天任务", rate, datestr)
	if len(strings.TrimSpace(rate)) == 0 {
		return false, fmt.Errorf("频率字符串[%s]格式有误,未设置.", rate)
	}
	strings.ToUpper(rate)
	if strings.HasPrefix(rate, "D") {
		return true, nil
	}
	if !strings.HasPrefix(rate, "M") {
		return false, fmt.Errorf("频率字符串[%s]格式有误", rate)
	}

	sp := strings.Split(rate[1:], "][")
	sp[0] = strings.ReplaceAll(sp[0], "[", "")
	sp[1] = strings.ReplaceAll(sp[1], "]", "")
	//glog.V(3).Infof("%s,%s", sp[0], sp[1])
	if len(sp[1]) == 0 {
		glog.V(3).Infof("频率字符串[%s]格式有误,未设置执行日期", rate)
		return false, fmt.Errorf("频率字符串[%s]格式有误,未设置执行日期", rate)
	}
	ok_m := false
	ok_d := false
	//判断月
	if len(sp[0]) == 0 {
		glog.V(6).Info("month is nil set")
		ok_m = true
	} else {
		glog.V(6).Infof("月份[%s]", datestr[4:6])
		iM, _ := strconv.Atoi(datestr[4:6])
		v := strings.Split(sp[0], "|")
		for i := 0; i < len(v); i++ {
			glog.V(6).Infof("debug=====>对比月份,%d,%s", iM, v[i])
			im, err := strconv.Atoi(v[i])
			if err != nil {
				glog.V(3).Infof("频率字符串[%s]格式有误,%s", rate, v[i])
				return false, fmt.Errorf("频率字符串[%s]格式有误,%v", rate, err)
			}
			if im == iM {
				ok_m = true
				break
			}
		}
	}
	if !ok_m {
		return false, nil
	}
	//判断天
	var iD [2]int
	var ilength = 1
	iD[0], _ = strconv.Atoi(datestr[6:])
	if ismonthend, _ := pub.IsMonthEnd(datestr); ismonthend {
		iD[1] = -1
		ilength++
	}
	v := strings.Split(sp[1], "|")
forloop:
	for i := 0; i < len(v); i++ {
		id, err := strconv.Atoi(v[i])
		if err != nil {
			glog.V(3).Infof("频率字符串[%s]格式有误,%s", rate, v[i])
			return false, fmt.Errorf("频率字符串[%s]格式有误,%v", rate, err)
		}
		for j := 0; j < ilength; j++ {
			glog.V(6).Infof("debug=====>对比日期,%d,%d", id, iD[j])
			if id == iD[j] {
				ok_d = true
				break forloop
			}
		}
	}
	if ok_d {
		return true, nil
	}
	return false, nil
}

func backupTask() (err error) {
	tx, err := gTB.Db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			glog.V(0).Infoln("rollback!!!!!!")
			tx.Rollback()
		} else {
			tx.Commit()
		}
		glog.V(3).Info("清理、备份任务表结束.")
	}()

	glog.V(1).Infof("开始清理备份上一日task数据...")
	ysd := pub.GetFmtDateStr4(time.Now().Add(-24 * time.Hour))

	//插入历史表
	str_ist := fmt.Sprintf("insert into task_his select * from task where id like '%s%%'", ysd)
	_, err = tx.Exec(str_ist)
	if err != nil {
		glog.V(0).Infof("insert into task_his failed,err:%v\n", err)
		return
	}
	//删除task表原数据
	str_del := fmt.Sprintf("delete from task where id like '%s%%'", ysd)
	_, err = tx.Exec(str_del)
	if err != nil {
		glog.V(0).Infof("delete task failed,err:%v\n", err)
		return
	}
	tx.Commit()
	return nil
}
