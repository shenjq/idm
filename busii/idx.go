package busii

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"idm/conf"
	"idm/pub"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Index struct {
	Id     string  `json:"id"` //字段首字符大写！！！
	Value  float32 `json:"value"`
	Host   string  `json:"host"`
	realId string
}

type Idxinfo struct {
	Id                string
	Name              string
	Note              string
	Unit              string
	EmpNo             sql.NullString
	EmpNm             sql.NullString
	Flag              sql.NullString  //标识
	Lv                sql.NullFloat64 //低水位
	Sv                sql.NullFloat64 //标准水位
	Uv                sql.NullFloat64 //高水位
	WarnNum           sql.NullInt32   //告警次数大于该值，发送预警事件,默认值=0
	Needup            bool            //更新指标数据？
	IsWarn            bool            //是否已经发送预警事件
	ContinuousWarnNum int32           //连续预警次数
}
type Result struct {
	Index
	tm  string
	msg string
}
type IdxBusi struct {
	Ch        *chan Index
	IdxMap    *map[string]Idxinfo
	ResultMap *map[string]Result
	Db        *sql.DB
	//	stmt_instnow *sql.Stmt
	//	stmt_insthis *sql.Stmt
	//	stmt_upnow   *sql.Stmt
}

var gCH_idx = make(chan Index, 256)
var gIdxMap = make(map[string]Idxinfo, 256)
var gResultMap = make(map[string]Result, 256)

var gIB IdxBusi

func CreateIB() {
	gIB.Ch = &gCH_idx
	gIB.IdxMap = &gIdxMap
	gIB.ResultMap = &gResultMap
	gIB.Db = pub.GetDb()
}

func GetIB() *IdxBusi {
	return &gIB
}

func (t *Idxinfo) toString() string {
	var strflag, strlv, strsv, struv string
	if t.Flag.Valid {
		strflag = fmt.Sprintf("预警标识:%s", t.Flag.String)
	}
	if t.Lv.Valid {
		strlv = fmt.Sprintf("低水位:%.2f", t.Lv.Float64)
	}
	if t.Sv.Valid {
		strsv = fmt.Sprintf("标准水位:%.2f", t.Sv.Float64)
	}
	if t.Uv.Valid {
		struv = fmt.Sprintf("高水位:%.2f", t.Uv.Float64)
	}

	return fmt.Sprintf("indxID:%s,Name:%s,说明:%s,单位:%s,%s,%s,%s,%s,负责人",
		t.Id, t.Name, t.Note, t.Unit, strflag, strlv, strsv, struv)
}

func InitIdxinfo() (err error) {
	var tmpmap = make(map[string]Idxinfo, 256)
	glog.V(1).Info("start InitIdxinfo...")
	idxif := new(Idxinfo)
	rows, err := gIB.Db.Query("select a.id,a.name,a.note,a.unit,a.empno,a.empnm,b.flag,b.lv,b.sv,b.uv,b.warnnum from idx_list a LEFT JOIN idx_warn b ON a.id=b.id;")
	if err != nil {
		glog.V(0).Infof("Query failed,err:%v\n", err)
		return err
	}
	defer func() {
		if rows != nil {
			rows.Close() //可以关闭掉未scan连接一直占用
		}
	}()

	for rows.Next() {
		err = rows.Scan(&idxif.Id, &idxif.Name, &idxif.Note, &idxif.Unit, &idxif.EmpNo, &idxif.EmpNm, &idxif.Flag, &idxif.Lv, &idxif.Sv, &idxif.Uv, &idxif.WarnNum) //不scan会导致连接不释放
		if err != nil {
			glog.V(0).Infof("Scan failed,err:%v\n", err)
			return
		}
		if idxif.Flag.Valid {
			var err error
			if strings.HasPrefix(idxif.Flag.String, "2") {
				idxif.Lv.Float64, err = strconv.ParseFloat(fmt.Sprintf("%.2f", idxif.Sv.Float64*(1+idxif.Lv.Float64)), 64)
				if err != nil {
					glog.V(0).Infof("计算高水位值错误")
					return err
				}
				idxif.Uv.Float64, err = strconv.ParseFloat(fmt.Sprintf("%.2f", idxif.Sv.Float64*(1+idxif.Uv.Float64)), 64)
				if err != nil {
					glog.V(0).Infof("计算低水位值错误")
					return err
				}
			}
		}
		tmpmap[idxif.Id] = *idxif
	}
	glog.V(3).Infof("debug-->gIdxMap address [%p]", gIdxMap)
	if len(gIdxMap) > 0 {
		gIdxMap = make(map[string]Idxinfo, 256)
	}
	var body []byte
	body, err = json.Marshal(tmpmap)
	if err != nil {
		glog.V(0).Infof("marshal err,%v", err)
		return err
	}
	err = json.Unmarshal(body, &gIdxMap)
	if err != nil {
		glog.V(0).Infof("unmarshal err,%v", err)
		return err
	}
	glog.V(3).Infof("debug-->gIdxMap address [%p]", gIdxMap)
	glog.V(1).Info("finish InitIdxinfo...")
	glog.V(1).Info("初始化后指标库信息：")
	for k, v := range gIdxMap {
		glog.V(3).Infof("[%v]-[%v]\n", k, v)
	}
	return nil
}

func (ib *IdxBusi) DoIdxBusi() {
	var err error
	for {
		v, ok := <-gCH_idx
		if !ok {
			glog.V(1).Infof("数据读取完毕.")
			break
		}
		glog.V(3).Infof("处理服务从通道获取数据:[%v]\n", v)
		err = v.updateIdx()
		if err != nil {
			glog.V(0).Infof("updateIdx failed,err:%v\n", err)
		} else {
			err = v.warn()
		}
		var r Result
		if err != nil {
			r = Result{v, time.Now().Format("2006-01-02 15:04:05"), err.Error()}
		} else {
			r = Result{v, time.Now().Format("2006-01-02 15:04:05"), "ok"}
		}
		gResultMap[r.realId] = r
	}
}

func (idx *Index) DoIdx() (err error) {
	_, e := gIdxMap[idx.Id]
	if !e {
		glog.V(0).Infof("query indexinfo  %s err", idx.Id)
		return fmt.Errorf("未定义指标[%s]", idx.Id)
	}
	glog.V(3).Infof("请求数据格式化后:%v\n", idx)
	gCH_idx <- *idx
	return nil
}

func Idx_handler(w http.ResponseWriter, r *http.Request) {
	var idx Index
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
		num := strings.Count(body_str, `"id"`) + strings.Count(body_str, `"value"`)
		err = json.Unmarshal(body, &idx)
		if err != nil || num != 2 {
			result.Code = "401"
			result.Msg = "参数有误"
			return
		} else {
			if len(strings.TrimSpace(idx.Host)) == 0 {
				idx.Host = RemoteIp(r)
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
		stmp, vok := r.Form["value"]
		host, hok := r.Form["host"]
		clientIp := RemoteIp(r)
		if !iok || !vok {
			result.Code = "401"
			result.Msg = "参数有误"
		} else {
			value, err := strconv.ParseFloat(stmp[0], 32)
			if err != nil {
				glog.V(0).Infof("ParseFloat err:%v", err)
				result.Code = "401"
				result.Msg = "参数有误"
				return
			} else {
				if hok {
					idx = Index{id[0], float32(value), host[0], ""}
				} else {
					idx = Index{id[0], float32(value), clientIp, ""}
				}
			}
		}
	}

	err := idx.DoIdx()
	if err != nil {
		glog.V(3).Infof("doIdx() err :%v", err)
		result.Msg = err.Error()
	} else {
		result.Msg = "提交成功"
	}
	result.Code = "200"
}

func (ib *IdxBusi) prepareSql(str string) (stmt *sql.Stmt, err error) {
	glog.V(3).Infof("prepare sql:%s", str)
	stmt, err = gIB.Db.Prepare(str)
	return
}

func (idx *Index) updateIdx() (err error) {
	//考虑到类似共性类指标
	//如主机ops、obs等，指标：020105X，该类指标最后一位为x，在写入idx_now,idx_his表时，将指标进行替换为020105CA3001
	if strings.HasSuffix(idx.Id, "X") || strings.HasSuffix(idx.Id, "x") {
		idx.realId = idx.Id[:len(idx.Id)-1] + strings.ToUpper(idx.Host)
	} else {
		idx.realId = idx.Id
	}
	glog.V(4).Infof("通用指标，RealId:%s\n", idx.realId)

	glog.V(3).Infof("###start updateIdx(),id=%s", idx.Id)
	var v Idxinfo
	v, e1 := gIdxMap[idx.realId]
	if !e1 {
		v2, e2 := gIdxMap[idx.Id]
		if !e2 {
			glog.V(0).Infof("query indexinfo err,%v", e2)
			return fmt.Errorf("未定义指标[%s]", idx.Id)
		} else {
			v = v2
		}
	}
	glog.V(6).Infof("Idxinfo:%v\n", v) //for debug

	var tx *sql.Tx
	tx, err = gIB.Db.Begin()
	if err != nil {
		glog.V(0).Infof("updateidx faile,%v", err)
		return
	}
	defer func() {
		if err != nil {
			glog.V(0).Infoln("rollback!!!!!!")
			tx.Rollback()
		} else {
			glog.V(6).Infof("###updateIdx() success,id=%s", idx.realId)
			tx.Commit()
		}
	}()

	tm := time.Now()
	var result sql.Result
	sqlstr_insthis := `insert into idx_his (id,time,host,value) values(?,?,?,?)`
	result, err = tx.Exec(sqlstr_insthis, idx.realId, tm, idx.Host, idx.Value)
	if err != nil {
		glog.V(0).Infof("insert into idx_his err,%v", err)
		return
	}
	glog.V(6).Infoln(result.RowsAffected())

	sqlstr_instnow := `insert into idx_now (id,time,host,value) values(?,?,?,?)`
	sqlstr_upnow := `update idx_now set time=?,host=?,value=? where id=?`
	if v.Needup {
		glog.V(3).Infof("直接更新指标数据\n")
		result, err = tx.Exec(sqlstr_upnow, tm, idx.Host, idx.Value, idx.realId)
		if err != nil {
			glog.V(0).Infof("update idx_now err,%v", err)
			return
		}
		glog.V(6).Infoln(result.RowsAffected())
	} else { //重启或kill -SIGUSR1后一定会执行
		ct := pub.SelectCount("select count(id) count from idx_now where id =?", idx.realId)
		glog.V(3).Infof("判断是否首次插入idx_now指标数据，ct=%d\n", ct)
		if ct == 0 {
			result, err = tx.Exec(sqlstr_instnow, idx.realId, tm, idx.Host, idx.Value)
			//result,err = gIB.stmt_instnow.Exec(idx.Id, tm, idx.Host, idx.Value)
		} else {
			result, err = tx.Exec(sqlstr_upnow, tm, idx.Host, idx.Value, idx.realId)
			//result,err = gIB.stmt_upnow.Exec(tm, idx.Host, idx.Value, idx.Id)
		}
		if err != nil {
			glog.V(0).Infof("insert/update idx_now err,%v", err)
			return
		}
		glog.V(6).Infoln(result.RowsAffected())
		v.Needup = true
		var iswarn sql.NullBool
		row1 := pub.QueryOneRow("select iswarn from idx_warn_count where id =?", idx.realId)
		err = row1.Scan(&iswarn)
		if err != nil { //首次无记录会报错
			glog.V(0).Infof("Scan failed,err:%v\n", err)
		}
		if iswarn.Bool { //当前预警事件打开状态，更新内存表
			v.IsWarn = true
		}
		//在通用指标时，需更新内存表中该指标信息
		row2 := pub.QueryOneRow("select flag,lv,sv,uv,warnnum from idx_warn where id=?;", idx.realId)
		err = row2.Scan(&v.Flag, &v.Lv, &v.Sv, &v.Uv, &v.WarnNum)
		if err != nil {
			glog.V(0).Infof("Scan failed,err:%v\n", err)
		}
		v.Id = idx.realId
		gIdxMap[idx.realId] = v
	}
	return nil
}

func (idx *Index) warn() (err error) {
	type warninfo struct {
		Id_original string `json:"id_original"`
		Source      string `json:"source"`
		Ip          string `json:"ip"`
		Hostname    string `json:"hostname"`
		Severity    string `json:"severity"`
		Title       string `json:"title"`
		Summary     string `json:"summary"`
		Status      string `json:"status"`
		ShowTimes   string `json:"showtimes"`
		NoticeEmpNo string `json:"noticeempno"`
	}
	var v Idxinfo
	v, ok := gIdxMap[idx.realId] //先找自有值，未找到情况再找公共值
	if !ok {
		v, _ = gIdxMap[idx.Id]
	}
	if !v.Flag.Valid {
		glog.V(3).Infof("指标[%s]未定义预警信息.\n", idx.Id)
		return nil
	}
	glog.V(6).Infof("Idxinfo:%v\n", v) //for debug

	var warn_content string
	warn_flag := true
	severity := "3"
	str_now := delzero(fmt.Sprintf("%.2f", idx.Value))
	str_lv := delzero(fmt.Sprintf("%.2f", v.Lv.Float64))
	str_sv := delzero(fmt.Sprintf("%.2f", v.Sv.Float64))
	str_uv := delzero(fmt.Sprintf("%.2f", v.Uv.Float64))

	switch []byte(v.Flag.String)[1] {
	case '1': //不在区间
		glog.V(3).Infof("Warn1:\n")
		if float64(idx.Value) > v.Uv.Float64 {
			warn_content = fmt.Sprintf("指标%s预警,%s,当前值%s%s,大于%s", idx.realId, v.Name, str_now, v.Unit, str_uv)
		} else if float64(idx.Value) < v.Lv.Float64 {
			warn_content = fmt.Sprintf("指标%s预警,%s,当前值%s%s,小于%s", idx.realId, v.Name, str_now, v.Unit, str_lv)
		} else {
			warn_flag = false
		}
	case '2': //不等于标准值
		glog.V(3).Infof("Warn2:\n")
		if float64(idx.Value) != v.Sv.Float64 {
			warn_content = fmt.Sprintf("指标%s预警,%s,当前值%s%s,不等于%s", idx.realId, v.Name, str_now, v.Unit, str_sv)
		} else {
			warn_flag = false
		}
	case '3': //高于高水位
		glog.V(3).Infof("Warn3:\n")
		if float64(idx.Value) > v.Uv.Float64 {
			warn_content = fmt.Sprintf("指标%s预警,%s,当前值%s%s,大于%s", idx.realId, v.Name, str_now, v.Unit, str_uv)
			severity = "1"
		} else if float64(idx.Value) > v.Sv.Float64 {
			warn_content = fmt.Sprintf("指标%s预警,%s,当前值%s%s,大于%s", idx.realId, v.Name, str_now, v.Unit, str_sv)
			severity = "2"
		} else if float64(idx.Value) > v.Lv.Float64 {
			warn_content = fmt.Sprintf("指标%s预警,%s,当前值%s%s,大于%s", idx.realId, v.Name, str_now, v.Unit, str_lv)
		} else {
			warn_flag = false
		}
	case '4': //低于低水位
		glog.V(3).Infof("Warn4:\n")
		if float64(idx.Value) < v.Lv.Float64 {
			warn_content = fmt.Sprintf("指标%s预警,%s,当前值%s%s,小于%s", idx.realId, v.Name, str_now, v.Unit, str_lv)
			severity = "1"
		} else if float64(idx.Value) < v.Sv.Float64 {
			warn_content = fmt.Sprintf("指标%s预警,%s,当前值%s%s,小于%s", idx.realId, v.Name, str_now, v.Unit, str_sv)
			severity = "2"
		} else if float64(idx.Value) < v.Uv.Float64 {
			warn_content = fmt.Sprintf("指标%s预警,%s,当前值%s%s,小于%s", idx.realId, v.Name, str_now, v.Unit, str_uv)
		} else {
			warn_flag = false
		}
	default:
		glog.V(3).Infof("warn:未定义\n")
		warn_flag = false
	}

	status := "1" //默认为打开
	if !warn_flag {
		if !v.IsWarn { //事件未打开
			return nil
		} else {
			status = "2" //关闭事件请求
		}
	}

	if status == "1" {
		v.IsWarn = true
		v.ContinuousWarnNum++

	} else if status == "2" {
		v.IsWarn = false
		v.ContinuousWarnNum = 0
		closeWarn(idx.realId)
	}
	gIdxMap[idx.realId] = v //更新覆盖原map对应值

	if status == "1" && v.ContinuousWarnNum < v.WarnNum.Int32 {
		glog.V(3).Infof("指标[%s]连续预警次数%d,预警次数%d,不发送预警事件.\n", idx.realId, v.ContinuousWarnNum, v.WarnNum.Int32)
		return nil
	}

	err, warnid := genWarnId(idx.realId,status) //暂时由本地自行维护id，后续可根据需要通过将indexid作为ectype进行上送；
	if err != nil {
		glog.V(0).Infof("genWarnId失败:%v", err)
		return err
	}
	winfo := new(warninfo)
	winfo.Id_original = warnid
	if len(strings.TrimSpace(idx.Host)) == 0 {
		winfo.Ip = conf.GetIni().LocalAddr //指标服务器地址
	} else if strings.Count(idx.Host, ".") == 3 {
		winfo.Ip = idx.Host
	} else {
		winfo.Hostname = idx.Host
	}
	winfo.Source = "index"
	winfo.Title = "指标预警"
	winfo.Severity = severity
	winfo.Status = status
	winfo.ShowTimes = "1800"
	winfo.Summary = warn_content
	if v.EmpNm.Valid {
		winfo.NoticeEmpNo = v.EmpNm.String
	}

	jsonbytes, _ := json.Marshal(winfo)
	glog.V(3).Infof("提交预警事件信息:%s", string(jsonbytes))
	r, err := pub.PostJson(conf.GetIni().WarnAddr, string(jsonbytes))
	glog.V(3).Infof("result:%s", r)
	if err != nil {
		glog.V(0).Infof("提交失败:%v", err)
		return err
	}

	return nil
}

func delzero(s string) string {
	byteStr := []byte(s)
	if len(byteStr) == 0 {
		return ""
	}
	i := len(byteStr) - 1
	for i > 0 {
		if byteStr[i] == '.' {
			i--
			break
		}
		if byteStr[i] == '0' {
			i--
		} else {
			break
		}
	}
	return string(byteStr[:i+1])
}

func closeWarn(idxid string) (err error) {
	sqlstr_up := `update idx_warn_count set iswarn=? where id=?`
	_, err = pub.GetDb().Exec(sqlstr_up, false, idxid)
	if err != nil {
		glog.V(0).Infof("update  idx_warn_count err,%v", err)
		return
	}
	return
}

func genWarnId(idxid ,status string) (err error, warnid string) {

	var num sql.NullInt32
	var iswarn sql.NullBool

	ct := pub.SelectCount("select count(id) count from idx_warn_count where id =?", idxid)
	glog.V(3).Infof("判断是否首次插入idx_warn_count，ct=%d\n", ct)
	if ct == 0 {
		sqlstr_inst := `insert into idx_warn_count (id,num,iswarn) values(?,?,?)`
		_, err = pub.GetDb().Exec(sqlstr_inst, idxid, 1, true)
		if err != nil {
			glog.V(0).Infof("insert  idx_warn_count err,%v", err)
			return
		}
		warnid = fmt.Sprintf("%s-%d", idxid, 1)
		return
	}

	row := pub.QueryOneRow("select num,iswarn from idx_warn_count where id =?", idxid)
	err = row.Scan(&num, &iswarn)
	if err != nil {
		glog.V(0).Infof("Scan failed,err:%v\n", err)
		return
	}
	if status == "2" { //当前预警事件打开状态，直接取当前值
		warnid = fmt.Sprintf("%s-%d", idxid, num.Int32)
		return
	}

	sqlstr_up := `update idx_warn_count set num=?,iswarn=? where id=?`
	_, err = pub.GetDb().Exec(sqlstr_up, num.Int32+1, true, idxid)
	if err != nil {
		glog.V(0).Infof("update  idx_warn_count err,%v", err)
		return
	}
	warnid = fmt.Sprintf("%s-%d", idxid, num.Int32+1)
	return
}
