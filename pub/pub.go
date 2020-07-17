package pub

import (
	"github.com/golang/glog"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

const (
	timeTemplate1 = "2006-01-02 15:04:05" //常规类型
	timeTemplate2 = "2006/01/02 15:04:05" //其他类型
	timeTemplate3 = "2006-01-02"          //其他类型
	timeTemplate4 = "20060102"
	timeTemplate5 = "15:04:05" //其他类型
)

//timeTemplate1 = "2006-01-02 15:04:05"
func GetFmtDateStr1(d time.Time) (s string) {
	return d.Format(timeTemplate1)
}

func GetFmtDateStr4(d time.Time) (s string) {
	return d.Format(timeTemplate4)
}

//timeTemplate4 = "20060102"
func GetDateStr4() (s string) {
	return time.Now().Format(timeTemplate4)
}

func GetNextDateStr4() (s string) {
	return time.Now().Add(24 * time.Hour).Format(timeTemplate4)
}

func IsMonthEnd(s string) (bool, error) {
	d1, err := time.Parse(timeTemplate4, s)
	if err != nil {
		return false, err
	}
	d2 := d1.Add(time.Hour * 24)
	if d1.Month() != d2.Month() {
		return true, nil
	} else {
		return false, nil
	}
}

func IsYearEnd(s string) (bool, error) {
	d1, err := time.Parse(timeTemplate4, s)
	if err != nil {
		return false, err
	}
	d2 := d1.Add(time.Hour * 24)
	if d1.Year() != d2.Year() {
		return true, nil
	} else {
		return false, nil
	}
}

func GetCurrentPath() (string, error) {
	return "", nil
}


//dataformat id=1000001&value=20.4&host=ipaddress
func PostForm(url string, data string) (string, error) {
	contentType := "application/x-www-form-urlencoded"
	return post(url, contentType, strings.NewReader(data))
}

//dataformat json.Unmarshal([]byte(jsonStr), &data) ##data struct
func PostJson(url, data string) (string, error) {
	contentType := "application/json"
	//jsonStr, _ := json.Marshal(data)
	//return post(url, contentType, bytes.NewReader(jsonStr))
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
		glog.V(0).Infof("http post err,%v\n", err)
		return "", err
	}
	defer resp.Body.Close()

	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(result), nil
}